__author__ = "Andreas Vollrath"

import os
import sys
import json
import glob

import requests
import urllib
import zipfile
import fnmatch
import xml.dom.minidom
import numpy as np
import geopandas as gpd
import xml.etree.ElementTree as ET

from shapely.wkt import loads
from urllib.error import URLError
from os.path import join as opj

from ost.helpers import scihub


class s1Metadata:
    """
    Get metadata from Sentinel-1 scene id
    """

    num_of_scenes = 0

    def __init__(self, scene_id):
        self.scene_id = scene_id
        self.mission_id = scene_id[0:3]
        self.mode_beam = scene_id[4:6]
        self.product_type = scene_id[7:10]
        self.res_class = scene_id[10]
        self.proc_level = scene_id[12]
        self.pol_mode = scene_id[14:16]
        self.start_date = scene_id[17:25]
        self.start_time = scene_id[26:32]
        self.stop_date = scene_id[33:41]
        self.stop_time = scene_id[42:48]
        self.abs_orbit = scene_id[49:55]
        self.data_take_id = scene_id[57:62]
        self.unique_id = scene_id[63:]
        self.year = scene_id[17:21]
        self.month = scene_id[21:23]
        self.day = scene_id[23:25]

        # Calculate the relative orbit out of absolute orbit
        # (from Peter Meadows (ESA) @
        # http://forum.step.esa.int/t/sentinel-1-relative-orbit-from-filename/7042)
        if self.mission_id == 'S1A':
            self.orbit_offset = 73
            self.satellite = "Sentinel-1A"
        elif self.mission_id == 'S1B':
            self.orbit_offset = 27
            self.satellite = "Sentinel-1A"

        self.rel_orbit = (((int(self.abs_orbit)
                            - int(self.orbit_offset)) % 175) + 1)

        # get acquisition mode
        if self.mode_beam == 'IW':
            self.acq_mode = "Interferometric Wide Swath"
        elif self.mode_beam == 'SM':
            self.acq_mode = "Stripmap"
        elif self.mode_beam == 'EW':
            self.acq_mode = "Extra-Wide swath"
        elif self.mode_beam == 'WV':
            self.acq_mode = "Wave"

        # get acquisition mode
        if self.product_type == 'GRD':
            self.p_type = "Ground Range Detected (GRD)"
        elif self.product_type == 'SLC':
            self.p_type = "Single-Look Complex (SLC)"
        elif self.product_type == 'OCN':
            self.p_type = "Ocean"
        elif self.product_type == 'RAW':
            self.p_type = "Raw Data (RAW)"

        # increment class variable for every scene read
        s1Metadata.num_of_scenes += 1

    def s1Info(self):

        print(" -------------------------------------------------")
        print(" Scene Information:")
        print(" Scene Identifier:        " + str(self.scene_id))
        print(" Satellite:               " + str(self.satellite))
        print(" Acquisition Mode:        " + str(self.acq_mode))
        print(" Processing Level:        " + str(self.proc_level))
        print(" Product Type:            " + str(self.p_type))
        print(" Acquisition Date:        " + str(self.start_date))
        print(" Start Time:              " + str(self.start_time))
        print(" Stop Time:               " + str(self.stop_time))
        print(" Absolute Orbit:          " + str(self.abs_orbit))
        print(" Relative Orbit:          " + str(self.rel_orbit))
        print(" -------------------------------------------------")

    def s1DwnPath(self, dwnDir):

        dlPath = opj(dwnDir, 'SAR',
                     self.product_type,
                     self.year,
                     self.month,
                     self.day)

        # make dir if not existent
        os.makedirs(dlPath, exist_ok=True)
        # get filePath
        filePath = opj(dlPath, '{}.zip'.format(self.scene_id))

        return filePath

    def s1EsaUuidFromId(self, opener):

        # construct the basic the url
        baseURL = ('https://scihub.copernicus.eu/apihub/odata/v1/'
                   'Products?$filter=')
        action = urllib.request.quote('Name eq \'{}\''.format(self.scene_id))
        # construct the download url
        url = baseURL + action

        try:
            # get the request
            req = opener.open(url)
        except URLError as e:
            if hasattr(e, 'reason'):
                print(' We failed to connect to the server.')
                print(' Reason: ', e.reason)
                sys.exit()
            elif hasattr(e, 'code'):
                print(' The server couldn\'t fulfill the request.')
                print(' Error code: ', e.code)
                sys.exit()
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from scihub)
            response = req.read().decode('utf-8')

            # parse the xml page from the response
            dom = xml.dom.minidom.parseString(response)

            # loop thorugh each entry (with all metadata)
            for node in dom.getElementsByTagName('entry'):
                downloadURL = node.getElementsByTagName(
                                            'id')[0].firstChild.nodeValue
                uuid = downloadURL.split('(\'')[1].split('\')')[0]

        return uuid

    def s1EsaSceneUrl(self, opener):

        uuid = self.s1EsaUuidFromId(opener)
        # scihub url
        scihubURL = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'
        # construct the download url
        urlDownload = '{}(\'{}\')/$value'.format(scihubURL, uuid)

        return urlDownload

    def s1EsaMd5Url(self, opener):

        uuid = self.s1EsaUuidFromId(opener)
        scihubURL = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'
        urlDownload = '{}(\'{}\')/Checksum/Value/$value'.format(scihubURL,
                                                                uuid)
        return urlDownload

    def checkOnlineStatus(self, opener):

        uuid = self.s1EsaUuidFromId(opener)
        scihubURL = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'

        url = '{}(\'{}\')/Online/$value'.format(scihubURL, uuid)

        try:
            # get the request
            req = opener.open(url)
        except URLError as e:
            if hasattr(e, 'reason'):
                print(' We failed to connect to the server.')
                print(' Reason: ', e.reason)
                sys.exit()
            elif hasattr(e, 'code'):
                print(' The server couldn\'t fulfill the request.')
                print(' Error code: ', e.code)
                sys.exit()
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from scihub)
            response = req.read().decode('utf-8')

            if response == 'true':
                response = True
            elif response == 'false':
                response = False

        return response

    def triggerScihubProduction(self, opener):

        uuid = self.s1EsaUuidFromId(opener)
        scihubURL = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'

        url = '{}(\'{}\')/$value'.format(scihubURL, uuid)

        try:
            # get the request
            req = opener.open(url)

        except URLError as e:
            if hasattr(e, 'reason'):
                print(' We failed to connect to the server.')
                print(' Reason: ', e.reason)
                sys.exit()
            elif hasattr(e, 'code'):
                print(' The server couldn\'t fulfill the request.')
                print(' Error code: ', e.code)
                sys.exit()

        # write the request to to the response variable
        # (i.e. the xml coming back from scihub)
        code = req.getcode()
        if code is 202:
            print(' Production of {} successfully requested.'
                  .format(self.scene_id))

        return code

    def s1EsaAnnoUrl(self, opener):

        uuid = self.s1EsaUuidFromId(opener)

        print(' INFO: Getting URLS of annotation files'
              ' for S1 product: {}.'.format(self.scene_id))
        scihubURL = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'
        annoPath = ('(\'{}\')/Nodes(\'{}.SAFE\')/Nodes(\'annotation\')/'
                    'Nodes'.format(uuid, self.scene_id))
        url = scihubURL + annoPath
        print(url)
        try:
            # get the request
            req = opener.open(url)
        except URLError as e:
            if hasattr(e, 'reason'):
                print(' We failed to connect to the server.')
                print(' Reason: ', e.reason)
                sys.exit()
            elif hasattr(e, 'code'):
                print(' The server couldn\'t fulfill the request.')
                print(' Error code: ', e.code)
                sys.exit()
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from scihub)
            response = req.read().decode('utf-8')

            # parse the xml page from the response
            dom = xml.dom.minidom.parseString(response)
            urlList = []
            # loop thorugh each entry (with all metadata)
            for node in dom.getElementsByTagName('entry'):
                downloadURL = node.getElementsByTagName(
                                                'id')[0].firstChild.nodeValue

                if downloadURL[-6:-2] == '.xml':
                    urlList.append('{}/$value'.format(downloadURL))

        return urlList

    def s1DwnAnno(self, dwnDir):

        colNames = ['SceneID', 'Track', 'Date', 'SwathID', 'AnxTime',
                    'BurstNr', 'geometry']

        # crs for empty dataframe
        crs = {'init': 'epsg:4326'}
        gdfFull = gpd.GeoDataFrame(columns=colNames, crs=crs)

        file = self.s1DwnPath(dwnDir)

        # extract info from archive
        archive = zipfile.ZipFile(file, 'r')
        nameList = archive.namelist()
        xmlFiles = fnmatch.filter(nameList, "*/annotation/s*.xml")

        # loop through xml annotation files
        for xmlFile in xmlFiles:
            xml = archive.open(xmlFile)

            gdf = self.s1BurstInfo(ET.parse(xml))
            gdfFull = gdfFull.append(gdf)

        return gdfFull.drop_duplicates(['AnxTime'], keep='first')

    def s1CreoPath(self, basePath='/eodata/Sentinel-1'):

        path = opj(basePath, 'SAR',
                   self.product_type,
                   self.year,
                   self.month,
                   self.day,
                   '{}.SAFE'.format(self.scene_id))

        return path

    def s1CreoAnno(self):

        colNames = ['SceneID', 'Track', 'Date', 'SwathID', 'AnxTime',
                    'BurstNr', 'geometry']
        gdfFull = gpd.GeoDataFrame(columns=colNames)

        for annoFile in glob.glob(
                '{}/annotation/*xml'.format(self.s1CreoPath())):
            # parse the xml page from the response
            gdf = self.s1BurstInfo(ET.parse(annoFile))

            gdfFull = gdfFull.append(gdf)

        return gdfFull.drop_duplicates(['AnxTime'], keep='first')

    def s1EsaAnno(self, uname=None, pword=None):

        # define column names fro BUrst DF
        COLUMN_NAMES = ['SceneID',
                        'Track',
                        'Date',
                        'SwathID',
                        'AnxTime',
                        'BurstNr',
                        'geometry']

        gdfFull = gpd.GeoDataFrame(columns=COLUMN_NAMES)

        baseURL = "https://scihub.copernicus.eu/apihub/"

        # get connected to scihub
        opener = scihub.scihubConnect(baseURL, uname, pword)

        annoList = self.s1EsaAnnoUrl(opener)

        for url in annoList:
            try:
                # get the request
                req = opener.open(url)
            except URLError as e:
                if hasattr(e, 'reason'):
                    print(' We failed to connect to the server.')
                    print(' Reason: ', e.reason)
                    sys.exit()
                elif hasattr(e, 'code'):
                    print(' The server couldn\'t fulfill the request.')
                    print(' Error code: ', e.code)
                    sys.exit()
            else:
                # write the request to to the response variable
                # (i.e. the xml coming back from scihub)
                response = req.read().decode('utf-8')

                ETroot = ET.fromstring(response)

                # parse the xml page from the response
                gdf = self.s1BurstInfo(ETroot)

                gdfFull = gdfFull.append(gdf)

        return gdfFull.drop_duplicates(['AnxTime'], keep='first')

    def s1BurstInfo(self, ETroot):
        '''
        This functions expects an xml string from a Sentinel-1 SLC
        annotation file and extracts relevant information for burst
        identification as a GeoPandas GeoDataFrame.

        Much of the code is taken from RapidSAR
        package (once upon a time on github).
        '''

        colNames = ['SceneID', 'Track', 'Date', 'SwathID', 'AnxTime',
                    'BurstNr', 'geometry']
        gdf = gpd.GeoDataFrame(columns=colNames)

        track = self.rel_orbit
        acqDate = self.start_date

        # pol = root.find('adsHeader').find('polarisation').text
        swath = ETroot.find('adsHeader').find('swath').text
        linesPerBurst = np.int(ETroot.find('swathTiming').find(
                                                    'linesPerBurst').text)
        pixelsPerBurst = np.int(ETroot.find('swathTiming').find(
                                                    'samplesPerBurst').text)
        burstlist = ETroot.find('swathTiming').find('burstList')
        geolocGrid = ETroot.find('geolocationGrid')[0]
        first = {}
        last = {}

        # Get burst corner geolocation info
        for geoPoint in geolocGrid:
            if geoPoint.find('pixel').text == '0':
                first[geoPoint.find('line').text] = np.float32(
                        [geoPoint.find('latitude').text,
                         geoPoint.find('longitude').text])
            elif geoPoint.find('pixel').text == str(pixelsPerBurst-1):
                last[geoPoint.find('line').text] = np.float32(
                        [geoPoint.find('latitude').text,
                         geoPoint.find('longitude').text])

        for i, b in enumerate(burstlist):
            firstline = str(i*linesPerBurst)
            lastline = str((i+1)*linesPerBurst)
            aziAnxTime = np.float32(b.find('azimuthAnxTime').text)
            aziAnxTime = np.int32(np.round(aziAnxTime*10))
#           burstid = 'T{}_{}_{}'.format(track, swath, burstid)
#           first and lastline sometimes shifts by 1 for some reason?
            try:
                firstthis = first[firstline]
            except:
                firstline = str(int(firstline)-1)
                try:
                    firstthis = first[firstline]
                except:
                    print('First line not found in annotation file')
                    firstthis = []
            try:
                lastthis = last[lastline]
            except:
                lastline = str(int(lastline)-1)
                try:
                    lastthis = last[lastline]
                except:
                    print('Last line not found in annotation file')
                    lastthis = []
            corners = np.zeros([4, 2], dtype=np.float32)

            # Had missing info for 1 burst in a file, hence the check
            if len(firstthis) > 0 and len(lastthis) > 0:
                corners[0] = first[firstline]
                corners[1] = last[firstline]
                corners[3] = first[lastline]
                corners[2] = last[lastline]

            wkt = 'POLYGON (({} {},{} {},{} {},{} {},{} {}))'.format(
                        np.around(float(corners[0, 1]), 3),
                        np.around(float(corners[0, 0]), 3),
                        np.around(float(corners[3, 1]), 3),
                        np.around(float(corners[3, 0]), 3),
                        np.around(float(corners[2, 1]), 3),
                        np.around(float(corners[2, 0]), 3),
                        np.around(float(corners[1, 1]), 3),
                        np.around(float(corners[1, 0]), 3),
                        np.around(float(corners[0, 1]), 3),
                        np.around(float(corners[0, 0]), 3))

            gDict = {'SceneID': self.scene_id, 'Track': track,
                     'Date': acqDate, 'SwathID': swath,
                     'AnxTime': aziAnxTime, 'BurstNr': i+1,
                     'geometry': loads(wkt)}

            gdf = gdf.append(gDict, ignore_index=True)

        return gdf

    def s1ASFURL(self):

        asfURL = 'https://datapool.asf.alaska.edu'

        if self.mission_id == 'S1A':
            mission = 'SA'
        elif self.mission_id == 'S1B':
            mission = 'SB'

        if self.product_type == 'SLC':
            pType = self.product_type
        elif self.product_type == 'GRD':
            pType = 'GRD_{}{}'.format(self.res_class, self.pol_mode[0])

        productURL = '{}/{}/{}/{}.zip'.format(asfURL, pType,
                                              mission, self.scene_id)
        return productURL

    def s1PepsUuid(self, uname, pword):

        url = ('https://peps.cnes.fr/resto/api/collections/S1/search.json?q={}'
               .format(self.scene_id))
        response = requests.get(url, stream=True, auth=(uname, pword))

        # check response
        if response.status_code == 401:
            raise ValueError(' ERROR: Username/Password are incorrect.')
        elif response.status_code != 200:
            response.raise_for_status()

        data = json.loads(response.text)
        pepsUuid = data['features'][0]['id']
        dwnUrl = (data['features'][0]['properties']
                      ['services']['download']['url'])

        return pepsUuid, dwnUrl

    def s1PepsStatus(self, uname, pword):

        """
        This function will download S1 products from CNES Peps mirror.

        :param url: the url to the file you want to download
        :param fileName: the absolute path to where the downloaded file should
                         be written to
        :param uname: ESA's scihub username
        :param pword: ESA's scihub password
        :return:
        """

        _, url = self.s1PepsUuid(uname, pword)

        # define url
        response = requests.get(url, stream=True, auth=(uname, pword))
        status = response.status_code

        # check response
        if status == 401:
            raise ValueError(' ERROR: Username/Password are incorrect.')
        elif status == 404:
            raise ValueError(' ERROR: File not found.')
        elif status == 200:
            status = 'online'
        elif status == 202:
            status = 'onTape'
        else:
            response.raise_for_status()

        return status, url
