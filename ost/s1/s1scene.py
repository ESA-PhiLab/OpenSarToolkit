'''This module contains the S1Scene class for handling of a Sentinel-1 product

'''
import os
from os.path import join as opj
import sys
import importlib
import json
import glob
import urllib
from urllib.error import URLError
import zipfile
import fnmatch
import xml.dom.minidom
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import geopandas as gpd
import requests
from shapely.wkt import loads

from ost.helpers import scihub, peps, onda, raster as ras
from ost.s1.grd_to_ard import grd_to_ard, ard_to_rgb, ard_to_thumbnail

__author__ = "Andreas Vollrath"
__version__ = 1.0
__license__ = 'MIT'


class Sentinel1_Scene():

    def __init__(self, scene_id, ard_type='OST Standard'):
        self.scene_id = scene_id
        self.mission_id = scene_id[0:3]
        self.mode_beam = scene_id[4:6]
        self.product_type = scene_id[7:10]
        self.resolution_class = scene_id[10]
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
        self.onda_class = scene_id[4:14]
        # Calculate the relative orbit out of absolute orbit
        # (from Peter Meadows (ESA) @
        # http://forum.step.esa.int/t/sentinel-1-relative-orbit-from-filename/7042)
        if self.mission_id == 'S1A':
            self.orbit_offset = 73
            self.satellite = "Sentinel-1A"
        elif self.mission_id == 'S1B':
            self.orbit_offset = 27
            self.satellite = "Sentinel-1B"

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

        # set initial product paths to None
        self.ard_dimap = None
        self.ard_rgb = None
        self.rgb_thumbnail = None

        # set initial ARD parameters to ard_type
        self.ard_parameters = {}
        self.get_ard_parameters(ard_type)

    def info(self):

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

    def download(self, download_dir, mirror=None):

        if not mirror:
            print(' INFO: One or more of your scenes need to be downloaded.')
            print(' Select the server from where you want to download:')
            print(' (1) Copernicus Apihub (ESA, rolling archive)')
            print(' (2) Alaska Satellite Facility (NASA, full archive)')
            print(' (3) PEPS (CNES, 1 year rolling archive)')
            print(' (4) ONDA DIAS (ONDA DIAS full archive for SLC - or GRD from 30 June 2019)')
            print(' (5) Alaska Satellite Facility (using WGET - unstable - use only if 2 fails)')
            mirror = input(' Type 1, 2, 3, 4 or 5: ')


        from ost.s1 import download

        if mirror == '1':
            uname, pword = scihub.ask_credentials()
            opener = scihub.connect(uname=uname, pword=pword)
            df = pd.DataFrame(
                {'identifier': [self.scene_id],
                 'uuid': [self.scihub_uuid(opener)]
                }
            )
        elif mirror == '3':
            uname, pword = peps.ask_credentials()
            df = pd.DataFrame(
                {'identifier': [self.scene_id],
                 'uuid': [self.peps_uuid(uname=uname, pword=pword)]
                }
            )
        elif mirror == '4':
            uname, pword = onda.ask_credentials()
            opener = onda.connect(uname=uname, pword=pword)
            df = pd.DataFrame(
                {'identifier': [self.scene_id],
                 'uuid': [self.ondadias_uuid(opener)]
                }
            )
        else:   # ASF
            df = pd.DataFrame({'identifier': [self.scene_id]})
            download.download_sentinel1(df, download_dir, mirror)
            return

        download.download_sentinel1(df, download_dir, mirror,
                                    uname=uname, pword=pword)


        del uname, pword

    # location of file (including diases)
    def _download_path(self, download_dir, mkdir=False):

        download_path = opj(download_dir, 'SAR',
                            self.product_type,
                            self.year,
                            self.month,
                            self.day)

        # make dir if not existent
        if mkdir:
            os.makedirs(download_path, exist_ok=True)

        # get filepath
        filepath = opj(download_path, '{}.zip'.format(self.scene_id))

        return filepath

    def _creodias_path(self, data_mount='/eodata'):


        path = opj(data_mount, 'Sentinel-1', 'SAR',
                   self.product_type,
                   self.year,
                   self.month,
                   self.day,
                   '{}.SAFE'.format(self.scene_id))

        return path

    def _aws_path(self, data_mount):

        # print('Dummy function for aws path to be added')
        return '/foo/foo/foo'

    def _mundi_path(self, mont_point):

        # print(' Dummy function for mundi paths to be added')
        return '/foo/foo/foo'

    def _onda_path(self, data_mount):


        path = opj(data_mount, 'S1', 'LEVEL-1',
                   '{}'.format(self.onda_class),
                   self.year,
                   self.month,
                   self.day,
                   '{}.zip'.format(self.scene_id),
                   '{}.SAFE'.format(self.scene_id))

        return path

    def get_path(self, download_dir=None, data_mount='/eodata'):

        if download_dir:
            if os.path.isfile(self._download_path(download_dir) + '.downloaded'):
                path = self._download_path(download_dir)
            else:
                path = None
        else:
            path=None

        if data_mount and not path:
            if os.path.isfile(opj(self._creodias_path(data_mount),
                                    'manifest.safe')):
                path = self._creodias_path(data_mount)
            elif os.path.isdir(self._onda_path(data_mount)):
                path = self._onda_path(data_mount)
            elif os.path.isfile(self._mundi_path(data_mount)):
                path = self._mundi_path(data_mount)
            elif os.path.isfile(self._aws_path(data_mount)):
                path = self._aws_path(data_mount)
            else:
                #print(' Scene {} is not found.'.format(self.scene_id))
                path = None

        return path

    # scihub related
    def scihub_uuid(self, opener):

        # construct the basic the url
        base_url = ('https://scihub.copernicus.eu/apihub/odata/v1/'
                    'Products?$filter=')
        action = urllib.request.quote('Name eq \'{}\''.format(self.scene_id))
        # construct the download url
        url = base_url + action

        try:
            # get the request
            req = opener.open(url)
        except URLError as error:
            if hasattr(error, 'reason'):
                print(' We failed to connect to the server.')
                print(' Reason: ', error.reason)
                sys.exit()
            elif hasattr(error, 'code'):
                print(' The server couldn\'t fulfill the request.')
                print(' Error code: ', error.code)
                sys.exit()
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from scihub)
            response = req.read().decode('utf-8')
            uuid = response.split("Products('")[1].split("')")[0]

            # parse the xml page from the response
            # dom = xml.dom.minidom.parseString(response)

            # loop thorugh each entry (with all metadata)
#            for node in dom.getElementsByTagName('entry'):
#                download_url = node.getElementsByTagName(
#                    'id')[0].firstChild.nodeValue
#                uuid = download_url.split('(\'')[1].split('\')')[0]

        return uuid

    def scihub_url(self, opener):

        uuid = self.scihub_uuid(opener)
        # scihub url
        scihub_url = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'
        # construct the download url
        download_url = '{}(\'{}\')/$value'.format(scihub_url, uuid)

        return download_url

    def scihub_md5(self, opener):

        uuid = self.scihub_uuid(opener)
        scihub_url = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'
        download_url = '{}(\'{}\')/Checksum/Value/$value'.format(scihub_url,
                                                                 uuid)
        return download_url

    def scihub_online_status(self, opener):

        uuid = self.scihub_uuid(opener)
        scihub_url = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'

        url = '{}(\'{}\')/Online/$value'.format(scihub_url, uuid)

        try:
            # get the request
            req = opener.open(url)
        except URLError as error:
            if hasattr(error, 'reason'):
                print(' We failed to connect to the server.')
                print(' Reason: ', error.reason)
                sys.exit()
            elif hasattr(error, 'code'):
                print(' The server couldn\'t fulfill the request.')
                print(' Error code: ', error.code)
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

    def scihub_trigger_production(self, opener):

        uuid = self.scihub_uuid(opener)
        scihub_url = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'

        url = '{}(\'{}\')/$value'.format(scihub_url, uuid)

        try:
            # get the request
            req = opener.open(url)

        except URLError as error:
            if hasattr(error, 'reason'):
                print(' We failed to connect to the server.')
                print(' Reason: ', error.reason)
                sys.exit()
            elif hasattr(error, 'code'):
                print(' The server couldn\'t fulfill the request.')
                print(' Error code: ', error.code)
                sys.exit()

        # write the request to to the response variable
        # (i.e. the xml coming back from scihub)
        code = req.getcode()
        if code == 202:
            print(' Production of {} successfully requested.'
                  .format(self.scene_id))

        return code

    # burst part
    def _scihub_annotation_url(self, opener):

        uuid = self.scihub_uuid(opener)

        print(' INFO: Getting URLS of annotation files'
              ' for S1 product: {}.'.format(self.scene_id))
        scihub_url = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'
        anno_path = ('(\'{}\')/Nodes(\'{}.SAFE\')/Nodes(\'annotation\')/'
                     'Nodes'.format(uuid, self.scene_id))
        url = scihub_url + anno_path
        # print(url)
        try:
            # get the request
            req = opener.open(url)
        except URLError as error:
            if hasattr(error, 'reason'):
                print(' We failed to connect to the server.')
                print(' Reason: ', error.reason)
                sys.exit()
            elif hasattr(error, 'code'):
                print(' The server couldn\'t fulfill the request.')
                print(' Error code: ', error.code)
                sys.exit()
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from scihub)
            response = req.read().decode('utf-8')

            # parse the xml page from the response
            dom = xml.dom.minidom.parseString(response)
            url_list = []
            # loop thorugh each entry (with all metadata)
            for node in dom.getElementsByTagName('entry'):
                download_url = node.getElementsByTagName(
                    'id')[0].firstChild.nodeValue

                if download_url[-6:-2] == '.xml':
                    url_list.append('{}/$value'.format(download_url))

        return url_list

    def _burst_database(self, et_root):
        '''
        This functions expects an xml string from a Sentinel-1 SLC
        annotation file and extracts relevant information for burst
        identification as a GeoPandas GeoDataFrame.

        Much of the code is taken from RapidSAR
        package (once upon a time on github).
        '''

        column_names = ['SceneID', 'Track', 'Date', 'SwathID', 'AnxTime',
                        'BurstNr', 'geometry']
        gdf = gpd.GeoDataFrame(columns=column_names)

        track = self.rel_orbit
        acq_date = self.start_date

        # pol = root.find('adsHeader').find('polarisation').text
        swath = et_root.find('adsHeader').find('swath').text
        lines_per_burst = np.int(et_root.find('swathTiming').find(
            'linesPerBurst').text)
        pixels_per_burst = np.int(et_root.find('swathTiming').find(
            'samplesPerBurst').text)
        burstlist = et_root.find('swathTiming').find('burstList')
        geolocation_grid = et_root.find('geolocationGrid')[0]
        first = {}
        last = {}

        # Get burst corner geolocation info
        for geo_point in geolocation_grid:
            if geo_point.find('pixel').text == '0':
                first[geo_point.find('line').text] = np.float32(
                    [geo_point.find('latitude').text,
                     geo_point.find('longitude').text])
            elif geo_point.find('pixel').text == str(pixels_per_burst-1):
                last[geo_point.find('line').text] = np.float32(
                    [geo_point.find('latitude').text,
                     geo_point.find('longitude').text])

        for i, b in enumerate(burstlist):
            firstline = str(i*lines_per_burst)
            lastline = str((i+1)*lines_per_burst)
            azi_anx_time = np.float32(b.find('azimuthAnxTime').text)
            orbit_time = 12*24*60*60/175

            if azi_anx_time > orbit_time:
                azi_anx_time = np.mod(azi_anx_time, orbit_time)

            azi_anx_time = np.int32(np.round(azi_anx_time*10))
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

            geo_dict = {'SceneID': self.scene_id, 'Track': track,
                        'Date': acq_date, 'SwathID': swath,
                        'AnxTime': azi_anx_time, 'BurstNr': i+1,
                        'geometry': loads(wkt)}

            gdf = gdf.append(geo_dict, ignore_index=True)

        return gdf

    def _scihub_annotation_get(self, uname=None, pword=None):

        # define column names fro BUrst DF
        column_names = ['SceneID', 'Track', 'Date', 'SwathID',
                        'AnxTime', 'BurstNr', 'geometry']

        gdf_final = gpd.GeoDataFrame(columns=column_names)

        base_url = 'https://scihub.copernicus.eu/apihub/'

        # get connected to scihub
        opener = scihub.connect(base_url, uname, pword)

        anno_list = self._scihub_annotation_url(opener)

        for url in anno_list:
            try:
                # get the request
                req = opener.open(url)
            except URLError as error:
                if hasattr(error, 'reason'):
                    print(' We failed to connect to the server.')
                    print(' Reason: ', error.reason)
                    sys.exit()
                elif hasattr(error, 'code'):
                    print(' The server couldn\'t fulfill the request.')
                    print(' Error code: ', error.code)
                    sys.exit()
            else:
                # write the request to to the response variable
                # (i.e. the xml coming back from scihub)
                response = req.read().decode('utf-8')

                et_root = ET.fromstring(response)

                # parse the xml page from the response
                gdf = self._burst_database(et_root)

                gdf_final = gdf_final.append(gdf)

        return gdf_final.drop_duplicates(['AnxTime'], keep='first')

    def _zip_annotation_get(self, download_dir, data_mount='/eodata'):

        column_names = ['SceneID', 'Track', 'Date', 'SwathID', 'AnxTime',
                        'BurstNr', 'geometry']

        # crs for empty dataframe
        crs = {'init': 'epsg:4326'}
        gdf_final = gpd.GeoDataFrame(columns=column_names, crs=crs)

        file = self.get_path(download_dir, data_mount)

        # extract info from archive
        archive = zipfile.ZipFile(file, 'r')
        namelist = archive.namelist()
        xml_files = fnmatch.filter(namelist, "*/annotation/s*.xml")

        # loop through xml annotation files
        for xml_file in xml_files:
            xml_string = archive.open(xml_file)

            gdf = self._burst_database(ET.parse(xml_string))
            gdf_final = gdf_final.append(gdf)

        return gdf_final.drop_duplicates(['AnxTime'], keep='first')

    def _safe_annotation_get(self, download_dir, data_mount='/eodata'):

        column_names = ['SceneID', 'Track', 'Date', 'SwathID',
                        'AnxTime', 'BurstNr', 'geometry']
        gdf_final = gpd.GeoDataFrame(columns=column_names)

        for anno_file in glob.glob(
                '{}/annotation/*xml'.format(
                    self.get_path(download_dir=download_dir,
                                  data_mount=data_mount))):

            # parse the xml page from the response
            gdf = self._burst_database(ET.parse(anno_file))
            gdf_final = gdf_final.append(gdf)

        return gdf_final.drop_duplicates(['AnxTime'], keep='first')

    # onda dias uuid extractor
    def ondadias_uuid(self,opener):

        # construct the basic the url
        base_url = ('https://catalogue.onda-dias.eu/dias-catalogue/'
                    'Products?$search=')
        action = '"'+self.scene_id+'.zip"'
        # construct the download url
        url = base_url + action

        try:
            # get the request
            req = opener.open(url)
        except URLError as error:
            if hasattr(error, 'reason'):
                print(' We failed to connect to the server.')
                print(' Reason: ', error.reason)
                sys.exit()
            elif hasattr(error, 'code'):
                print(' The server couldn\'t fulfill the request.')
                print(' Error code: ', error.code)
                sys.exit()
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from onda dias)
            response = req.read().decode('utf-8')

            # parse the uuid from the response (a messy pseudo xml)
            uuid=response.split('":"')[3].split('","')[0]
            #except IndexError as error:
            #    print('Image not available on ONDA DIAS now, please select another repository')
            #    sys.exit()
            # parse the xml page from the response - does not work at present
            """dom = xml.dom.minidom.parseString(response)

            # loop thorugh each entry (with all metadata)
            for node in dom.getElementsByTagName('a:entry'):
                download_url = node.getElementsByTagName(
                    'a:id')[0].firstChild.nodeValue
                uuid = download_url.split('(\'')[1].split('\')')[0]"""

        return uuid

    # other data providers
    def asf_url(self):

        asf_url = 'https://datapool.asf.alaska.edu'

        if self.mission_id == 'S1A':
            mission = 'SA'
        elif self.mission_id == 'S1B':
            mission = 'SB'

        if self.product_type == 'SLC':
            pType = self.product_type
        elif self.product_type == 'GRD':
            pType = 'GRD_{}{}'.format(self.resolution_class, self.pol_mode[0])

        productURL = '{}/{}/{}/{}.zip'.format(asf_url, pType,
                                              mission, self.scene_id)
        return productURL

    def peps_uuid(self, uname, pword):

        url = ('https://peps.cnes.fr/resto/api/collections/S1/search.json?q={}'
               .format(self.scene_id))
        response = requests.get(url, stream=True, auth=(uname, pword))

        # check response
        if response.status_code == 401:
            raise ValueError(' ERROR: Username/Password are incorrect.')
        elif response.status_code != 200:
            response.raise_for_status()

        data = json.loads(response.text)
        peps_uuid = data['features'][0]['id']
        download_url = (data['features'][0]['properties']
                        ['services']['download']['url'])

        return peps_uuid, download_url

    def peps_online_status(self, uname, pword):

        """
        This function will download S1 products from CNES Peps mirror.

        :param url: the url to the file you want to download
        :param fileName: the absolute path to where the downloaded file should
                         be written to
        :param uname: ESA's scihub username
        :param pword: ESA's scihub password
        :return:
        """

        _, url = self.peps_uuid(uname, pword)

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

    # processing related functions
    def get_ard_parameters(self, ard_type='OST Standard'):

        # get path to ost package
        rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
        rootpath = opj(rootpath, 'graphs', 'ard_json')

        template_file = opj(rootpath, '{}.{}.json'.format(
                self.product_type.lower(),
                ard_type.replace(' ', '_').lower()))

        with open(template_file, 'r') as ard_file:
            self.ard_parameters = json.load(ard_file)['processing parameters']


    def set_external_dem(self, dem_file):

        import rasterio

        # check if file exists
        if not os.path.isfile(dem_file):
            print(' ERROR: No dem file found at location {}.'.format(dem_file))
            return

        # get no data value
        with rasterio.open(dem_file) as file:
            dem_nodata = file.nodata

        # get resapmpling
        img_res = self.ard_parameters['single ARD']['dem']['image resampling']
        dem_res = self.ard_parameters['single ARD']['dem']['dem resampling']

        # update ard parameters
        dem_dict = dict({'dem name': 'External DEM',
                         'dem file': dem_file,
                         'dem nodata': dem_nodata,
                         'dem resampling': dem_res ,
                         'image resampling': img_res})
        self.ard_parameters['single ARD']['dem'] = dem_dict

    def update_ard_parameters(self):

        with open (self.proc_file, 'w') as outfile:
            json.dump(dict({'processing parameters': self.ard_parameters}),
                      outfile,
                      indent=4)


    def create_ard(self, infile, out_dir, out_prefix, temp_dir,
                   subset=None, polar='VV,VH,HH,HV'):

        self.proc_file = opj(out_dir, 'processing.json')
        self.update_ard_parameters()
         # check for correctness of ARD paramters


#        self.center_lat = self._get_center_lat(infile)
#        if float(self.center_lat) > 59 or float(self.center_lat) < -59:
#            print(' INFO: Scene is outside SRTM coverage. Will use 30m ASTER'
#                  ' DEM instead.')
#            self.ard_parameters['dem'] = 'ASTER 1sec GDEM'

        #self.ard_parameters['resolution'] = h.resolution_in_degree(
        #    self.center_lat, self.ard_parameters['resolution'])

        if self.product_type == 'GRD':

            # run the processing
            grd_to_ard([infile],
                       out_dir,
                       out_prefix,
                       temp_dir,
                       self.proc_file,
                       subset=subset)

            # write to class attribute
            self.ard_dimap = glob.glob(opj(out_dir, '{}*bs.dim'
                                           .format(out_prefix)))[0]

        elif self.product_type != 'GRD':

            print(' ERROR: create_ard method for single products is currently'
                  ' only available for GRD products')

    def create_rgb(self, outfile, driver='GTiff'):

        # invert ot db from create_ard workflow for rgb creation
        # (otherwise we do it double)
        if self.ard_parameters['single ARD']['to db']:
            to_db = False
        else:
            to_db = True

        ard_to_rgb(self.ard_dimap, outfile, driver, to_db)
        self.ard_rgb = outfile

    def create_rgb_thumbnail(self, outfile, driver='JPEG', shrink_factor=25):

        # invert ot db from create_ard workflow for rgb creation
        # (otherwise we do it double)
        if self.ard_parameters['single ARD']['to db']:
            to_db = False
        else:
            to_db = True

        self.rgb_thumbnail = outfile
        ard_to_thumbnail(self.ard_dimap, self.rgb_thumbnail,
                         driver, shrink_factor, to_db)

    def visualise_rgb(self, shrink_factor=25):

        ras.visualise_rgb(self.ard_rgb, shrink_factor)

    # other functions
    def _get_center_lat(self, scene_path=None):

        if scene_path[-4:] == '.zip':
            zip_archive = zipfile.ZipFile(scene_path)
            manifest = zip_archive.read('{}.SAFE/manifest.safe'
                                                .format(self.scene_id))
        elif scene_path[-5:] == '.SAFE':
            with open(opj(scene_path, 'manifest.safe'), 'rb') as file:
                manifest = file.read()

        root = ET.fromstring(manifest)
        for child in root:
            metadata = child.findall('metadataObject')
            for meta in metadata:
                for wrap in meta.findall('metadataWrap'):
                    for data in wrap.findall('xmlData'):
                        for frameSet in data.findall(
                        '{http://www.esa.int/safe/sentinel-1.0}frameSet'):
                            for frame in frameSet.findall(
                            '{http://www.esa.int/safe/sentinel-1.0}frame'):
                                for footprint in frame.findall(
                                '{http://www.esa.int/'
                                'safe/sentinel-1.0}footPrint'):
                                    for coords in footprint.findall(
                                    '{http://www.opengis.net/gml}'
                                    'coordinates'):
                                        coordinates = coords.text.split(' ')

        sums = 0
        for i, coords in enumerate(coordinates):
            sums = sums + float(coords.split(',')[0])

        return sums / (i + 1)
