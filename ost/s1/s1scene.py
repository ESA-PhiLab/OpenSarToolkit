import os
from datetime import datetime
from os.path import join as opj
import sys
import json
import glob
import logging
from urllib import parse
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

from godale import Executor

from ost.settings import SNAP_S1_RESAMPLING_METHODS
from ost.helpers import scihub, raster as ras
from ost.helpers.helpers import execute_ard
from ost.s1.grd_to_ard import grd_to_ard
from ost.s1.convert_format import ard_to_rgb, ard_to_thumbnail, ard_slc_to_rgb, \
    ard_slc_to_thumbnail
from ost.helpers.bursts import get_bursts_by_polygon


logger = logging.getLogger(__name__)


class Sentinel1Scene:

    def __init__(self, scene_id, ard_type='OST'):
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

        self.timestamp = datetime.strptime(
            self.start_date+'T'+self.start_time, '%Y%m%dT%H%M%S'
        )

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
        self.set_ard_parameters(ard_type)

    def info(self):
        inf_dict = {}
        inf_dict.update(
            Scene_Identifier=str(self.scene_id),
            Satellite=str(self.satellite),
            Acquisition_Mode=str(self.acq_mode),
            Processing_Level=str(self.proc_level),
            Product_Type=str(self.p_type),
            Acquisition_Date=str(self.start_date),
            Start_Time=str(self.start_time),
            Stop_Time=str(self.stop_time),
            Absolute_Orbit=str(self.abs_orbit),
            Relative_Orbit=str(self.rel_orbit),
        )
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
        return inf_dict

    def download(self, download_dir, mirror=None):
        # if not mirror:
        #    logger.debug('INFO: One or more of your scenes need to be downloaded.')
        #    logger.debug('Select the server from where you want to download:')
        #    logger.debug('(1) Copernicus Apihub (ESA, rolling archive)')
        #    logger.debug('(2) Alaska Satellite Facility (NASA, full archive)')
        #    logger.debug('(3) PEPS (CNES, 1 year rolling archive)')
        #    mirror = input(' Type 1, 2 or 3: ')

        from ost.s1 import s1_dl
        df = pd.DataFrame({'identifier': [self.scene_id]})
        s1_dl.download_sentinel1(df, download_dir, mirror=mirror)

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
                   '{}.SAFE'.format(self.scene_id)
                   )
        return path

    def _aws_path(self, data_mount):
        # print('Dummy function for aws path to be added')
        return '/foo/foo/foo'
    
    def _mundi_path(self, mont_point):

        # logger.debug('Dummy function for mundi paths to be added')
        return '/foo/foo/foo'
    
    def _onda_path(self, data_mount):
        path = opj(data_mount, 'S1', 'LEVEL-1',
                   '{}'.format(self.onda_class),
                   self.year,
                   self.month,
                   self.day,
                   '{}.zip'.format(self.scene_id),
                   '{}.SAFE'.format(self.scene_id)
                   )
        return path

    def get_path(self, download_dir=None, data_mount='/eodata'):
        if download_dir:
            if os.path.isfile(self._download_path(download_dir) + '.downloaded'):
                path = self._download_path(download_dir)
            else:
                path = None
        else:
            path = None
        
        if data_mount and not path:
            if os.path.isfile(opj(self._creodias_path(data_mount), 'manifest.safe')):
                path = self._creodias_path(data_mount)
            elif os.path.isdir(self._onda_path(data_mount)):
                path = self._onda_path(data_mount)
            elif os.path.isfile(self._mundi_path(data_mount)):
                path = self._mundi_path(data_mount)
            elif os.path.isfile(self._aws_path(data_mount)):
                path = self._aws_path(data_mount)
            else:
                path = None
        return path

    # scihub related
    def scihub_uuid(self, opener):
        # construct the basic the url
        base_url = ('https://scihub.copernicus.eu/apihub/odata/v1/'
                    'Products?$filter='
                    )
        action = parse.quote('Name eq \'{}\''.format(self.scene_id))
        # construct the download url
        url = base_url + action
        try:
            # get the request
            # requests.get(url, stream=False, auth=(uname, pword))
            req = opener.open(url)
        except URLError as error:
            if hasattr(error, 'reason'):
                logger.error('We failed to connect to the server.')
                logger.error('Reason: ', error.reason)
                sys.exit()
            elif hasattr(error, 'code'):
                logger.error('The server couldn\'t fulfill the request.')
                logger.error('Error code: ', error.code)
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
                logger.debug('We failed to connect to the server.')
                logger.debug('Reason: ', error.reason)
                sys.exit()
            elif hasattr(error, 'code'):
                logger.debug('The server couldn\'t fulfill the request.')
                logger.debug('Error code: ', error.code)
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
                logger.debug('We failed to connect to the server.')
                logger.debug('Reason: ', error.reason)
                sys.exit()
            elif hasattr(error, 'code'):
                logger.debug('The server couldn\'t fulfill the request.')
                logger.debug('Error code: ', error.code)
                sys.exit()

        # write the request to to the response variable
        # (i.e. the xml coming back from scihub)
        code = req.getcode()
        if code == 202:
            logger.debug('Production of {} successfully requested.'
                  .format(self.scene_id))

        return code

    # burst part
    def _scihub_annotation_url(self, opener):
        uuid = self.scihub_uuid(opener)

        logger.debug('INFO: Getting URLS of annotation files'
                     ' for S1 product: {}.'.format(self.scene_id)
                     )
        scihub_url = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'
        anno_path = ('(\'{}\')/Nodes(\'{}.SAFE\')/Nodes(\'annotation\')/'
                     'Nodes'.format(uuid, self.scene_id))
        url = scihub_url + anno_path
        try:
            # get the request
            req = opener.open(url)
        except URLError as error:
            if hasattr(error, 'reason'):
                logger.debug('We failed to connect to the server.')
                logger.debug('Reason: ', error.reason)
                sys.exit()
            elif hasattr(error, 'code'):
                logger.debug('The server couldn\'t fulfill the request.')
                logger.debug('Error code: ', error.code)
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
                    logger.debug('First line not found in annotation file')
                    firstthis = []
            try:
                lastthis = last[lastline]
            except:
                lastline = str(int(lastline)-1)
                try:
                    lastthis = last[lastline]
                except:
                    logger.debug('Last line not found in annotation file')
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
                    logger.debug('We failed to connect to the server.')
                    logger.debug('Reason: ', error.reason)
                    sys.exit()
                elif hasattr(error, 'code'):
                    logger.debug('The server couldn\'t fulfill the request.')
                    logger.debug('Error code: ', error.code)
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

    # other data providers
    def asf_url(self):
        asf_url = 'https://datapool.asf.alaska.edu'
        if self.mission_id == 'S1A':
            mission = 'SA'
        elif self.mission_id == 'S1B':
            mission = 'SB'
        if self.product_type == 'SLC':
            product_type = self.product_type
        elif self.product_type == 'GRD':
            product_type = 'GRD_{}{}'.format(self.resolution_class, self.pol_mode[0])

        return '{}/{}/{}/{}.zip'.format(asf_url,
                                        product_type,
                                        mission,
                                        self.scene_id
                                        )

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
    def set_ard_parameters(self, ard_type='OST'):
        if ard_type == 'OST':
            self.ard_parameters['type'] = ard_type
            self.ard_parameters['resolution'] = 20
            self.ard_parameters['border_noise'] = True
            self.ard_parameters['product_type'] = 'GTCgamma'
            self.ard_parameters['speckle_filter'] = False
            self.ard_parameters['ls_mask_create'] = False
            self.ard_parameters['to_db'] = False
            self.ard_parameters['dem'] = 'SRTM 1Sec HGT'
            self.ard_parameters['resampling'] = SNAP_S1_RESAMPLING_METHODS[2]
        elif ard_type == 'OST Flat':
            self.ard_parameters['type'] = ard_type
            self.ard_parameters['resolution'] = 20
            self.ard_parameters['border_noise'] = True
            self.ard_parameters['product_type'] = 'RTC'
            self.ard_parameters['speckle_filter'] = False
            self.ard_parameters['ls_mask_create'] = True
            self.ard_parameters['to_db'] = False
            self.ard_parameters['dem'] = 'SRTM 1Sec HGT'
            self.ard_parameters['resampling'] = SNAP_S1_RESAMPLING_METHODS[2]
        elif ard_type == 'CEOS':
            self.ard_parameters['type'] = ard_type
            self.ard_parameters['resolution'] = 10
            self.ard_parameters['border_noise'] = True
            self.ard_parameters['product_type'] = 'RTC'
            self.ard_parameters['speckle_filter'] = False
            self.ard_parameters['ls_mask_create'] = False
            self.ard_parameters['to_db'] = False
            self.ard_parameters['dem'] = 'SRTM 1Sec HGT'
            self.ard_parameters['resampling'] = SNAP_S1_RESAMPLING_METHODS[3]
        elif ard_type == 'EarthEngine':
            self.ard_parameters['type'] = ard_type
            self.ard_parameters['resolution'] = 10
            self.ard_parameters['border_noise'] = True
            self.ard_parameters['product_type'] = 'GTCsigma'
            self.ard_parameters['speckle_filter'] = False
            self.ard_parameters['ls_mask_create'] = False
            self.ard_parameters['to_db'] = True
            self.ard_parameters['dem'] = 'SRTM 1Sec HGT'
            self.ard_parameters['resampling'] = SNAP_S1_RESAMPLING_METHODS[3]
        elif ard_type == 'Zhuo':
            self.ard_parameters['type'] = ard_type
            self.ard_parameters['resolution'] = 25
            self.ard_parameters['border_noise'] = False
            self.ard_parameters['product_type'] = 'RTC'
            self.ard_parameters['speckle_filter'] = True
            self.ard_parameters['ls_mask_create'] = True
            self.ard_parameters['to_db'] = True
            self.ard_parameters['dem'] = 'SRTM 1Sec HGT'
            self.ard_parameters['resampling'] = SNAP_S1_RESAMPLING_METHODS[2]

    def create_ard(self, infile, out_dir, out_prefix, temp_dir,
                   subset=None, polar='VV,VH,HH,HV', max_workers=int(os.cpu_count()/2)):
        out_paths = []
        if subset is not None:
            p_poly = loads(subset)
            self.processing_poly = p_poly
            self.center_lat = p_poly.bounds[3]-p_poly.bounds[1]
        else:
            self.processing_poly = None
            try:
                self.center_lat = self._get_center_lat(infile)
            except Exception as e:
                raise
        if float(self.center_lat) > 59 or float(self.center_lat) < -59:
            logger.debug('INFO: Scene is outside SRTM coverage. Will use 30m ASTER'
                         ' DEM instead.'
                         )
            self.ard_parameters['dem'] = 'ASTER 1sec GDEM'
        if self.product_type == 'GRD':
            if not self.ard_parameters:
                logger.debug('INFO: No ARD definition given.'
                             ' Using the OST standard ARD defintion'
                             ' Use object.set_ard_defintion() first if you want to'
                             ' change the ARD defintion.'
                             )
                self.set_ard_parameters('OST')
            if self.ard_parameters['resampling'] not in SNAP_S1_RESAMPLING_METHODS:
                self.ard_parameters['resampling'] = 'BILINEAR_INTERPOLATION'
                logger.debug('WARNING: Invalid resampling method '
                             'using BILINEAR_INTERPOLATION'
                             )

            # we need to convert the infile t a list for the grd_to_ard routine
            infile = [infile]
            out_prefix = out_prefix.replace(' ', '_')
            # run the processing
            return_code = grd_to_ard(
                infile,
                out_dir,
                out_prefix,
                temp_dir,
                self.ard_parameters['resolution'],
                self.ard_parameters['resampling'],
                self.ard_parameters['product_type'],
                self.ard_parameters['ls_mask_create'],
                self.ard_parameters['speckle_filter'],
                self.ard_parameters['dem'],
                self.ard_parameters['to_db'],
                self.ard_parameters['border_noise'],
                subset=subset,
                polarisation=polar
            )
            if return_code != 0:
                raise RuntimeError(
                    'Something went wrong with the GPT processing! '
                    'with return code: %s' % return_code
                )
            # write to class attribute
            self.ard_dimap = glob.glob(opj(out_dir, '{}*TC.dim'
                                           .format(out_prefix)))[0]
            if not os.path.isfile(self.ard_dimap):
                raise RuntimeError
            out_paths.append(self.ard_dimap)

        elif self.product_type == 'SLC':
            # TODO align ARD types with GRD
            """
            Works for only one product at a time, all products are handled as 
            master products in this condition, returning an ARD with 
            the provided ARD parameters!
            """
            if not self.ard_parameters:
                logger.debug('INFO: No ARD definition given.'
                             ' Using the OST standard ARD defintion'
                             ' Use object.set_ard_defintion() first if you want to'
                             ' change the ARD defintion.'
                             )
                self.set_ard_parameters('GTCgamma')
                self.ard_parameters['type'] = 'GTCgamma'
            if self.ard_parameters['resampling'] not in SNAP_S1_RESAMPLING_METHODS:
                self.ard_parameters['resampling'] = 'BILINEAR_INTERPOLATION'
                logger.debug('WARNING: Invalid resampling method '
                             'using BILINEAR_INTERPOLATION'
                             )
            # we need to convert the infile t a list for the grd_to_ard routine
            if subset is not None:
                try:
                    processing_poly = loads(subset)
                    self.processing_poly = processing_poly
                except Exception as e:
                    raise e
            else:
                processing_poly = None
            # get file paths
            master_file = self.get_path(out_dir)
            # get bursts
            master_bursts = self._zip_annotation_get(download_dir=out_dir)
            bursts_dict = get_bursts_by_polygon(
                master_annotation=master_bursts,
                out_poly=processing_poly
            )
            exception_flag = True
            exception_counter = 0
            while exception_flag is True:
                executor_type = 'concurrent_processes'
                executor = Executor(executor=executor_type, max_workers=max_workers)
                if exception_counter > 3 or exception_flag is False:
                    break
                for swath, b in bursts_dict.items():
                    if b != []:
                        try:
                            for task in executor.as_completed(
                                    func=execute_ard,
                                    iterable=b,
                                    fargs=(swath,
                                           master_file,
                                           out_dir,
                                           out_prefix,
                                           temp_dir,
                                           self.ard_parameters
                                           )

                            ):
                                return_code, out_file = task.result()
                                out_paths.append(out_file)
                        except Exception as e:
                            logger.debug(e)
                            max_workers = int(max_workers/2)
                            exception_flag = True
                            exception_counter += 1
                        else:
                            exception_flag = False
                    else:
                        exception_flag = False
                        continue
            self.ard_dimap = out_paths
        else:
            raise RuntimeError('ERROR: create_ard needs S1 SLC or GRD')
        return out_paths

    def create_rgb(self, outfile, process_bounds=None, driver='GTiff'):
        # invert ot db from create_ard workflow for rgb creation
        # (otherwise we do it double)
        logger.debug('Creating RGB Geotiff for scene: %s', self.scene_id)
        if self.ard_parameters['to_db']:
            to_db = False
        else:
            to_db = True
        if self.product_type == 'GRD':
            self.processing_poly = None
            ard_to_rgb(self.ard_dimap, outfile, driver, to_db)
        elif self.product_type == 'SLC':
            if process_bounds is None:
                process_bounds = self.processing_poly.bounds
            ard_slc_to_rgb(self.ard_dimap, outfile, process_bounds, driver)
        self.ard_rgb = outfile
        logger.debug('RGB Geotiff done for scene: %s', self.scene_id)
        return outfile

    def create_rgb_thumbnail(self, outfile, driver='JPEG', shrink_factor=25):
        # invert to db from create_ard workflow for rgb creation
        # (otherwise we do it double)
        if self.product_type == 'GRD':
            if self.ard_parameters['to_db']:
                to_db = False
            else:
                to_db = True
            self.rgb_thumbnail = outfile
            ard_to_thumbnail(
                self.ard_dimap,
                self.rgb_thumbnail,
                driver,
                shrink_factor,
                to_db
            )
        elif self.product_type == 'SLC':
            to_db = False
            self.rgb_thumbnail = outfile
            ard_slc_to_thumbnail(
                self.ard_rgb,
                self.rgb_thumbnail,
                driver,
                shrink_factor
            )
        return outfile

    def visualise_rgb(self, shrink_factor=25):
        ras.visualise_rgb(self.ard_rgb, shrink_factor)

    # other functions
    def _get_center_lat(self, scene_path=None):
        if scene_path.endswith('.zip'):
            zip_archive = zipfile.ZipFile(scene_path)
            manifest = zip_archive.read('{}.SAFE/manifest.safe'
                                        .format(self.scene_id)
                                        )
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
                                '{http://www.esa.int/safe/sentinel-1.0}frameSet'
                        ):
                            for frame in frameSet.findall(
                                    '{http://www.esa.int/safe/sentinel-1.0}frame'
                            ):
                                for footprint in frame.findall(
                                        '{http://www.esa.int/safe/sentinel-1.0}footPrint'
                                ):
                                    for coords in footprint.findall(
                                            '{http://www.opengis.net/gml}coordinates'
                                    ):
                                        coordinates = coords.text.split(' ')
        sums = 0
        for i, coords in enumerate(coordinates):
            sums = sums + float(coords.split(',')[0])
        return sums / (i + 1)
