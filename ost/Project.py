# -*- coding: utf-8 -*-

# import standard libs
import os
import sys
import glob
import logging
import geopandas as gpd

# create the opj alias to handle independent os paths
from os.path import join as opj
from datetime import datetime
from shapely.wkt import loads

from .s1 import search, refine, download, burst
from .helpers import scihub, helpers as h
from .helpers import vector as vec

# set logging
logging.basicConfig(stream=sys.stdout,
                    format='%(levelname)s:%(message)s',
                    level=logging.INFO)


class Generic():

    def __init__(self, project_dir, aoi,
                 start='1978-06-28',
                 end=datetime.today().strftime("%Y-%m-%d")):

        self.project_dir = os.path.abspath(project_dir)
        self.start = start
        self.end = end

        # handle the import of different aoi formats and transform
        # to a WKT string
        if aoi.split('.')[-1] != 'shp' and len(aoi) == 3:

            # get lowres data
            world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
            country = world.name[world.iso_a3 == aoi].values[0]
            print(' INFO: Getting the country boundaries from Geopandas low'
                  ' resolution data for {}'.format(country))

            self.aoi = (world['geometry']
                        [world['iso_a3'] == aoi].values[0].to_wkt())
        elif aoi.split('.')[-1] == 'shp':
            self.aoi = str(vec.shp_to_wkt(aoi))
            print(' INFO: Using {} shapefile as Area of Interest definition.')
        else:
            try:
                loads(str(aoi))
            except:
                print(' ERROR: No valid OST AOI defintion.')
                sys.exit()
            else:
                self.aoi = aoi

    def create_project_dir(self, if_not_empty=True):
        '''Creates the high-lvel project directory

        :param instance attribute project_dir

        :return None
        '''

        if os.path.isdir(self.project_dir):
            logging.warning(' Project directory already exists.'
                            ' No data has been deleted at this point but'
                            ' make sure you really want to use this folder.')
        else:

            os.makedirs(self.project_dir, exist_ok=True)
            logging.info(' Created project directory at {}'
                         .format(self.project_dir))

    def create_download_dir(self, download_dir=None):
        '''Creates the high-level download directory

        :param instance attribute download_dir or
               default value (i.e. /path/to/project_dir/download)

        :return None
        '''

        if download_dir is None:
            self.download_dir = opj(self.project_dir, 'download')
        else:
            self.download_dir = download_dir

        os.makedirs(self.download_dir, exist_ok=True)
        logging.info(' Downloaded data will be stored in:{}'
                     .format(self.download_dir))

    def create_processing_dir(self, processing_dir=None):
        '''Creates the high-level processing directory

        :param instance attribute processing_dir or
               default value (i.e. /path/to/project_dir/processing)

        :return None
        '''

        if processing_dir is None:
            self.processing_dir = opj(self.project_dir, 'processing')
        else:
            self.processing_dir = processing_dir

        os.makedirs(self.processing_dir, exist_ok=True)
        logging.info(' Processed data will be stored in: {}'
                     .format(self.processing_dir))

    def create_inventory_dir(self, inventory_dir=None):
        '''Creates the high-level inventory directory

        :param instance attribute inventory_dir or
               default value (i.e. /path/to/project_dir/inventory)

        :return None
        '''

        if inventory_dir is None:
            self.inventory_dir = opj(self.project_dir, 'inventory')
        else:
            self.inventory_dir = inventory_dir

        os.makedirs(self.inventory_dir, exist_ok=True)
        logging.info(' Inventory files will be stored in: {}'
                     .format(self.inventory_dir))

    def create_temporary_dir(self, temp_dir=None):
        '''Creates the high-level temporary directory

        :param instance attribute temp_dir or
               default value (i.e. /path/to/project_dir/temp)

        :return None
        '''
        if temp_dir is None:
            self.temp_dir = opj(self.project_dir, 'temp')
        else:
            self.temp_dir = temp_dir

        os.makedirs(self.temp_dir, exist_ok=True)
        logging.info(' Using {} as  directory for temporary files.'
                     .format(self.temp_dir))

    def create_directory_structure(self, project_dir=None, download_dir=None,
                                   inventory_dir=None, processing_dir=None,
                                   temp_dir=None):

        logging.info(' Setting up the directory structure of the project.')
        self.create_project_dir()
        self.create_download_dir(download_dir)
        self.create_inventory_dir(inventory_dir)
        self.create_processing_dir(processing_dir)
        self.create_temporary_dir(temp_dir)


class Sentinel1(Generic):
    ''' A Sentinel-1 specific subclass of the Generic OST class

    This subclass creates a Sentinel-1 specific
    '''

    def __init__(self, project_dir, aoi,
                 start='2014-10-01',
                 end=datetime.today().strftime("%Y-%m-%d"),
                 product_type='SLC',
                 beam_mode='IW',
                 polarisation='*'
                 ):

        super().__init__(project_dir, aoi, start, end)
        self.product_type = product_type
        self.beam_mode = beam_mode
        self.polarisation = polarisation

        self.ard_parameters = {}
        self.inventory = None
        self.inventory_file = None
        self.refined_inventory_dict = None
        self.coverages = None

    def search(self, outfile='full.inventory.shp', append=False,
               uname=None, pword=None):

        # create scihub conform aoi string
        aoi_str = scihub.create_aoi_str(self.aoi)

        # create scihub conform TOI
        toi_str = scihub.create_toi_str(self.start, self.end)

        # create scihub conform product specification
        product_specs_str = scihub.create_s1_product_specs(
                self.product_type, self.polarisation, self.beam_mode)

        # join the query
        query = scihub.create_query('Sentinel-1', aoi_str, toi_str,
                                    product_specs_str)

        if not uname or not pword:
            # ask for username and password
            uname, pword = scihub.ask_credentials()

        # do the search
        self.inventory_file = opj(self.inventory_dir, outfile)
        search.scihub_catalogue(query, self.inventory_file, append,
                                uname, pword)

        # read inventory into the inventoryGdf attribute
        self.read_inventory()

    def read_inventory(self):
        '''Read the Sentinel-1 data inventory from a OST invetory shapefile

        :param

        '''

#       define column names of inventory file (since in shp they are truncated)
        column_names = ['id', 'identifier', 'polarisationmode',
                        'orbitdirection', 'acquisitiondate', 'relativeorbit',
                        'orbitnumber', 'product_type', 'slicenumber', 'size',
                        'beginposition', 'endposition',
                        'lastrelativeorbitnumber', 'lastorbitnumber',
                        'uuid', 'platformidentifier', 'missiondatatakeid',
                        'swathidentifier', 'ingestiondate',
                        'sensoroperationalmode', 'geometry']

        geodataframe = gpd.read_file(self.inventory_file)
        geodataframe.columns = column_names
        self.inventory = geodataframe

        return geodataframe

    def download_size(self, inventory_df=None):

        if inventory_df:
            download_size = inventory_df[
                'size'].str.replace(' GB', '').astype('float32').sum()
        else:
            download_size = self.inventory[
                'size'].str.replace(' GB', '').astype('float32').sum()

        print(' INFO: There are about {} GB need to be downloaded.'.format(
                download_size))

    def refine(self,
               exclude_marginal=True,
               full_aoi_crossing=True,
               mosaic_refine=True,
               area_reduce=0.05):

        self.refined_inventory_dict, self.coverages = refine.search_refinement(
                                       self.aoi,
                                       self.read_inventory(),
                                       self.inventory_dir,
                                       exclude_marginal=exclude_marginal,
                                       full_aoi_crossing=full_aoi_crossing,
                                       mosaic_refine=mosaic_refine,
                                       area_reduce=area_reduce)

    def download(self, inventory_df, mirror=None, concurrent=2,
                 uname=None, pword=None):

        download.download_sentinel1(inventory_df,
                                    self.download_dir,
                                    mirror=mirror,
                                    concurrent=concurrent,
                                    uname=uname,
                                    pword=pword)

    def burst_inventory_(self, key=None, refine=True):

        if key:
            self.burst_inventory = burst.burst_inventory(
                self.refined_inventory_dict[key],
                download_dir=self.download_dir)
        else:
            self.burst_inventory = burst.burst_inventory(
                    self.inventory, download_dir=self.download_dir)

        if refine:
            self.burst_inventory = burst.refine_burst_inventory(
                    self.aoi, self.burst_inventory)



    def set_ard_definition(self, ard_type='OST Plus'):

        if ard_type == 'OST Plus':

            # scene specific
            self.ard_parameters['type'] = ard_type
            self.ard_parameters['resolution'] = 20
            self.ard_parameters['border_noise'] = False
            self.ard_parameters['product_type'] = 'RTC'
            self.ard_parameters['to_db'] = False
            self.ard_parameters['speckle_filter'] = False
            self.ard_parameters['pol_speckle_filter'] = False
            self.ard_parameters['ls_mask_create'] = False
            self.ard_parameters['ls_mask_apply'] = False
            self.ard_parameters['dem'] = 'SRTM 1Sec HGT'
            self.ard_parameters['coherence'] = True
            self.ard_parameters['polarimetry'] = True

            # timeseries specific
            self.ard_parameters['to_db_mt'] = True
            self.ard_parameters['mt_speckle_filter'] = True
            self.ard_parameters['datatype'] = 'float32'

            # timescan specific
            self.ard_parameters['metrics'] = ['avg', 'max', 'min',
                                              'std', 'cov']
            self.ard_parameters['outlier_removal'] = True

        elif ard_type == 'Zhuo':

            # scene specific
            self.ard_parameters['type'] = ard_type
            self.ard_parameters['resolution'] = 25
            self.ard_parameters['border_noise'] = False
            self.ard_parameters['product_type'] = 'RTC'
            self.ard_parameters['to_db'] = False
            self.ard_parameters['speckle_filter'] = True
            self.ard_parameters['pol_speckle_filter'] = True
            self.ard_parameters['ls_mask_create'] = False
            self.ard_parameters['ls_mask_apply'] = False
            self.ard_parameters['dem'] = 'SRTM 1Sec HGT'
            self.ard_parameters['coherence'] = False
            self.ard_parameters['polarimetry'] = True

            # timeseries specific
            self.ard_parameters['to_db_mt'] = False
            self.ard_parameters['mt_speckle_filter'] = False
            self.ard_parameters['datatype'] = 'float32'

            # timescan specific
            self.ard_parameters['metrics'] = ['avg', 'max', 'min',
                                              'std', 'cov']
            self.ard_parameters['outlier_removal'] = False

        # assure that we do not convert twice to dB
        if self.ard_parameters['to_db']:
            self.ard_parameters['to_db_mt'] = False

    def burst_to_ard(self, timeseries=False, timescan=False, mosaic=False,
                     overwrite=False):

        if overwrite:
            print(' INFO: Deleting processing folder to start from scratch')
            h.remove_folder_content(self.processing_dir)

        if not self.ard_parameters:
            self.set_ard_definition()

        # set resolution in degree
        self.center_lat = loads(self.aoi).centroid.y
        if float(self.center_lat) > 59 or float(self.center_lat) < -59:
            print(' INFO: Scene is outside SRTM coverage. Will use 30m ASTER'
                  ' DEM instead.')
            self.ard_parameters['dem'] = 'ASTER 1sec GDEM'

        # set resolution to degree
        self.ard_parameters['resolution'] = h.resolution_in_degree(
            self.center_lat, self.ard_parameters['resolution'])

        nr_of_processed = len(
            glob.glob(opj(self.processing_dir, '*', '*', '.processed')))

        # check and retry function
        i = 0
        while len(self.burst_inventory) > nr_of_processed:

            burst.burst_to_ard_batch(self.burst_inventory,
                                     self.download_dir,
                                     self.processing_dir,
                                     self.temp_dir,
                                     self.ard_parameters)

            nr_of_processed = len(
                glob.glob(opj(self.processing_dir, '*', '*', '.processed')))

            i += 1

            # not more than 5 trys
            if i == 5:
                break

        # do we delete the downloads here?
        if timeseries or timescan:
            burst.burst_ards_to_timeseries(self.burst_inventory,
                                           self.processing_dir,
                                           self.temp_dir,
                                           self.ard_parameters)

            # do we deleete the single ARDs here?
            if timescan:
                burst.timeseries_to_timescan(self.burst_inventory,
                                             self.processing_dir,
                                             self.temp_dir,
                                             self.ard_parameters)

        if mosaic and timeseries:
            burst.mosaic_timescan(self.burst_inventory,
                                  self.processing_dir,
                                  self.temp_dir,
                                  self.ard_parameters)

        if mosaic and timescan:
            burst.mosaic_timescan(self.burst_inventory,
                                  self.processing_dir,
                                  self.temp_dir,
                                  self.ard_parameters)

    def plot_inventory(self, inventory_df=None, transperancy=0.05):

        if inventory_df is None:
            vec.plot_inventory(self.aoi, self.inventory, transperancy)
        else:
            vec.plot_inventory(self.aoi, inventory_df, transperancy)
