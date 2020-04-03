import os
import importlib
import json
import glob

import logging
import rasterio
import geopandas as gpd

from os.path import join as opj
from datetime import datetime
from shapely.wkt import loads
from pathlib import Path

from ost.helpers import vector as vec, raster as ras
from ost.helpers import scihub, helpers as h
from ost.helpers.settings import set_log_level, setup_logfile, OST_ROOT
from ost.helpers.settings import check_ard_parameters, exception_handler, config_check

from ost.s1 import search, refine, download, burst, grd_batch
from ost.s1.burst_batch import burst_to_ard_batch

from ost.generic import ard_to_ts, timescan as tscan, mosaic as mos, ts_ls_mask, ts_extent

# get the logger
logger = logging.getLogger(__name__)


class DevNull(object):
    def write(self, arg):
        pass


class Generic:

    def __init__(
            self, project_dir, aoi,
            start='1978-06-28',
            end=datetime.today().strftime("%Y-%m-%d"),
            data_mount=None,
            log_level=logging.INFO
    ):

        # ------------------------------------------
        # 1 Start logger
        # set log level to logging.INFO as standard
        set_log_level(log_level)

        # ------------------------------------------
        # 2 Handle directories

        # get absolute path to project directory
        self.project_dir = Path(project_dir).resolve()

        # create project directory if not existent
        try:
            self.project_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f'Created project directory at {self.project_dir}')
        except FileExistsError:
            logger.info('Project directory already exists. '
                         'No data has been deleted at this point but '
                         'make sure you really want to use this folder.')
            pass

        # define project sub-directories if not set, and create folders
        self.download_dir = self.project_dir.joinpath('download')

        self.download_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            f'Downloaded data will be stored in: {self.download_dir}'
        )

        self.inventory_dir = self.project_dir.joinpath('inventory')

        self.inventory_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            f'Inventory files will be stored in: {self.inventory_dir}'
        )

        self.processing_dir = self.project_dir.joinpath('processing')

        self.processing_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            f'Processed data will be stored in: {self.processing_dir}'
        )

        self.temp_dir = self.project_dir.joinpath('temp')

        self.temp_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            f'Using {self.temp_dir} as directory for temporary files.'
        )

        # ------------------------------------------
        # 3 create a standard logfile
        setup_logfile(self.project_dir.joinpath('.processing.log'))

        # ------------------------------------------
        # 4 handle AOI (read and get back WKT)
        self.aoi = h.aoi_to_wkt(aoi)

        # ------------------------------------------
        # 5 Handle Period of Interest
        try:
            datetime.strptime(start, '%Y-%m-%d')
            self.start = start
        except ValueError:
            raise ValueError("Incorrect date format for start date. "
                             "It should be YYYY-MM-DD")

        try:
            datetime.strptime(end, '%Y-%m-%d')
            self.end = end
        except ValueError:
            raise ValueError("Incorrect date format for end date. "
                             "It should be YYYY-MM-DD")

        # ------------------------------------------
        # 6 Check data mount
        if data_mount:
            if Path(data_mount).exists():
                self.data_mount = Path(data_mount)
            else:
                raise NotADirectoryError(f'{data_mount} is not a directory.')
        else:
            self.data_mount = None

        # ------------------------------------------
        # 7 put all parameters in a dictionary
        self.project_dict = {
            'project_dir': str(self.project_dir),
            'download_dir': str(self.download_dir),
            'inventory_dir': str(self.inventory_dir),
            'processing_dir': str(self.processing_dir),
            'temp_dir': str(self.temp_dir),
            'data_mount': str(self.data_mount),
            'aoi': self.aoi,
            'start_date': self.start,
            'end_date': self.end
        }


class Sentinel1(Generic):
    """ A Sentinel-1 specific subclass of the Generic OST class

    This subclass creates a Sentinel-1 specific
    """

    def __init__(self, project_dir, aoi,
                 start='2014-10-01',
                 end=datetime.today().strftime("%Y-%m-%d"),
                 data_mount=None,
                 product_type='*',
                 beam_mode='*',
                 polarisation='*',
                 log_level=logging.INFO
                 ):

        # ------------------------------------------
        # 1 Get Generic class attributes
        super().__init__(
            project_dir, aoi, start, end, data_mount, log_level
        )

        # ------------------------------------------
        # 2 Check and set product type
        if product_type in ['*', 'RAW', 'SLC', 'GRD']:
            self.product_type = product_type
            self.project_dict.update(product_type=product_type)
        else:
            raise ValueError(
                "Product type must be one out of '*', 'RAW', 'SLC', 'GRD'"
            )

        # ------------------------------------------
        # 3 Check and set beam mode
        if beam_mode in ['*', 'IW', 'EW', 'SM']:
            self.beam_mode = beam_mode
        else:
            raise ValueError("Beam mode must be one out of 'IW', 'EW', 'SM'")

        # ------------------------------------------
        # 4 Check and set polarisations
        possible_pols = ['*', 'VV', 'VH', 'HV', 'HH', 'VV VH', 'HH HV']
        if polarisation in possible_pols:
            self.polarisation = polarisation
        else:
            raise ValueError(
                f"Polarisation must be one out of {possible_pols}"
            )

        # ------------------------------------------
        # 5 Initialize the inventory file
        inventory_file = self.inventory_dir.joinpath('full.inventory.gpkg')
        if inventory_file.exists():
            self.inventory_file = inventory_file
            logging.info(
                'Found an existing inventory file. This can be overwritten '
                'by re-executing the search.'
            )
            self.read_inventory()
        else:
            self.inventory = None
            self.inventory_file = None

        # ------------------------------------------
        # 6 Initialize refinements
        self.refined_inventory_dict = None
        self.coverages = None

        # ------------------------------------------
        # 7 Initialize burst inventories
        if product_type == 'SLC':
            self.burst_inventory = None
            self.burst_inventory_file = self.inventory_dir.joinpath(
                'burst.inventory.gpkg'
            )
        else:
            self.burst_inventory = None
            self.burst_inventory_file = None

        # ------------------------------------------
        # 8 create a dictionary for project dict
        self.inventory_dict = dict(
            {'product_type': self.product_type,
             'beam_mode': self.beam_mode,
             'polarisation': self.polarisation,
             # 'full_inventory': self.inventory,
             'full_inventory_file': str(self.inventory_file),
             'refine': False,
             # 'refined_dict': self.refined_inventory_dict,
             'selected_refine_key': None,
             # 'coverages': self.coverages,
             # 'burst_inventory': self.burst_inventory,
             'burst_inventory_file': str(self.burst_inventory_file),
             'refine_burst': False
             }
        )

    # ------------------------------------------
    # methods
    def search(self, outfile='full.inventory.gpkg', append=False,
               uname=None, pword=None):
        """

        :param outfile:
        :param append:
        :param uname:
        :param pword:
        :return:
        """

        # create scihub conform aoi string
        aoi_str = scihub.create_aoi_str(self.aoi)

        # create scihub conform TOI
        toi_str = scihub.create_toi_str(self.start, self.end)

        # create scihub conform product specification
        product_specs_str = scihub.create_s1_product_specs(
            self.product_type, self.polarisation, self.beam_mode
        )

        # join the query
        query = scihub.create_query(
            'Sentinel-1', aoi_str, toi_str, product_specs_str
        )

        if not uname or not pword:
            # ask for username and password
            uname, pword = scihub.ask_credentials()

        # do the search
        if outfile == 'full.inventory.gpkg':
            self.inventory_file = self.inventory_dir.joinpath(
                'full.inventory.gpkg'
            )
        else:
            Path(outfile)

        search.scihub_catalogue(
            query, self.inventory_file, append, uname, pword
        )

        if self.inventory_file.exists():
            # read inventory into the inventory attribute
            self.read_inventory()
        else:
            print('No images found in the AOI for this date range')

    def read_inventory(self):
        """Read the Sentinel-1 data inventory from a OST invetory shapefile

        :param

        """

        # define column names of inventory file (since in shp they are
        # truncated)
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
        # add download_path to inventory, so we can check if data needs to be
        # downloaded
        self.inventory = search.check_availability(
            geodataframe, self.download_dir, self.data_mount)

    def download_size(self, inventory_df=None):

        if inventory_df is None:
            download_size = self.inventory[
                'size'].str.replace(' GB', '').astype('float32').sum()
        else:
            download_size = inventory_df[
                'size'].str.replace(' GB', '').astype('float32').sum()

        logger.info(
            f'There are about {download_size} GB need to be downloaded.'
        )

    def refine_inventory(self,
                         exclude_marginal=True,
                         full_aoi_crossing=True,
                         mosaic_refine=True,
                         area_reduce=0.05,
                         complete_coverage=True):

        self.refined_inventory_dict, self.coverages = refine.search_refinement(
            self.aoi,
            self.inventory,
            self.inventory_dir,
            exclude_marginal=exclude_marginal,
            full_aoi_crossing=full_aoi_crossing,
            mosaic_refine=mosaic_refine,
            area_reduce=area_reduce,
            complete_coverage=complete_coverage
        )

        # summing up information
        print('--------------------------------------------')
        print(' Summing up the info about mosaics')
        print('--------------------------------------------')
        for key in self.refined_inventory_dict:
            print('')
            print(f' {self.coverages[key]} mosaics for mosaic key {key}')

    def download(self, inventory_df, mirror=None, concurrent=2,
                 uname=None, pword=None):

        # if an old inventory exists drop download_path
        if 'download_path' in inventory_df:
            inventory_df.drop('download_path', axis=1)

        # check if scenes exist
        inventory_df = search.check_availability(
            inventory_df, self.download_dir, self.data_mount)

        # extract only those scenes that need to be downloaded
        download_df = inventory_df[inventory_df.download_path.isnull()]

        # to download or not ot download - that is here the question
        if not download_df.any().any():
            logger.info('All scenes are ready for being processed.')
        else:
            logger.info('One or more scene(s) need(s) to be downloaded.')
            download.download_sentinel1(
                download_df,
                self.download_dir,
                mirror=mirror,
                concurrent=concurrent,
                uname=uname,
                pword=pword
            )

    def create_burst_inventory(self,
                               inventory_df=None,
                               refine=True,
                               outfile=None,
                               ):

        # assert SLC product type
        if not self.product_type == 'SLC':
            raise ValueError(
                'Burst inventory is only possible for the SLC product type'
            )

        # in case a custom inventory is given (e.g. a refined inventory)
        if inventory_df is None:
            inventory_df = self.inventory
        else:
            # assert that all products are SLCs
            if not inventory_df.product_type.unique() == 'SLC':
                raise ValueError(
                    'The inventory dataframe can only contain SLC products '
                    'for the burst inventory '
                )

        if not outfile:
            outfile = self.burst_inventory_file

        # run the burst inventory
        self.burst_inventory = burst.burst_inventory(
            inventory_df=inventory_df,
            outfile=outfile,
            download_dir=self.download_dir,
            data_mount=self.data_mount,
        )

        # refine the burst inventory
        if refine:
            self.burst_inventory = burst.refine_burst_inventory(
                self.aoi,
                self.burst_inventory,
                outfile=outfile
            )

    def read_burst_inventory(self, burst_file=None):
        """

        :param burst_file: a GeoPackage file created by OST holding a burst
                           inventory
        :return: geodataframe
        """
        if not burst_file:
            burst_file = self.inventory_dir.joinpath('burst_inventory.gpkg')

        # define column names of file (since in shp they are truncated)
        # create column names for empty data frame
        column_names = ['SceneID', 'Track', 'Direction', 'Date', 'SwathID',
                        'AnxTime', 'BurstNr', 'geometry']

        geodataframe = gpd.read_file(burst_file)
        geodataframe.columns = column_names
        self.burst_inventory = geodataframe

        return geodataframe

    def plot_inventory(self, inventory_df=None, transparency=0.05,
                       annotate=False):

        if inventory_df is None:
            vec.plot_inventory(
                self.aoi, self.inventory, transparency, annotate
            )
        else:
            vec.plot_inventory(self.aoi, inventory_df, transparency, annotate)


class Sentinel1Batch(Sentinel1):
    """ A Sentinel-1 specific subclass of the Generic OST class

    This subclass creates a Sentinel-1 specific
    """

    def __init__(self, project_dir, aoi,
                 start='2014-10-01',
                 end=datetime.today().strftime("%Y-%m-%d"),
                 data_mount=None,
                 product_type='SLC',
                 beam_mode='IW',
                 polarisation='*',
                 ard_type='OST-GTC',
                 log_level=logging.INFO
                 ):
        # ------------------------------------------
        # 1 Initialize super class
        super().__init__(
            project_dir, aoi, start, end, data_mount,
            product_type, beam_mode, polarisation,
            log_level
        )
        if product_type == 'GRD':
            cpus_per_process = os.cpu_count()*2
        elif product_type == 'SLC':
            cpus_per_process = 2

        self.ard_type = ard_type
        self.get_ard_parameters()
        # add information to the generic class self.project dict
        self.project_dict.update(
            cpus_per_process=cpus_per_process,
            inventory=self.inventory_dict,
            processing_parameters=self.ard_parameters
        )

        # ---------------------------------------
        # 1 Check and set ARD type

        # possible ard types to select from for GRD
        if product_type == 'GRD':
            ard_types_grd = ['CEOS', 'Earth-Engine', 'OST-GTC', 'OST-RTC']
            if ard_type in ard_types_grd:
                self.ard_type = ard_type
            else:
                raise ValueError('No valid ARD type for product type GRD.'
                                 f'Select from {ard_types_grd}')

        # possible ard types to select from for GRD
        elif product_type == 'SLC':

            ard_types_slc = ['OST-GTC', 'OST-RTC', 'OST-COH', 'OST-RTCCOH',
                             'OST-POL', 'OST-ALL']

            if ard_type in ard_types_slc:
                self.ard_type = ard_type
            else:
                raise ValueError('No valid ARD type for product type GRD.'
                                 f'Select from {ard_types_slc}')

        # otherwise the product type is not supported
        else:
            raise ValueError(f'Product type {self.product_type} not '
                             f'supported for processing. Only GRD and SLC '
                             f'are supported.')

        # ---------------------------------------
        # 2 Check beam mode
        if not beam_mode == 'IW':
            raise ValueError("Only 'IW' beam mode supported for processing.")

        # ---------------------------------------
        # 3 Set up project JSON

        # find respective template for selected ARD type
        template_file = OST_ROOT.joinpath(
            f"graphs/ard_json/{self.product_type.lower()}"
            f".{ard_type.replace('-', '_').lower()}.json"
        )
        # open and load parameters
        with open(template_file, 'r') as ard_file:
            self.ard_parameters = json.load(ard_file)['processing_parameters']

        # define project file
        self.project_json = self.project_dir.joinpath('project.json')
        with open(self.project_json, 'w+') as out:
            json.dump(self.project_dict, out, indent=4)

    # ---------------------------------------
    # methods

    # processing related functions
    def get_ard_parameters(self):
        # get path to graph
        # get path to ost package
        rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
        rootpath = opj(rootpath, 'graphs', 'ard_json')

        template_file = opj(rootpath, '{}.{}.json'.format(
            self.product_type.lower(),
            self.ard_type.replace('-', '_').lower()))

        with open(template_file, 'r') as ard_file:
            self.ard_parameters = json.load(ard_file)['processing_parameters']

    def update_ard_parameters(self):
        # check for correctness of ard parameters
        check_ard_parameters(self.ard_parameters)
        if self.ard_type != self.ard_parameters['single_ARD']['type']:
            self.ard_type = self.ard_parameters['single_ARD']['type']
            self.get_ard_parameters()

        # re-create project dict with update ard parameters
        self.project_dict.update(
            inventory=self.inventory_dict,
            processing_parameters=self.ard_parameters
        )
        # dump to json file
        with open(self.project_json, 'w') as outfile:
            json.dump(self.project_dict, outfile, indent=4)

    def set_external_dem(self, dem_file, ellipsoid_correction=True):
        # check if file exists
        if not Path(dem_file).eixtst():
            raise FileNotFoundError(f'No file found at {dem_file}.')

        # get no data value
        with rasterio.open(dem_file) as file:
            dem_nodata = file.nodata

        # get resampling
        img_res = self.ard_parameters['single_ARD']['dem']['image_resampling']
        dem_res = self.ard_parameters['single_ARD']['dem']['dem_resampling']

        # update ard parameters
        dem_dict = dict({'dem_name': 'External DEM',
                         'dem_file': dem_file,
                         'dem_nodata': dem_nodata,
                         'dem_resampling': dem_res,
                         'image_resampling': img_res,
                         'egm_correction': ellipsoid_correction,
                         'out_projection': 'WGS84(DD)'
                         })

        # update ard_parameters
        self.ard_parameters['single_ARD']['dem'] = dem_dict

    def bursts_to_ard(
            self,
            timeseries=False,
            timescan=False,
            mosaic=False,
            overwrite=False,
            cut_to_aoi=False,
            ncores=os.cpu_count()
    ):
        # in case we strat from scratch, delete all data
        # within processing folder
        if overwrite:
            logger.info('Deleting processing folder to start from scratch')
            h.remove_folder_content(self.processing_dir)

        # --------------------------------------------
        # 2 Check if within SRTM coverage
        # set ellipsoid correction and force GTC production
        # when outside SRTM
        self.center_lat = loads(self.aoi).centroid.y
        if float(self.center_lat) > 59 or float(self.center_lat) < -59:
            logger.info('Scene is outside SRTM coverage. Will use '
                        'ellipsoid based terrain correction.')
            self.ard_parameters['single_ARD']['geocoding'] = 'ellipsoid'

        # --------------------------------------------
        # 3 Check ard parameters in case they have been updated,
        #   and write them to json file
        self.update_ard_parameters()

        # --------------------------------------------
        # 4 set resolution to degree
        # self.ard_parameters['resolution'] = h.resolution_in_degree(
        #    self.center_lat, self.ard_parameters['resolution'])

        # --------------------------------------------
        # 5 run the burst to ard batch routine
        # check and retry function
        burst_to_ard_batch(burst_inv=self.burst_inventory,
                           project_dict=self.project_dict,
                           ncores=ncores
                           )
        # --------------------------------------------
        # 6 run the timeseries creation
        if timeseries or timescan:
            burst.ards_to_timeseries(self.burst_inventory, self.project_json)

        # --------------------------------------------
        # 7 run the timescan creation
        if timescan:
            burst.timeseries_to_timescan(
                self.burst_inventory, self.project_json
            )

        # --------------------------------------------
        # 8 mosaic the time-series
        if mosaic and timeseries:
            burst.mosaic_timeseries(self.burst_inventory, self.project_json)

        # --------------------------------------------
        # 9 mosaic the timescans
        if mosaic and timescan:
            burst.mosaic_timescan(self.burst_inventory, self.project_json)

    def create_timeseries_animation(
            self,
            timeseries_dir, product_list,
            outfile,
            shrink_factor=1,
            resampling_factor=5,
            duration=1,
            add_dates=False,
            prefix=False
    ):
        ras.create_timeseries_animation(timeseries_dir, product_list, outfile,
                                        shrink_factor=shrink_factor,
                                        duration=duration,
                                        resampling_factor=resampling_factor,
                                        add_dates=add_dates, prefix=prefix)

    def grds_to_ard(
            self,
            timeseries=False,
            timescan=False,
            mosaic=False,
            overwrite=False,
            cut_to_aoi=False
    ):
        self.update_ard_parameters()

        if overwrite:
            logger.info('Deleting processing folder to start from scratch')
            h.remove_folder_content(self.processing_dir)

        # set resolution in degree
        #        self.center_lat = loads(self.aoi).centroid.y
        #        if float(self.center_lat) > 59 or float(self.center_lat) < -59:
        #            logger.info('Scene is outside SRTM coverage. Will use 30m ASTER'
        #                  ' DEM instead.')
        #            self.ard_parameters['dem'] = 'ASTER 1sec GDEM'

        # the grd to ard batch routine
        grd_batch.grd_to_ard_batch(
            inventory_df=self.inventory,
            download_dir=self.download_dir,
            processing_dir=self.processing_dir,
            temp_dir=self.temp_dir,
            project_dict=self.project_dict,
            subset=self.aoi,
            )

        # time-series part
        if timeseries or timescan:

            nr_of_processed = len(
                glob.glob(opj(self.processing_dir, '*',
                              'Timeseries', '.*processed')))

            nr_of_polar = len(
                self.inventory.polarisationmode.unique()[0].split(' '))
            nr_of_tracks = len(self.inventory.relativeorbit.unique())
            nr_of_ts = nr_of_polar * nr_of_tracks

            # check and retry function
            i = 0
            while nr_of_ts > nr_of_processed:

                grd_batch.ards_to_timeseries(self.inventory,
                                             self.processing_dir,
                                             self.temp_dir,
                                             self.proc_file,
                                             )

                nr_of_processed = len(
                    glob.glob(opj(self.processing_dir, '*',
                                  'Timeseries', '.*processed')))
                i += 1

                # not more than 5 trys
                if i == 5:
                    break

        if timescan:

            # number of already processed timescans
            nr_of_processed = len(glob.glob(opj(
                self.processing_dir, '*', 'Timescan', '.*processed')))

            # number of expected timescans
            nr_of_polar = len(
                self.inventory.polarisationmode.unique()[0].split(' '))
            nr_of_tracks = len(self.inventory.relativeorbit.unique())
            nr_of_ts = nr_of_polar * nr_of_tracks

            i = 0
            while nr_of_ts > nr_of_processed:

                grd_batch.timeseries_to_timescan(
                    self.inventory,
                    self.processing_dir,
                    self.proc_file)

                nr_of_processed = len(glob.glob(opj(
                    self.processing_dir, '*', 'Timescan', '.*processed')))

                i += 1

                # not more than 5 trys
                if i == 5:
                    break

        if cut_to_aoi:
            cut_to_aoi = self.aoi

        if mosaic and timeseries:
            grd_batch.mosaic_timeseries(
                self.inventory,
                self.processing_dir,
                self.temp_dir,
                cut_to_aoi
            )
