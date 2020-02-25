# -*- coding: utf-8 -*-

# import standard libs
import os
import importlib
import json
import glob
import logging
import geopandas as gpd
import multiprocessing
# create the opj alias to handle independent os paths
from os.path import join as opj
from datetime import datetime
from shapely.wkt import loads
from joblib import Parallel, delayed
from ost.helpers import vector as vec, raster as ras
from ost.s1 import search, refine, download, burst, grd_batch
from ost.helpers import scihub, helpers as h
from ost.multitemporal import ard_to_ts, common_extent, common_ls_mask, timescan as tscan
from ost.mosaic import mosaic as mos
import sys

# set logging
logging.basicConfig(stream=sys.stdout,
                    format='%(levelname)s:%(message)s',
                    level=logging.INFO)
class DevNull(object):
    def write(self, arg):
        pass


class Generic():

    def __init__(self, project_dir, aoi,
                 start='1978-06-28',
                 end=datetime.today().strftime("%Y-%m-%d"),
                 data_mount=None,
                 download_dir=None,
                 inventory_dir=None,
                 processing_dir=None,
                 temp_dir=None):

        self.project_dir = os.path.abspath(project_dir)
        self.start = start
        self.end = end
        self.data_mount = data_mount
        self.download_dir = download_dir
        self.inventory_dir = inventory_dir
        self.processing_dir = processing_dir
        self.temp_dir = temp_dir

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

        if not self.download_dir:
            self.download_dir = opj(project_dir, 'download')
        if not self.inventory_dir:
            self.inventory_dir = opj(project_dir, 'inventory')
        if not self.processing_dir:
            self.processing_dir = opj(project_dir, 'processing')
        if not self.temp_dir:
            self.temp_dir = opj(project_dir, 'temp')

        self._create_project_dir()
        self._create_download_dir(self.download_dir)
        self._create_inventory_dir(self.inventory_dir)
        self._create_processing_dir(self.processing_dir)
        self._create_temp_dir(self.temp_dir)

    def _create_project_dir(self, if_not_empty=True):
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

    def _create_download_dir(self, download_dir=None):
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

    def _create_processing_dir(self, processing_dir=None):
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

    def _create_inventory_dir(self, inventory_dir=None):
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

    def _create_temp_dir(self, temp_dir=None):
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


class Sentinel1(Generic):
    ''' A Sentinel-1 specific subclass of the Generic OST class

    This subclass creates a Sentinel-1 specific
    '''
    
    def __init__(self, project_dir, aoi,
                 start='2014-10-01',
                 end=datetime.today().strftime("%Y-%m-%d"),
                 data_mount='/eodata',
                 download_dir=None,
                 inventory_dir=None,
                 processing_dir=None,
                 temp_dir=None,
                 product_type='*',
                 beam_mode='*',
                 polarisation='*'
                 ):

        super().__init__(project_dir, aoi, start, end, data_mount,
                         download_dir, inventory_dir, processing_dir, temp_dir)

        self.product_type = product_type
        self.beam_mode = beam_mode
        self.polarisation = polarisation

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
        
        if os.path.exists(self.inventory_file):
            # read inventory into the inventory attribute
            self.read_inventory()
        else:
            print('No images found in the AOI for this date range')

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
        
        # add download_path to inventory, so we can check if data needs to be 
        # downloaded
        self.inventory = search.check_availability(
            geodataframe, self.download_dir, self.data_mount)
        
        # return geodataframe

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
                                       complete_coverage=complete_coverage)

        # summing up information
        print('--------------------------------------------')
        print(' Summing up the info about mosaics')
        print('--------------------------------------------')
        for key in self.refined_inventory_dict:
            print('')
            print(' {} mosaics for mosaic key {}'.format(self.coverages[key],
                                                         key))

    def download(self, inventory_df, mirror=None, concurrent=2,
                 uname=None, pword=None):

        # if an old inventory exists dorp download_path
        if 'download_path' in inventory_df:
            inventory_df.drop('download_path', axis=1)
        
        # check if scenes exist
        inventory_df = search.check_availability(
            inventory_df, self.download_dir, self.data_mount)
        
        # extract only those scenes that need to be downloaded
        download_df = inventory_df[inventory_df.download_path.isnull()]

        # to download or not ot download - that is here the question
        if not download_df.any().any():
            print(' INFO: All scenes are ready for being processed.')    
        else:
            print(' INFO: One or more of your scenes need to be downloaded.')
            download.download_sentinel1(download_df,
                                        self.download_dir,
                                        mirror=mirror,
                                        concurrent=concurrent,
                                        uname=uname,
                                        pword=pword)

    def plot_inventory(self, inventory_df=None, transparency=0.05, annotate=False):

        if inventory_df is None:
            vec.plot_inventory(self.aoi, self.inventory, transparency, annotate)
        else:
            vec.plot_inventory(self.aoi, inventory_df, transparency, annotate)


class Sentinel1_SLCBatch(Sentinel1):
    ''' A Sentinel-1 specific subclass of the Generic OST class

    This subclass creates a Sentinel-1 specific
    '''

    def __init__(self, project_dir, aoi,
                 start='2014-10-01',
                 end=datetime.today().strftime("%Y-%m-%d"),
                 data_mount='/eodata',
                 download_dir=None,
                 inventory_dir=None,
                 processing_dir=None,
                 temp_dir=None,
                 product_type='SLC',
                 beam_mode='IW',
                 polarisation='*',
                 ard_type='OST Standard',
                 multiprocess=None
                 ):

        super().__init__(project_dir, aoi, start, end, data_mount,
                         download_dir, inventory_dir, processing_dir, temp_dir,
                         product_type, beam_mode, polarisation)

        self.ard_type = ard_type
        self.proc_file = opj(self.project_dir, 'processing.json')
        self.get_ard_parameters(self.ard_type)
        self.burst_inventory = None
        self.burst_inventory_file = None

    def create_burst_inventory(self, key=None, refine=True, 
                               uname=None, pword=None):

        if key:
            coverages = self.coverages[key]
            outfile = opj(self.inventory_dir,
                          'bursts.{}.shp').format(key)
            self.burst_inventory = burst.burst_inventory(
                self.refined_inventory_dict[key],
                outfile,
                download_dir=self.download_dir,
                data_mount=self.data_mount,
                uname=uname, pword=pword)
        else:
            coverages = None
            outfile = opj(self.inventory_dir,
                          'bursts.full.shp')
        
            self.burst_inventory = burst.burst_inventory(
                    self.inventory,
                    outfile,
                    download_dir=self.download_dir,
                    data_mount=self.data_mount,
                    uname=uname, pword=pword)

        if refine:
            #print('{}.refined.shp'.format(outfile[:-4]))
            self.burst_inventory = burst.refine_burst_inventory(
                    self.aoi, self.burst_inventory,
                    '{}.refined.shp'.format(outfile[:-4]),
                    coverages
                    )

    def read_burst_inventory(self, key):
        '''Read the Sentinel-1 data inventory from a OST inventory shapefile

        :param

        '''

        if key:
            file = opj(self.inventory_dir, 'burst_inventory.{}.shp').format(
                key)
        else:
            file = opj(self.inventory_dir, 'burst_inventory.shp')

        # define column names of file (since in shp they are truncated)
        # create column names for empty data frame
        column_names = ['SceneID', 'Track', 'Direction', 'Date', 'SwathID',
                        'AnxTime', 'BurstNr', 'geometry']

        geodataframe = gpd.read_file(file)
        geodataframe.columns = column_names
        geodataframe['Date'] = geodataframe['Date'].astype(int)
        geodataframe['BurstNr'] = geodataframe['BurstNr'].astype(int)
        geodataframe['AnxTime'] = geodataframe['AnxTime'].astype(int)
        geodataframe['Track'] = geodataframe['Track'].astype(int)
        self.burst_inventory = geodataframe

        return geodataframe

    def get_ard_parameters(self, ard_type=None):
        
        # we read the existent processing file
        if not ard_type:
            with open(self.proc_file, 'r') as ard_file:
                self.ard_parameters = json.load(ard_file)['processing parameters']
        # when ard_type is defined we read from template
        else:
            # get path to graph
            # get path to ost package
            rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
            rootpath = opj(rootpath, 'graphs', 'ard_json')
        
            template_file = opj(rootpath, '{}.{}.json'.format(
                    self.product_type.lower(),
                    ard_type.replace(' ', '_').lower()))
            
            with open(template_file, 'r') as ard_file:
                self.ard_parameters = json.load(ard_file)['processing parameters']
                
        with open (self.proc_file, 'w') as outfile:
            json.dump(dict({'processing parameters': self.ard_parameters}),
                      outfile,
                      indent=4)
           
    def update_ard_parameters(self):
        
        with open (self.proc_file, 'w') as outfile:
            json.dump(dict({'processing parameters': self.ard_parameters}),
                      outfile,
                      indent=4)

    
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

    def bursts_to_ard(self, timeseries=False, timescan=False, mosaic=False,
                     overwrite=False, exec_file=None, cut_to_aoi=False, ncores=os.cpu_count()):
        #check for previous exec files and remove them
        if exec_file:
            [os.remove(n) for n in glob.glob(exec_file+'*') if os.path.isfile(n)]

        # in case ard parameters have been updated, write them to json file
        self.update_ard_parameters()
        
        if overwrite:
            print(' INFO: Deleting processing folder to start from scratch')
            h.remove_folder_content(self.processing_dir)

        # set resolution in degree
        self.center_lat = loads(self.aoi).centroid.y
        if float(self.center_lat) > 59 or float(self.center_lat) < -59:
            print(' INFO: Scene is outside SRTM coverage. Will use 30m ASTER'
                  ' DEM instead.')
            self.ard_parameters['single ARD']['dem'] = 'ASTER 1sec GDEM'

        # set resolution to degree
        # self.ard_parameters['resolution'] = h.resolution_in_degree(
        #    self.center_lat, self.ard_parameters['resolution'])

        nr_of_processed = len(
            glob.glob(opj(self.processing_dir, '*', '*', '.processed')))

        # check and retry function
        if exec_file:
            [os.remove(n) for n in glob.glob(exec_file+'*') if os.path.isfile(n)]
            burst.burst_to_ard_batch(self.burst_inventory,
                                     self.download_dir,
                                     self.processing_dir,
                                     self.temp_dir,
                                     self.proc_file,
                                     self.data_mount,
                                     exec_file,
                                     ncores)

        else:
            i = 0
            while len(self.burst_inventory) > nr_of_processed:

                burst.burst_to_ard_batch(self.burst_inventory,
                                         self.download_dir,
                                         self.processing_dir,
                                         self.temp_dir,
                                         self.proc_file,
                                         self.data_mount,
                                         exec_file,
                                         ncores)

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
                                           self.proc_file,
                                           exec_file,
                                           ncores)

            # do we deleete the single ARDs here?
            if timescan:
                burst.timeseries_to_timescan(self.burst_inventory,
                                             self.processing_dir,
                                             self.temp_dir,
                                             self.proc_file,
                                             exec_file)


        if cut_to_aoi:
            cut_to_aoi = self.aoi
            
        if mosaic and timeseries:
            burst.mosaic_timeseries(self.burst_inventory,
                                    self.processing_dir,
                                    self.temp_dir,
                                    cut_to_aoi,
                                    exec_file,
                                    ncores
                                    )

        if mosaic and timescan:
            burst.mosaic_timescan(self.burst_inventory,
                                  self.processing_dir,
                                  self.temp_dir,
                                  self.proc_file,
                                  cut_to_aoi,
                                  exec_file,
                                  ncores
                                  )

    def create_timeseries_animation(self, timeseries_dir, product_list, outfile,
                                    shrink_factor=1, resampling_factor=5, duration=1,
                                    add_dates=False, prefix=False):
        
        ras.create_timeseries_animation(timeseries_dir, product_list, outfile, 
                                    shrink_factor=shrink_factor, duration=duration, resampling_factor=resampling_factor,
                                    add_dates=add_dates, prefix=prefix)
    def multiprocess(self, timeseries=False, timescan=False, mosaic=False,
                     overwrite=False, exec_file=None, cut_to_aoi=False, ncores=os.cpu_count(), multiproc=os.cpu_count()):
        '''
        Function to read previously generated exec text files and run them using
        a specified number of cores in parallel (or the number of available cpus)
        Some thought should be given to how many cores are available and the optimal number of cpus
        required to process a single burst
        Exec files should be recreated at each step to add parameters such as filenames, extents, that have been
        generated at previous steps
        '''
        #list exec files
        exec_burst_to_ard = exec_file + '_burst_to_ard.txt'
        exec_timeseries = exec_file + '_timeseries.txt'
        exec_tscan = exec_file + '_tscan.txt'
        exec_tscan_vrt = exec_file + '_tscan_vrt.txt'
        exec_mosaic_timeseries = exec_file + '_mosaic_timeseries.txt'
        exec_mosaic_ts_vrt = exec_file + '_mosaic_ts_vrt.txt'
        exec_mosaic_timescan = exec_file + '_mosaic_tscan.txt'
        exec_mosaic_tscan_vrt = exec_file + '_mosaic_tscan_vrt.txt'
        exec_mt_extent = exec_file + '_mt_extent.txt'
        exec_mt_ls = exec_file + '_mt_ls.txt'

        #test existence of burst to ard exec files and run them in parallel
        if os.path.isfile(exec_burst_to_ard):
            print("Running Burst to ARD in parallel mode")
            from ost.s1 import burst_to_ard
            burst_ard_params = []
            with open(exec_burst_to_ard, "r") as fp:
                burst_ard_params = [line.strip() for line in fp]
            fp.close()

            ##replaced multiprocessing pools with joblib (only prints when run in ipython or command line though)
            #def run_burst_ard_multiprocess(params):
            #    from ost.s1 import burst_to_ard
            #    burst_to_ard.burst_to_ard(*params.split(','))
            nr_of_processed = len(
                glob.glob(opj(self.processing_dir, '*', '*', '.processed')))

            i = 0
            if self.ard_parameters['single ARD']['product type'] == 'Coherence_only':
                i = 0

                while (len(self.burst_inventory) - len(unique(s1_batch.burst_inventory['bid']))) > nr_of_processed:
                    Parallel(n_jobs=multiproc, verbose=53, backend=multiprocessing)(
                        delayed(burst_to_ard.burst_to_ard)(*params.split(';')) for params in burst_ard_params)

                    nr_of_processed = len(
                        glob.glob(opj(self.processing_dir, '*', '*', '.processed')))

                    i += 1

                    # not more than 5 trys
                    if i == 5:
                        break
            else:
                i = 0

                while len(self.burst_inventory) > nr_of_processed:

                    Parallel(n_jobs=multiproc, verbose=53, backend=multiprocessing)(delayed(burst_to_ard.burst_to_ard)(*params.split(';')) for params in burst_ard_params)

                    nr_of_processed = len(
                        glob.glob(opj(self.processing_dir, '*', '*', '.processed')))

                    i += 1

                    # not more than 5 trys
                    if i == 5:
                        break

            #pool = multiprocessing.Pool(processes=multiproc)
            #pool.map(run_burst_ard_multiprocess, burst_ard_params)

        # test existence of multitemporal extent exec files and run them in parallel
        if timeseries:
            print("Rerunning exec file generation and Calculating ARD extents in parallel mode")

            _stdout = sys.stdout
            sys.stdout = DevNull()

            self.bursts_to_ard(timeseries=timeseries, timescan=timescan, mosaic=mosaic,
                     overwrite=overwrite, exec_file=exec_file, cut_to_aoi=cut_to_aoi, ncores=ncores)
            sys.stdout = _stdout
            if os.path.isfile(exec_mt_extent):
                mt_extent_params = []
                with open(exec_mt_extent, "r") as fp:
                    mt_extent_params = [line.strip() for line in fp]
                fp.close()

                ##replaced multiprocessing pools with joblib (only prints when run in ipython or command line though)
                #def run_mt_extent_multiprocess(params):
                #    from ost.multitemporal import common_extent
                #    common_extent.mt_extent(*params.split(','))
                Parallel(n_jobs=multiproc, verbose=53, backend=multiprocessing)(delayed(common_extent.mt_extent)(*params.split(';')) for params in mt_extent_params)

                #pool = multiprocessing.Pool(processes=multiproc)
                #pool.map(run_mt_extent_multiprocess, mt_extent_params)

        # test existence of multitemporal layover shadow generation exec files and run them in parallel
        if os.path.isfile(exec_mt_ls):
            print("Rerunning exec file generation and Calculating ARD layover in parallel mode")

            _stdout = sys.stdout
            sys.stdout = DevNull()

            self.bursts_to_ard(timeseries=timeseries, timescan=timescan, mosaic=mosaic,
                               overwrite=overwrite, exec_file=exec_file, cut_to_aoi=cut_to_aoi, ncores=ncores)
            sys.stdout = _stdout

            if os.path.isfile(exec_mt_ls):

                mt_ls_params = []
                with open(exec_mt_ls, "r") as fp:
                    mt_ls_params = [line.strip() for line in fp]
                fp.close()

                ##replaced multiprocessing pools with joblib (only prints when run in ipython or command line though)
                #def run_mt_ls_multiprocess(params):
                #    from ost.multitemporal import common_ls_mask
                #    common_ls_mask.mt_layover(*params.split(','))
                Parallel(n_jobs=multiproc, verbose=53, backend=multiprocessing)(delayed(common_ls_mask.mt_layover)(*params.split(';')) for params in mt_ls_params)

                #pool = multiprocessing.Pool(processes=multiproc)
                #pool.map(run_mt_ls_multiprocess, mt_ls_params)

        #test existence of ard to timeseries exec files and run them in parallel
        if timeseries:
            print("Rerunning exec file generation and processing ARD to timeseries in parallel mode")

            _stdout = sys.stdout
            sys.stdout = DevNull()

            self.bursts_to_ard(timeseries=timeseries, timescan=timescan, mosaic=mosaic,
                               overwrite=overwrite, exec_file=exec_file, cut_to_aoi=cut_to_aoi, ncores=ncores)
            sys.stdout = _stdout

            timeseries_params = []
            if os.path.isfile(exec_timeseries):
                with open(exec_timeseries, "r") as fp:
                    timeseries_params = [line.strip() for line in fp]
                fp.close()
                ##replaced multiprocessing pools with joblib (only prints when run in ipython or command line though)
                #def run_timeseries_multiprocess(params):
                #   from ost.multitemporal import ard_to_ts
                #   ard_to_ts.ard_to_ts(*params.split(','))
                Parallel(n_jobs=multiproc, verbose=53, backend=multiprocessing)(delayed(ard_to_ts.ard_to_ts)(*params.split(';')) for params in timeseries_params)

                #pool = multiprocessing.Pool(processes=multiproc)
                #pool.map(run_timeseries_multiprocess, timeseries_params)

        #test existence of timescan exec files and run them in parallel
        if timeseries and timescan:
            print("Rerunning exec file generation and processing timeseries to timescan in parallel mode")

            _stdout = sys.stdout
            sys.stdout = DevNull()

            self.bursts_to_ard(timeseries=timeseries, timescan=timescan, mosaic=mosaic,
                               overwrite=overwrite, exec_file=exec_file, cut_to_aoi=cut_to_aoi, ncores=ncores)
            sys.stdout = _stdout

            if os.path.isfile(exec_tscan):

                tscan_params = []
                with open(exec_tscan, "r") as fp:
                    tscan_params = [line.strip() for line in fp]
                fp.close()

                ##replaced multiprocessing pools with joblib (only prints when run in ipython or command line though)
                Parallel(n_jobs=multiproc, verbose=53, backend=multiprocessing)(delayed(tscan.mt_metrics)(*params.split(';')) for params in tscan_params)

               # def run_tscan_multiprocess(params):
               #     from ost.multitemporal import timescan
               #     timescan.mt_metrics(*params.split(','))
               # pool = multiprocessing.Pool(processes=multiproc)
               # pool.map(run_tscan_multiprocess, tscan_params)

        #test existence of timescan vrt exec files and run them in parallel
        if timeseries and timescan:
            print("Rerunning exec file generation and generating timescan vrt files in parallel mode")

            _stdout = sys.stdout
            sys.stdout = DevNull()

            self.bursts_to_ard(timeseries=timeseries, timescan=timescan, mosaic=mosaic,
                               overwrite=overwrite, exec_file=exec_file, cut_to_aoi=cut_to_aoi, ncores=ncores)
            sys.stdout = _stdout

            if os.path.isfile(exec_tscan_vrt):

                tscan_vrt_params = []
                with open(exec_tscan_vrt, "r") as fp:
                    tscan_vrt_params = [line.strip() for line in fp]
                fp.close()
                ##replaced multiprocessing pools with joblib (only prints when run in ipython or command line though)
                Parallel(n_jobs=multiproc, verbose=53, backend=multiprocessing)(delayed(ras.create_tscan_vrt)(*params.split(';')) for params in tscan_vrt_params)
                #def run_tscan_vrt_multiprocess(params):
                #    from ost.helpers import raster as ras
                #    ras.create_tscan_vrt(*params.split(','))
                #pool = multiprocessing.Pool(processes=multiproc)
                #pool.map(run_tscan_vrt_multiprocess, tscan_vrt_params)

        # test existence of mosaic timeseries exec files and run them in parallel
        if mosaic and timeseries:
            print("Rerunning exec file generation and generating timeseries mosaics in parallel mode")

            _stdout = sys.stdout
            sys.stdout = DevNull()

            self.bursts_to_ard(timeseries=timeseries, timescan=timescan, mosaic=mosaic,
                               overwrite=overwrite, exec_file=exec_file, cut_to_aoi=cut_to_aoi, ncores=ncores)
            sys.stdout = _stdout

            if os.path.isfile(exec_mosaic_timeseries):
                mosaic_timeseries_params = []
                with open(exec_mosaic_timeseries, "r") as fp:
                    mosaic_timeseries_params = [line.strip() for line in fp]
                fp.close()
                ##replaced multiprocessing pools with joblib (only prints when run in ipython or command line though)
                Parallel(n_jobs=1, verbose=53, backend=multiprocessing)(delayed(mos.mosaic)(*params.split(';')) for params in mosaic_timeseries_params)
                #def run_mosaic_timeseries_multiprocess(params):
                #    from ost.mosaic import mosaic
                #    mos.mosaic(*params.split(','))

                #pool = multiprocessing.Pool(processes=multiproc)
                #pool.map(run_mosaic_timeseries_multiprocess, mosaic_timeseries_params)

        # test existence of mosaic timeseries vrt exec files and run them in parallel
        if mosaic and timeseries:
            print("Rerunning exec file generation and generating timeseries mosaic vrt files in parallel mode")

            _stdout = sys.stdout
            sys.stdout = DevNull()

            self.bursts_to_ard(timeseries=timeseries, timescan=timescan, mosaic=mosaic,
                               overwrite=overwrite, exec_file=exec_file, cut_to_aoi=cut_to_aoi, ncores=ncores)
            sys.stdout = _stdout

            if os.path.isfile(exec_mosaic_ts_vrt):

                mosaic_ts_vrt_params = []
                with open(exec_mosaic_ts_vrt, "r") as fp:
                    mosaic_ts_vrt_params = [line.strip() for line in fp]
                fp.close()
                ##replaced multiprocessing pools with joblib (only prints when run in ipython or command line though)
                Parallel(n_jobs=multiproc, verbose=53, backend=multiprocessing)(delayed(mos.mosaic_to_vrt)(*params.split(';')) for params in mosaic_ts_vrt_params)

                #def run_mosaic_ts_vrt_multiprocess(params):
                #    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
                #    ts_dir, product, outfiles = params.split(',')
                #    gdal.BuildVRT(opj(ts_dir, '{}.Timeseries.vrt'.format(product)),
                #                  outfiles,
                #                  options=vrt_options)

                #pool = multiprocessing.Pool(processes=multiproc)
                #pool.map(run_mosaic_ts_vrt_multiprocess, mosaic_ts_vrt_params)

        # test existence of mosaic timescan exec files and run them in parallel
        if mosaic and timescan:
            print("Rerunning exec file generation and generating timescan mosaics in parallel mode")

            _stdout = sys.stdout
            sys.stdout = DevNull()

            self.bursts_to_ard(timeseries=timeseries, timescan=timescan, mosaic=mosaic,
                               overwrite=overwrite, exec_file=exec_file, cut_to_aoi=cut_to_aoi, ncores=ncores)
            sys.stdout = _stdout

            if os.path.isfile(exec_mosaic_timescan):

                mosaic_timescan_params = []
                with open(exec_mosaic_timescan, "r") as fp:
                    mosaic_timescan_params = [line.strip() for line in fp]
                fp.close()

                ##replaced multiprocessing pools with joblib (only prints when run in ipython or command line though)
                Parallel(n_jobs=1, verbose=53, backend=multiprocessing)(delayed(mos.mosaic)(*params.split(';')) for params in mosaic_timescan_params)
                #def run_mosaic_timescan_multiprocess(params):
                #    from ost.mosaic import mosaic
                #    mos.mosaic(*params.split(','))

                #pool = multiprocessing.Pool(processes=multiproc)
                #pool.map(run_mosaic_timescan_multiprocess, mosaic_timescan_params)

        # test existence of mosaic timescan vrt exec files and run them in parallel
        if mosaic and timescan:
            print("Rerunning exec file generation and generating timeseries mosaic vrt files in parallel mode")

            _stdout = sys.stdout
            sys.stdout = DevNull()

            self.bursts_to_ard(timeseries=timeseries, timescan=timescan, mosaic=mosaic,
                               overwrite=overwrite, exec_file=exec_file, cut_to_aoi=cut_to_aoi, ncores=ncores)
            sys.stdout = _stdout

            if os.path.isfile(exec_mosaic_tscan_vrt):

                mosaic_tscan_vrt_params = []
                with open(exec_mosaic_tscan_vrt, "r") as fp:
                    mosaic_tscan_vrt_params = [line.strip() for line in fp]
                fp.close()

                ##replaced multiprocessing pools with joblib (only prints when run in ipython or command line though)
                Parallel(n_jobs=multiproc, verbose=53, backend=multiprocessing)(delayed(ras.create_tscan_vrt)(*params.split(';')) for params in mosaic_tscan_vrt_params)
                #def run_mosaic_tscan_vrt_multiprocess(params):
                #    from ost.helpers import raster as ras
                #    ras.create_tscan_vrt(*params.split(','))

                #pool = multiprocessing.Pool(processes=multiproc)
                #pool.map(run_mosaic_tscan_vrt_multiprocess, mosaic_tscan_vrt_params)



class Sentinel1_GRDBatch(Sentinel1):
    ''' A Sentinel-1 specific subclass of the Generic OST class

    This subclass creates a Sentinel-1 specific
    '''

    
    
    def __init__(self, project_dir, aoi,
                 start='2014-10-01',
                 end=datetime.today().strftime("%Y-%m-%d"),
                 data_mount=None,
                 download_dir=None,
                 inventory_dir=None,
                 processing_dir=None,
                 temp_dir=None,
                 product_type='GRD',
                 beam_mode='IW',
                 polarisation='*',
                 ard_type='OST Standard'
                 ):

        super().__init__(project_dir, aoi, start, end, data_mount,
                         download_dir, inventory_dir, processing_dir, temp_dir,
                         product_type, beam_mode, polarisation)

        self.ard_type = ard_type
        self.proc_file = opj(self.project_dir, 'processing.json')
        self.get_ard_parameters(self.ard_type)
        
    # processing related functions
    def get_ard_parameters(self, ard_type='OST Standard'):
        
        # get path to graph
        # get path to ost package
        rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
        rootpath = opj(rootpath, 'graphs', 'ard_json')

        template_file = opj(rootpath, '{}.{}.json'.format(
                self.product_type.lower(),
                ard_type.replace(' ', '_').lower()))
        
        with open(template_file, 'r') as ard_file:
            self.ard_parameters = json.load(ard_file)['processing parameters']
       
    def update_ard_parameters(self):
        
        with open (self.proc_file, 'w') as outfile:
            json.dump(dict({'processing parameters': self.ard_parameters}),
                      outfile,
                      indent=4)
    
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
        
    def grds_to_ard(self, inventory_df=None, subset=None, timeseries=False, 
                   timescan=False, mosaic=False, overwrite=False, 
                   exec_file=None, cut_to_aoi=False):

        self.update_ard_parameters()
        
        if overwrite:
            print(' INFO: Deleting processing folder to start from scratch')
            h.remove_folder_content(self.processing_dir)

        # set resolution in degree
#        self.center_lat = loads(self.aoi).centroid.y
#        if float(self.center_lat) > 59 or float(self.center_lat) < -59:
#            print(' INFO: Scene is outside SRTM coverage. Will use 30m ASTER'
#                  ' DEM instead.')
#            self.ard_parameters['dem'] = 'ASTER 1sec GDEM'

        if subset:
            if subset.split('.')[-1] == '.shp':
                subset = str(vec.shp_to_wkt(subset, buffer=0.1, envelope=True))
            elif subset.startswith('POLYGON (('):
                subset = loads(subset).buffer(0.1).to_wkt()
            else:
                print(' ERROR: No valid subset given.'
                      ' Should be either path to a shapefile or a WKT Polygon.')
                sys.exit()

        # check number of already prcessed acquisitions
        nr_of_processed = len(
            glob.glob(opj(self.processing_dir, '*', '20*', '.processed'))
        )

        # number of acquisitions to process
        nr_of_acq = len(
            inventory_df.groupby(['relativeorbit', 'acquisitiondate'])
        )

        # check and retry function
        i = 0
        while nr_of_acq > nr_of_processed:

            # the grd to ard batch routine
            grd_batch.grd_to_ard_batch(
                                  inventory_df,
                                  self.download_dir,
                                  self.processing_dir,
                                  self.temp_dir,
                                  self.proc_file,
                                  subset,
                                  self.data_mount,
                                  exec_file)
            
            # reset number of already processed acquisitions
            nr_of_processed = len(
                glob.glob(opj(self.processing_dir, '*', '20*', '.processed')))
            i += 1
            
            # not more than 5 trys
            if i == 5:
                break
        
        # time-series part
        if timeseries or timescan:
            
            nr_of_processed = len(
                    glob.glob(opj(self.processing_dir, '*', 
                                  'Timeseries', '.*processed')))
            
            nr_of_polar = len(
                inventory_df.polarisationmode.unique()[0].split(' '))
            nr_of_tracks = len(inventory_df.relativeorbit.unique())
            nr_of_ts = nr_of_polar * nr_of_tracks
            
            
            # check and retry function
            i = 0
            while nr_of_ts > nr_of_processed:
                
                grd_batch.ards_to_timeseries(inventory_df,
                                             self.processing_dir,
                                             self.temp_dir,
                                             self.proc_file,
                                             exec_file)
                
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
                inventory_df.polarisationmode.unique()[0].split(' '))
            nr_of_tracks = len(inventory_df.relativeorbit.unique())
            nr_of_ts = nr_of_polar * nr_of_tracks
            
            i = 0
            while nr_of_ts > nr_of_processed:
                 
                 grd_batch.timeseries_to_timescan(
                         inventory_df,
                         self.processing_dir,
                         self.proc_file)
                 
                 nr_of_processed = len(glob.glob(opj(
                    self.processing_dir, '*', 'Timescan', '.*processed')))
                 
                 i += 1
        
                # not more than 5 trys
                 if i == 5:
                     break
                
            if i < 5 and exec_file:
                print(' create vrt command')                
                
        if cut_to_aoi:
            cut_to_aoi = self.aoi
            
        if mosaic and timeseries and not subset:
            grd_batch.mosaic_timeseries(
                    inventory_df,
                    self.processing_dir,
                    self.temp_dir, 
                    cut_to_aoi
            )


        if mosaic and timescan and not subset:
            grd_batch.mosaic_timescan(inventory_df,
                                      self.processing_dir,
                                      self.temp_dir,
                                      self.proc_file,
                                      cut_to_aoi
                                      )
