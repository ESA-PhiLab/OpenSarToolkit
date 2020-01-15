'''
This script allows to produce Sentinel-1 backscatter ARD data
from a set of different GRD products.
The script allows to process consecutive frames from one acquisition and
outputs a single file.


----------------
Functions:
----------------
    grd_to_ardBatch:
        processes all acquisitions
    ard2Ts:
        processes all time-series

------------------
Main function
------------------
  grd2Ts:
    handles the whole workflow

------------------
Contributors
------------------

Andreas Vollrath, ESA phi-lab
-----------------------------------
November 2018: Original implementation

------------------
Usage
------------------

python3 grd_to_ardBatch.py -i /path/to/inventory -r 20 -p RTC -l True -s False
                   -t /path/to/tmp -o /path/to/output

    -i    defines the path to one or a list of consecutive slices
    -r    resolution in meters (should be 10 or more, default=20)
    -p    defines the product type (GTCsigma, GTCgamma, RTC, default=GTCgamma)
    -l    defines the layover/shadow mask creation (True/False, default=True)
    -s    defines the speckle filter (True/False, default=False)
    -t    defines the folder for temporary products (default=/tmp)
    -o    defines the /path/to/the/output
'''

import os
from os.path import join as opj
import glob
import datetime
import gdal
import logging
import itertools
import shutil

from ost import Sentinel1Scene
from ost.s1 import grd_to_ard, ts
from ost.helpers import raster as ras, vector as vec
from ost.helpers import helpers as h

logger = logging.getLogger(__name__)


def _create_processing_dict(inventory_df):
    '''This function might be obsolete?

    '''

    # initialize empty dictionary
    dict_scenes = {}

    # get relative orbits and loop through each
    tracklist = inventory_df['relativeorbit'].unique()
    for track in tracklist:

        # initialize an empty list that will be filled by
        # list of scenes per acq. date
        all_ids = []

        # get acquisition dates and loop through each
        acquisition_dates = inventory_df['acquisitiondate'][
            inventory_df['relativeorbit'] == track].unique()

        # loop through dates
        for acquisition_date in acquisition_dates:

            # get the scene ids per acquisition_date and write into a list
            single_id = []
            single_id.append(inventory_df['identifier'][
                (inventory_df['relativeorbit'] == track) &
                (inventory_df['acquisitiondate'] == acquisition_date)].tolist())

            # append the list of scenes to the list of scenes per track
            all_ids.append(single_id[0])

        # add this list to the dctionary and associate the track number
        # as dict key
        dict_scenes[track] = all_ids

    return dict_scenes


def grd_to_ard_batch(inventory_df, download_dir, processing_dir,
                     temp_dir, ard_parameters, subset=None, 
                     data_mount='/eodata'):
     
    # get params
    resolution = ard_parameters['resolution']
    product_type = ard_parameters['product_type']
    ls_mask_create = ard_parameters['ls_mask_create']
    speckle_filter = ard_parameters['speckle_filter']
    polarisation = ard_parameters['polarisation']
    dem = ard_parameters['dem']
    to_db = ard_parameters['to_db']
    border_noise = ard_parameters['border_noise']

    # we create a processing dictionary,
    # where all frames are grouped into acquisitions
    processing_dict = _create_processing_dict(inventory_df)

    for track, allScenes in processing_dict.items():
        for list_of_scenes in processing_dict[track]:
            # get acquisition date
            acquisition_date = Sentinel1Scene(list_of_scenes[0]).start_date
            # create a subdirectory baed on acq. date
            out_dir = opj(processing_dir, track, acquisition_date)
            os.makedirs(out_dir, exist_ok=True)

            # check if already processed
            if os.path.isfile(opj(out_dir, '.processed')):
                logger.debug('INFO: Acquisition from {} of track {}'
                             'already processed'.format(acquisition_date, track)
                             )
            else:
                # get the paths to the file
                scene_paths = ([Sentinel1Scene(i).get_path(download_dir)
                               for i in list_of_scenes])

                # apply the grd_to_ard function
                grd_to_ard.grd_to_ard(
                    scene_paths,
                    out_dir,
                    acquisition_date,
                    temp_dir,
                    resolution=resolution,
                    product_type=product_type,
                    ls_mask_create=ls_mask_create,
                    speckle_filter=speckle_filter,
                    dem=dem,
                    to_db=to_db,
                    border_noise=border_noise,
                    subset=subset,
                    polarisation=polarisation
                )


def ards_to_timeseries(inventory_df, processing_dir, temp_dir, ard_parameters):
    # get params
    to_db = ard_parameters['to_db']
    if to_db:
        to_db_mt = False
    else:
        to_db_mt = ard_parameters['to_db_mt']
    
    datatype = ard_parameters['datatype']
    ls_mask_create = ard_parameters['ls_mask_create']
    ls_mask_apply = ard_parameters['ls_mask_apply']
    mt_speckle_filter = ard_parameters['mt_speckle_filter']

    # 1) we convert input to a geopandas GeoDataFrame object
    processing_dict = _create_processing_dict(inventory_df)
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)

    for track, allScenes in processing_dict.items():
  
        logger.debug('INFO: Entering track {}.'.format(track))
        track_dir = opj(processing_dir, track)
        all_outfiles = []
        
        if os.path.isfile(opj(track_dir, 'Timeseries', '.processed')):
            logger.debug('INFO: Timeseries for track {} already processed.'.format(track))
        else:
            logger.debug('INFO: Processing Timeseries for track {}.'.format(track))
            # 1) get minimum valid extent
            # (i.e. assure value fo each pixel throughout the whole time-series)
            logger.debug('INFO: Calculating the minimum extent.')
            list_of_scenes = glob.glob(opj(track_dir, '20*', '*data*', '*img'))
            list_of_scenes = [x for x in list_of_scenes if 'layover'not in x]
            extent = opj(track_dir, '{}.extent.shp'.format(track))
            ts.mt_extent(list_of_scenes, extent, temp_dir, buffer=-0.0018)
        
            # create a list of dimap files and format to comma-separated list
            list_of_ards = sorted(glob.glob(opj(track_dir, '20*', '*TC*dim')))
            list_of_ards = '\'{}\''.format(','.join(list_of_ards))
            
            if ls_mask_create:
                list_of_scenes = glob.glob(opj(track_dir, '20*', '*data*', '*img'))
                list_of_layover = [x for x in list_of_scenes if 'layover'in x]
                out_ls = opj(track_dir, '{}.ls_mask.tif'.format(track))
                ts.mt_layover(list_of_layover, out_ls, temp_dir, extent=extent)
                logger.debug('INFO: Our common layover mask is located at {}'.format(
                        out_ls))
    
            if ls_mask_apply:
                logger.debug('INFO: Calculating symetrical difference of extent and ls_mask')
                ras.polygonize_raster(out_ls, '{}.shp'.format(out_ls[:-4]))
                extent_ls_masked = opj(track_dir, '{}.extent.masked.shp'.format(track))
                vec.difference(extent, '{}.shp'.format(out_ls[:-4]), extent_ls_masked)
                extent = extent_ls_masked
    
            for p in ['VV', 'VH', 'HH', 'HV']:
    
                # check if polarisation is existent
                list_of_pols = sorted(glob.glob(opj(track_dir, '20*', '*TC*data', '*{}*.img'.format(p))))
    
                if len(list_of_pols) >= 2:
    
                    # create output stack name for RTC
                    temp_stack = opj(temp_dir, 'stack_{}_{}'.format(track, p))
                    out_stack = opj(temp_dir, 'mt_stack_{}_{}'.format(track, p))
    
                    os.makedirs(opj(track_dir, 'Timeseries'), exist_ok=True)
                    logfile = opj(track_dir, 'Timeseries', '{}.stack.errLog'.format(p))
    
                    # create the stack of same polarised data if polarisation is existent
                    return_code = ts.create_stack(list_of_ards, temp_stack, logfile, p)
                    if return_code != 0:
                        h.remove_folder_content(temp_dir)
                        return return_code
                    
                    if mt_speckle_filter is True:
                        # do the multi-temporal filtering
                        logfile = opj(track_dir, 'Timeseries', 
                                      '{}.mt_speckle_filter.errLog'.format(p))
                        return_code = ts.mt_speckle_filter('{}.dim'.format(
                            temp_stack), out_stack, logfile
                        )
                        if return_code != 0:
                            h.remove_folder_content(temp_dir)
                            return return_code
                        h.delete_dimap(temp_stack)
                        
                    else:
                        out_stack = temp_stack
    
                    # get the dates of the files
                    dates = [datetime.datetime.strptime(x.split('_')[-1][:-4], '%d%b%Y')
                             for x in glob.glob(opj('{}.data'.format(out_stack), '*img'))
                             ]
                    # sort them
                    dates.sort()
                    # write them back to string for following loop
                    sortedDates = [
                        datetime.datetime.strftime(ts, "%d%b%Y") for ts in dates
                    ]
                    i, outfiles = 1, []
                    for date in sortedDates:
    
                        # restructure date to YYMMDD
                        indate = datetime.datetime.strptime(date, '%d%b%Y')
                        outdate = datetime.datetime.strftime(indate, '%y%m%d')
    
                        infile = glob.glob(opj('{}.data'.format(out_stack), '*{}*{}*img'.format(p, date)))[0]
                        # create outFile
                        outfile = opj(track_dir, 'Timeseries', '{}.{}.BS.{}.tif'.format(i, outdate, p))
                        # mask by extent
                        ras.mask_by_shape(
                            infile, outfile,
                            extent,
                            to_db=to_db_mt,
                            datatype=datatype,
                            min_value=-30, max_value=5,
                            ndv=0
                        )
                        # add ot a list for subsequent vrt creation
                        outfiles.append(outfile)
                        all_outfiles.append(outfile)
    
                        i += 1
    
                    # build vrt of timeseries
                    gdal.BuildVRT(opj(track_dir, 'Timeseries', 'BS.Timeseries.{}.vrt'.format(p)), outfiles, options=vrt_options)
                    #if os.path.isdir('{}.data'.format(out_stack)):
                    h.delete_dimap(out_stack)
        
            for file in all_outfiles:
                return_code = h.check_out_tiff(file)
                if return_code != 0:
                    h.remove_folder_content(temp_dir)
                    h.remove_folder_content(opj(track_dir, 'Timeseries'))
                    return return_code
            
            # write file, so we know this ts has been succesfully processed
            if return_code == 0:
                check_file = opj(track_dir, 'Timeseries', '.processed')
                with open(str(check_file), 'w') as file:
                    file.write('passed all tests \n')
    

def timeseries_to_timescan(inventory_df, processing_dir, ard_parameters):

    metrics = ard_parameters['metrics']
    outlier_removal = ard_parameters['outlier_removal']
    
    if ard_parameters['to_db_mt'] or ard_parameters['to_db']:
        to_db = True
    else:
        to_db = False
        
    # read inventory_df to processing dictionary
    processing_dict = _create_processing_dict(inventory_df)

    # loop through tracks
    for track, allScenes in processing_dict.items():

        logger.debug('INFO: Entering track {}.'.format(track))
        
        # get track directory
        track_dir = opj(processing_dir, track)
        
        if os.path.isfile(opj(track_dir, 'Timescan', '.processed')):
            logger.debug('INFO: Timescans for track {} already processed.'.format(track))
        else:
            logger.debug('INFO: Processing Timescans for track {}.'.format(track))
            # define and create Timescan directory
            timescan_dir = opj(track_dir, 'Timescan')
            os.makedirs(timescan_dir, exist_ok=True)
    
            # loop thorugh each polarization
            for p in ['VV', 'VH', 'HH', 'HV']:
    
                # Get timeseries vrt
                timeseries = opj(track_dir, 'Timeseries', 'BS.Timeseries.{}.vrt'.format( p))
    
                # define timescan prefix
                timescan_prefix = opj(timescan_dir, 'BS.{}'.format(p))
    
                # check if timeseries vrt exists
                if os.path.exists(timeseries):
    
                    # calculate the multi-temporal metrics
                    ts.mt_metrics(timeseries, timescan_prefix, metrics,
                                  rescale_to_datatype=False,
                                  to_power=to_db,
                                  outlier_removal=outlier_removal)
                
            product_list = ['BS.HH', 'BS.VV', 'BS.HV', 'BS.VH']
            i, list_of_files = 0, []
            for product in itertools.product(product_list, metrics):
        
                file = glob.glob(
                    opj(track_dir, 'Timescan', '*{}.{}.tif'.format(
                        product[0], product[1])))
        
                if file:
                    i += 1
                    outfile = opj(track_dir, 'Timescan', '{}.{}.{}.tif'.format(
                        i, product[0], product[1]))
                    shutil.move(file[0], outfile)
                    list_of_files.append(outfile)
                    
            for file in list_of_files:
                return_code = h.check_out_tiff(file)
                if return_code != 0:
                    h.remove_folder_content(opj(track_dir, 'Timescan'))
                    return return_code
            
            # write file, so we know this ts has been succesfully processed
            if return_code == 0:
                check_file = opj(track_dir, 'Timescan', '.processed')
                with open(str(check_file), 'w') as file:
                    file.write('passed all tests \n')
            
            # create vrt
            vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
            gdal.BuildVRT(opj(track_dir, 'Timescan', 'Timescan.vrt'),
                          list_of_files,
                          options=vrt_options)


def mosaic_timeseries(inventory_df, processing_dir, temp_dir):

    logger.debug('INFO: Mosaicking Time-series layers')
    
    for p in ['VV', 'VH', 'HH', 'HV']:
        processing_dict = _create_processing_dict(inventory_df)
        keys = [x for x in processing_dict.keys()]
        outfiles = []

        os.makedirs(opj(processing_dir, 'Mosaic', 'Timeseries'), exist_ok=True)
        
        nrOfTs = len(glob.glob(opj(
            processing_dir, keys[0], 'Timeseries', '*.{}.tif'.format(p))))
        
        if nrOfTs >= 1:
            for i in range(nrOfTs):
                j = i + 1
                filelist = glob.glob(opj(
                    processing_dir, '*', 'Timeseries', 
                    '{}.*.{}.tif'.format(j, p)))
                filelist = [file for file in filelist if 'Mosaic'not in file]
                # logger.debug(filelist)
                datelist = []
                for file in filelist:
                    datelist.append(os.path.basename(file).split('.')[1])
                    
                filelist = ''.join(filelist)
                start = sorted(datelist)[0]
                end = sorted(datelist)[-1]
                    
                if start == end:
                    outfile = opj(
                        processing_dir,
                        'Mosaic',
                        'Timeseries',
                        '{}.BS.{}.{}.tif'.format(j, start, p)
                    )
                    check_file = opj(processing_dir,
                                     'Mosaic',
                                     'Timeseries',
                                     '.{}.BS.{}.{}.processed'.format(j, start, p)
                                     )
                    logfile = opj(processing_dir,
                                  'Mosaic',
                                  'Timeseries',
                                  '{}.BS.{}.{}.errLog'.format(j, start, p)
                                  )
                else: 
                    outfile = opj(processing_dir,
                                  'Mosaic',
                                  'Timeseries',
                                  '{}.BS.{}-{}.{}.tif'.format(j, start, end, p)
                                  )
                    check_file = opj(processing_dir,
                                     'Mosaic',
                                     'Timeseries',
                                     '.{}.BS.{}-{}.{}.processed'.format(j, start, end, p)
                                     )
                    logfile = opj(processing_dir,
                                  'Mosaic',
                                  'Timeseries',
                                  '{}.BS.{}-{}.{}.errLog'.format(j, start, end, p)
                                  )
                
                outfiles.append(outfile)
                
                if os.path.isfile(check_file):
                    logger.debug(
                        'INFO: Mosaic layer {} already processed.'.format(
                            os.path.basename(outfile)
                        )
                    )
                else:
                    logger.debug(
                        'INFO: Mosaicking layer {}.'.format(
                            os.path.basename(outfile)
                        )
                    )
                    cmd = ('otbcli_Mosaic -ram 4096 -progress 1 \
                            -comp.feather large -harmo.method band \
                            -harmo.cost rmse -temp_dir {} -il {} \
                            -out {}'.format(temp_dir, filelist, outfile))
    
                    return_code = h.run_command(cmd, logfile)
                    if return_code != 0:
                        if os.path.isfile(outfile):
                            os.remove(outfile)
                    
                    return_code = h.check_out_tiff(outfile)
                    if return_code != 0:
                        if os.path.isfile(outfile):
                            os.remove(outfile)
                        
                    # write file, so we know this ts has been succesfully processed
                    if return_code == 0:
                        with open(str(check_file), 'w') as file:
                            file.write('passed all tests \n')  
                     
            # create vrt
        vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
        gdal.BuildVRT(opj(processing_dir, 'Mosaic', 'Timeseries', 'Timeseries.{}.vrt'.format(p)),
                      outfiles,
                      options=vrt_options) 
        

def mosaic_timescan(inventory_df, processing_dir, temp_dir, ard_parameters):
    
    logger.debug('INFO: Mosaicking Timescan layers')
    metrics = ard_parameters['metrics']
    outfiles = []
    for p in ['VV', 'VH', 'HH', 'HV']: 
        
        os.makedirs(opj(processing_dir, 'Mosaic', 'Timescan'), exist_ok=True)
        
        for metric in metrics:

            filelist = glob.glob(
                opj(processing_dir, '*', 'Timescan', 
                    '*BS.{}.{}.tif'.format(p, metric)))
            
            if len(filelist) >= 2:
                # get number
                i = os.path.basename(filelist[0]).split('.')[0]
                filelist = ''.join(filelist)
                
                outfile = opj(processing_dir, 'Mosaic', 'Timescan', '{}.BS.{}.{}.tif'.format(i, p, metric))
                check_file = opj(processing_dir, 'Mosaic', 'Timescan', '.{}.BS.{}.{}.processed'.format(i, p, metric))
                logfile = opj(processing_dir, 'Mosaic', 'Timescan', '{}.BS.{}.{}.errLog'.format(i, p, metric))
                 
                outfiles.append(outfile)
                
                if os.path.isfile(check_file):
                    logger.debug('INFO: Mosaic layer {} already processed.'.format(os.path.basename(outfile)))
                else:
                    logger.debug('INFO: Mosaicking layer {}.'.format(os.path.basename(outfile)))
                    cmd = ('otbcli_Mosaic -ram 4096 -progress 1 \
                                -comp.feather large -harmo.method band \
                                -harmo.cost rmse -temp_dir {} -il {} \
                                -out {}'.format(temp_dir, filelist, outfile))
        
                    return_code = h.run_command(cmd, logfile)
                    if return_code != 0:
                        if os.path.isfile(outfile):
                            os.remove(outfile)

                    return_code = h.check_out_tiff(outfile)
                    if return_code != 0:
                        if os.path.isfile(outfile):
                            os.remove(outfile)
                        
                    # write file, so we know this ts has been succesfully processed
                    if return_code == 0:
                        with open(str(check_file), 'w') as file:
                            file.write('passed all tests \n')  
                            
    # create vrt
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
    gdal.BuildVRT(opj(processing_dir, 'Mosaic', 'Timescan', 'Timescan.vrt'),
                  outfiles,
                  options=vrt_options)       