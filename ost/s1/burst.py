# -*- coding: utf-8 -*-
'''This module handles the burst inventory

'''

import os
from os.path import join as opj
import shutil
import glob
import time
import datetime
import itertools

import gdal
import geopandas as gpd

from ost.helpers import scihub, vector as vec, raster as ras, helpers as h
from ost.s1 import burst_to_ard, ts
from ost import Sentinel1_Scene as S1Scene


def burst_inventory(inventory_df, outfile, download_dir=os.getenv('HOME'),
                    data_mount='/eodata', uname=None, pword=None):
    '''Creates a Burst GeoDataFrame from an OST inventory file

    Args:

    Returns:


    '''
    # create column names for empty data frame
    column_names = ['SceneID', 'Track', 'Direction', 'Date', 'SwathID',
                    'AnxTime', 'BurstNr', 'geometry']

    # crs for empty dataframe
    crs = {'init': 'epsg:4326', 'no_defs': True}
    # create empty dataframe
    gdf_full = gpd.GeoDataFrame(columns=column_names, crs=crs)
    # uname, pword = scihub.askScihubCreds()

    for scene_id in inventory_df.identifier:

        # read into S1scene class
        scene = S1Scene(scene_id)

        print(' INFO: Getting burst info from {}.'.format(scene.scene_id))
        
        # get orbit direction
        orbit_direction = inventory_df[
            inventory_df.identifier == scene_id].orbitdirection.values[0]

        filepath = scene.get_path(download_dir, data_mount)
        if not filepath:
            print(' INFO: Retrieving burst info from scihub'
                  ' (need to download xml files)')
            if not uname and not pword:
                uname, pword = scihub.ask_credentials()
                
            opener = scihub.connect(uname=uname, pword=pword)
            if scene.scihub_online_status(opener) is False:
                print(' INFO: Product needs to be online'
                      ' to create a burst database.')
                print(' INFO: Download the product first and '
                      ' do the burst list from the local data.')
            else:
                single_gdf = scene._scihub_annotation_get(uname, pword)
        elif filepath[-4:] == '.zip':
            single_gdf = scene._zip_annotation_get(download_dir, data_mount)
        elif filepath[-5:] == '.SAFE':
            single_gdf = scene._safe_annotation_get(download_dir, data_mount)
           

        # add orbit direction
        single_gdf['Direction'] = orbit_direction

        # append
        gdf_full = gdf_full.append(single_gdf)


    gdf_full = gdf_full.reset_index(drop=True)

    for i in gdf_full['AnxTime'].unique():

        # get similar burst times
        idx = gdf_full.index[(gdf_full.AnxTime >= i - 1) &
                             (gdf_full.AnxTime <= i + 1) &
                             (gdf_full.AnxTime != i)].unique().values

        # reset all to first value
        for j in idx:
            gdf_full.at[j, 'AnxTime'] = i

    # create the acrual burst id
    gdf_full['bid'] = gdf_full.Direction.str[0] + \
        gdf_full.Track.astype(str) + '_' + \
        gdf_full.SwathID.astype(str) + '_' + \
        gdf_full.AnxTime.astype(str)

    # save file to out
    gdf_full['Date'] = gdf_full['Date'].astype(str)
    gdf_full['BurstNr'] = gdf_full['BurstNr'].astype(str)
    gdf_full['AnxTime'] = gdf_full['AnxTime'].astype(str)
    gdf_full['Track'] = gdf_full['Track'].astype(str)
    gdf_full.to_file(outfile)
    
    return gdf_full


def refine_burst_inventory(aoi, burst_gdf, outfile):
    '''Creates a Burst GeoDataFrame from an OST inventory file

    Args:

    Returns:


    '''

    # turn aoi into a geodataframe
    aoi_gdf = vec.wkt_to_gdf(aoi)

    # get columns of input dataframe for later return function
    cols = burst_gdf.columns

    # 1) get only intersecting footprints (double, since we do this before)
    burst_gdf = gpd.sjoin(burst_gdf, aoi_gdf, how='inner', op='intersects')

    # if aoi  gdf has an id field we need to rename the changed id_left field
    if 'id_left' in burst_gdf.columns.tolist():
        # rename id_left to id
        burst_gdf.columns = (['id' if x == 'id_left' else x
                              for x in burst_gdf.columns.tolist()])

    # save file to out
    burst_gdf['Date'] = burst_gdf['Date'].astype(str)
    burst_gdf['BurstNr'] = burst_gdf['BurstNr'].astype(str)
    burst_gdf['AnxTime'] = burst_gdf['AnxTime'].astype(str)
    burst_gdf['Track'] = burst_gdf['Track'].astype(str)
    burst_gdf.to_file(outfile)
    return burst_gdf[cols]


def burst_to_ard_batch(burst_inventory, download_dir, processing_dir,
                       temp_dir, ard_parameters, data_mount='/eodata'):
    '''Handles the batch processing of a OST complinat burst inventory file

    Args:
        burst_inventory (GeoDataFrame):
        download_dir (str):
        processing_dir (str):
        temp_dir (str):
        ard_parameters (dict):

    '''

    resolution = ard_parameters['resolution']
    # border_noise = ard_parameters['border_noise']
    product_type = ard_parameters['product_type']
    speckle_filter = ard_parameters['speckle_filter']
    ls_mask_create = ard_parameters['ls_mask_create']
    to_db = ard_parameters['to_db']
    dem = ard_parameters['dem']
    coherence = ard_parameters['coherence']
    polarimetry = ard_parameters['polarimetry']
    pol_speckle_filter = ard_parameters['pol_speckle_filter']

    for burst in burst_inventory.bid.unique():      # ***

        # create a list of dates over which we loop
        dates = burst_inventory.Date[
                burst_inventory.bid == burst].sort_values().tolist()

        # loop through dates
        for idx, date in enumerate(dates):      # ******

            print(' INFO: Entering burst {} at date {}.'.format(burst, date))
            # get master date
            master_date = dates[idx]
            # we set this for handling the end of the time-series
            end = False
            coherence = ard_parameters['coherence']

            # try to get slave date
            try:
                slave_date = dates[idx + 1]    # last burst in timeseries?
            except IndexError:
                end = True
                print(' INFO: Reached the end of the time-series.'
                      ' Therefore no coherence calculation is done.')
            else:
                end = False

            # read master burst
            master_burst = burst_inventory[
                (burst_inventory.Date == master_date) &
                (burst_inventory.bid == burst)]

            master_scene = S1Scene(master_burst.SceneID.values[0])

            # get path to file
            master_file = master_scene.get_path(download_dir, data_mount)
            # get subswath
            subswath = master_burst.SwathID.values[0]
            # get burst number in file
            master_burst_nr = master_burst.BurstNr.values[0]
            # create a fileId
            master_id = '{}_{}'.format(master_date, master_burst.bid.values[0])

            # create out folder
            out_dir = '{}/{}/{}'.format(processing_dir, burst, date)
            os.makedirs(out_dir, exist_ok=True)

            # check if already processed
            if os.path.isfile(opj(out_dir, '.processed')):
                print(' INFO: Burst {} from {} already processed'.format(
                    burst, date))
                # return_code = 0
            else:
                if end is True:
                    coherence = False
                    slave_file, slave_burst_nr, slave_id = None, None, None
                else:
                    # read slave burst
                    slave_burst = burst_inventory[
                            (burst_inventory.Date == slave_date) &
                            (burst_inventory.bid == burst)]

                    slave_scene = S1Scene(slave_burst.SceneID.values[0])

                    # get path to slave file
                    slave_file = slave_scene.get_path(download_dir,
                                                      data_mount)

                    # burst number in slave file (subswath is same)
                    slave_burst_nr = slave_burst.BurstNr.values[0]

                    # outfile name
                    slave_id = '{}_{}'.format(slave_date,
                                              slave_burst.bid.values[0])

                # run routine
                burst_to_ard.burst_to_ard(
                     master_file=master_file,
                     swath=subswath,
                     master_burst_nr=master_burst_nr,
                     master_burst_id=master_id,
                     out_dir=out_dir,
                     temp_dir=temp_dir,
                     slave_file=slave_file,
                     slave_burst_nr=slave_burst_nr,
                     slave_burst_id=slave_id,
                     coherence=coherence,
                     polarimetry=polarimetry,
                     pol_speckle_filter=pol_speckle_filter,
                     resolution=resolution,
                     product_type=product_type,
                     speckle_filter=speckle_filter,
                     to_db=to_db,
                     ls_mask_create=ls_mask_create,
                     dem=dem,
                     remove_slave_import=False)

    # return return_code


def _ard_to_ts(burst_inventory, processing_dir, temp_dir,
               burst, to_db, ls_mask_create, ls_mask_apply, mt_speckle_filter,
               datatype):

    burst_dir = opj(processing_dir, burst)
    
    # get common burst extent
    list_of_scenes = glob.glob(opj(burst_dir, '20*', '*data*', '*img'))
    list_of_scenes = [x for x in list_of_scenes if 'layover' not in x]
    extent = opj(burst_dir, '{}.extent.shp'.format(burst))
    ts.mt_extent(list_of_scenes, extent, temp_dir, buffer=-0.0018)

    # remove inital extent
    for file in glob.glob(opj(burst_dir, 'tmp*')):
        os.remove(file)

    # layover/shadow mask
    if ls_mask_create is True:
        list_of_scenes = glob.glob(opj(burst_dir, '20*', '*data*', '*img'))
        list_of_layover = [x for x in list_of_scenes if 'layover' in x]
        out_ls = opj(burst_dir, '{}.ls_mask.tif'.format(burst))
        ts.mt_layover(list_of_layover, out_ls, temp_dir, extent=extent)
        print(' INFO: Our common layover mask is located at {}'.format(
              out_ls))

    if ls_mask_apply:
        print(' INFO: Calculating symetrical difference of extent and ls_mask')
        ras.polygonize_raster(out_ls, '{}.shp'.format(out_ls[:-4]))
        extent_ls_masked = opj(burst_dir, '{}.extent.masked.shp'.format(burst))
        vec.difference(extent, '{}.shp'.format(out_ls[:-4]), extent_ls_masked)
        extent = extent_ls_masked

    list_of_product_types = {'BS': 'Gamma0', 'coh': 'coh',
                             'ha_alpha': 'Alpha'}

    # we loop through each possible product
    for p, product_name in list_of_product_types.items():

        # we loop through each polarisation
        for pol in ['VV', 'VH', 'HH', 'HV']:

            # see if there is actually any imagery
            list_of_ts_bursts = sorted(glob.glob(
                opj(processing_dir, burst, '20*', '*data*', '{}*{}*img'
                    .format(product_name, pol))))

            if len(list_of_ts_bursts) > 1:

                # check for all datafiles of this product type
                list_of_ts_bursts = sorted(glob.glob(
                    opj(processing_dir, burst, '20*/', '*{}*dim'.format(
                            p))))
                list_of_ts_bursts = '\'{}\''.format(
                    ','.join(list_of_ts_bursts))

                # define out_dir for stacking routine

                out_dir = opj(processing_dir,
                              '{}/Timeseries'.format(burst))
                os.makedirs(out_dir, exist_ok=True)

                # create namespaces

                temp_stack = opj(temp_dir,
                                 '{}_{}_{}_mt'.format(burst, p, pol))

                out_stack = opj(out_dir,
                                '{}_{}_{}_mt'.format(burst, p, pol))

                stack_log = opj(out_dir,
                                '{}_{}_{}_stack.err_log'.format(
                                    burst, p, pol))

                # run stacking routines
                ts.create_stack(list_of_ts_bursts, temp_stack, stack_log,
                                polarisation=pol)

                # run mt speckle filter
                if mt_speckle_filter is True:
                    speckle_log = opj(
                        out_dir, '{}_{}_{}_mt_speckle.err_log'.format(
                            burst, p, pol))

                    ts.mt_speckle_filter('{}.dim'.format(temp_stack),
                                         out_stack, speckle_log)
                    # remove tmp files
                    h.delete_dimap(temp_stack)
                else:
                    out_stack = temp_stack

                # convert to GeoTiffs
                if p == 'BS':
                    # get the dates of the files
                    dates = [datetime.datetime.strptime(
                        x.split('_')[-1][:-4], '%d%b%Y')
                            for x in glob.glob(
                                opj('{}.data'.format(out_stack), '*img'))]
                    # sort them
                    dates.sort()
                    # write them back to string for following loop
                    sortedDates = [datetime.datetime.strftime(
                        ts, "%d%b%Y") for ts in dates]

                    i, outfiles = 1, []
                    for date in sortedDates:

                        # restructure date to YYMMDD
                        inDate = datetime.datetime.strptime(date, '%d%b%Y')
                        outDate = datetime.datetime.strftime(inDate,
                                                             '%y%m%d')

                        infile = glob.glob(opj('{}.data'.format(out_stack),
                                               '*{}*{}*img'.format(
                                                           pol, date)))[0]

                        # create outfile
                        outfile = opj(out_dir, '{}.{}.{}.{}.tif'.format(
                            i, outDate, p, pol))

                        # mask by extent
                        ras.mask_by_shape(
                            infile, outfile,
                            extent,
                            to_db=to_db, datatype=datatype,
                            min_value=-30, max_value=5,
                            ndv=0)
                        # add ot a list for subsequent vrt creation
                        outfiles.append(outfile)

                        i += 1

                    # build vrt of timeseries
                    vrt_options = gdal.BuildVRTOptions(srcNodata=0,
                                                       separate=True)
                    gdal.BuildVRT(opj(out_dir,
                                      'Timeseries.{}.{}.vrt'.format(
                                          p, pol)),
                                  outfiles,
                                  options=vrt_options)

                if p == 'coh':

                    # get slave and master Date
                    mstDates = [datetime.datetime.strptime(
                        os.path.basename(x).split('_')[3].split('.')[0],
                        '%d%b%Y') for x in glob.glob(
                            opj('{}.data'.format(out_stack), '*img'))]

                    slvDates = [datetime.datetime.strptime(
                        os.path.basename(x).split('_')[4].split('.')[0],
                        '%d%b%Y') for x in glob.glob(
                            opj('{}.data'.format(out_stack), '*img'))]
                    # sort them
                    mstDates.sort()
                    slvDates.sort()
                    # write them back to string for following loop
                    sortedMstDates = [datetime.datetime.strftime(
                        ts, "%d%b%Y") for ts in mstDates]
                    sortedSlvDates = [datetime.datetime.strftime(
                        ts, "%d%b%Y") for ts in slvDates]

                    i, outfiles = 1, []
                    for mst, slv in zip(sortedMstDates, sortedSlvDates):

                        inMst = datetime.datetime.strptime(mst, '%d%b%Y')
                        inSlv = datetime.datetime.strptime(slv, '%d%b%Y')

                        outMst = datetime.datetime.strftime(inMst,
                                                            '%y%m%d')
                        outSlv = datetime.datetime.strftime(inSlv,
                                                            '%y%m%d')

                        infile = glob.glob(opj('{}.data'.format(out_stack),
                                               '*{}*{}_{}*img'.format(
                                                       pol, mst, slv)))[0]
                        outfile = opj(out_dir, '{}.{}.{}.{}.{}.tif'.format(
                            i, outMst, outSlv, p, pol))

                        ras.mask_by_shape(
                            infile, outfile,
                            extent,
                            to_db=False, datatype=datatype,
                            min_value=0.000001, max_value=1,
                            ndv=0)

                        # add ot a list for subsequent vrt creation
                        outfiles.append(outfile)

                        i += 1

                    # build vrt of timeseries
                    vrt_options = gdal.BuildVRTOptions(srcNodata=0,
                                                       separate=True)
                    gdal.BuildVRT(
                            opj(out_dir,
                                'Timeseries.{}.{}.vrt'.format(p, pol)),
                            outfiles,
                            options=vrt_options)

                # remove tmp files
                h.delete_dimap(out_stack)

    for pol in ['Alpha', 'Entropy', 'Anisotropy']:

        list_of_ts_bursts = sorted(glob.glob(
            opj(processing_dir, burst, '20*',
                '*{}*'.format(p), '*{}.img'.format(pol))))

        if len(list_of_ts_bursts) > 1:

            list_of_ts_bursts = sorted(glob.glob(
                opj(processing_dir, burst, '20*/', '*{}*dim'.format(p))))
            list_of_ts_bursts = '\'{}\''.format(','.join(
                list_of_ts_bursts))

            # print(list_of_ts_bursts)

            out_dir = opj(processing_dir, '{}/Timeseries'.format(burst))
            os.makedirs(out_dir, exist_ok=True)

            temp_stack = opj(temp_dir, '{}_{}_mt'.format(burst, pol))
            out_stack = opj(out_dir, '{}_{}_mt'.format(burst, pol))

            stack_log = opj(out_dir,
                            '{}_{}_stack.err_log'.format(burst, pol))
            # processing routines
            ts.create_stack(list_of_ts_bursts, temp_stack, stack_log,
                            pattern=pol)

            if mt_speckle_filter is True:
                speckle_log = opj(out_dir,
                                  '{}_{}_mt_speckle.err_log'.format(
                                         burst, pol))
                ts.mt_speckle_filter('{}.dim'.format(temp_stack),
                                     out_stack, speckle_log)
                # remove tmp files
                h.delete_dimap(temp_stack)
            else:
                out_stack = temp_stack

            # get the dates of the files
            dates = [datetime.datetime.strptime(x.split('_')[-1][:-4],
                     '%d%b%Y') for x in glob.glob(
                        opj('{}.data'.format(out_stack), '*img'))]
            # sort them
            dates.sort()
            # write them back to string for following loop
            sortedDates = [datetime.datetime.strftime(
                ts, "%d%b%Y") for ts in dates]

            i, outfiles = 1, []
            for date in sortedDates:

                # restructure date to YYMMDD
                inDate = datetime.datetime.strptime(date, '%d%b%Y')
                outDate = datetime.datetime.strftime(inDate, '%y%m%d')

                infile = glob.glob(opj('{}.data'.format(out_stack),
                                       '*{}*{}*img'.format(pol, date)))[0]
                # create outfile
                outfile = opj(out_dir, '{}.{}.{}.{}.tif'.format(
                        i, outDate, p, pol))
                # mask by extent
                max_value = 90 if pol is 'Alpha' else 1
                ras.mask_by_shape(
                    infile, outfile,
                    extent,
                    to_db=False, datatype=datatype, min_value=0.000001,
                    max_value=max_value, ndv=0)

                # add ot a list for subsequent vrt creation
                outfiles.append(outfile)
                i += 1

            # build vrt of timeseries
            vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
            gdal.BuildVRT(opj(out_dir, 'Timeseries.{}.vrt'.format(pol)),
                          outfiles,
                          options=vrt_options)

            # remove tmp files
            h.delete_dimap(out_stack)


def burst_ards_to_timeseries(burst_inventory, processing_dir, temp_dir,
                             ard_parameters):

    datatype = ard_parameters['datatype']
    to_db = ard_parameters['to_db_mt']
    
    if to_db:
        to_db_mt = False
    else:
        to_db_mt = ard_parameters['to_db_mt']
    
    ls_mask_create = ard_parameters['ls_mask_create']
    ls_mask_apply = ard_parameters['ls_mask_apply']
    mt_speckle_filter = ard_parameters['mt_speckle_filter']

    for burst in burst_inventory.bid.unique():      # ***

        #if os.path.isfile(opj(burst_dir, 'Timeseries', '.processed')):
        #    print(' INFO: Timeseries for track {} already processed.'.format(burst))
        #else:
        _ard_to_ts(burst_inventory, processing_dir, temp_dir,
               burst, to_db_mt, ls_mask_create, ls_mask_apply,
               mt_speckle_filter, datatype)


# --------------------
# timescan part
# --------------------
def _timeseries_to_timescan(burst_inventory, processing_dir, temp_dir,
                            burst_dir, to_db, metrics, outlier_removal):

    product_list = ['BS.HH', 'BS.VV', 'BS.HV', 'BS.VH',
                    'coh.VV', 'coh.VH', 'Alpha', 'Entropy', 'Anisotropy']

    for product in product_list:
        for timeseries in glob.glob(opj(burst_dir, 'Timeseries',
                                        '*{}*vrt'.format(product))):

            print(' INFO: Creating timescan for {}'.format(product))
            timescan_dir = opj(burst_dir, 'Timescan')
            os.makedirs(timescan_dir, exist_ok=True)

            # we get the name of the time-series parameter
            polarisation = timeseries.split('/')[-1].split('.')[2]
            if polarisation == 'vrt':
                timescan_prefix = opj(
                    '{}'.format(timescan_dir),
                    '{}'.format(timeseries.split('/')[-1].split('.')[1]))
            else:
                timescan_prefix = opj(
                    '{}'.format(timescan_dir),
                    '{}.{}'.format(timeseries.split('/')[-1].split('.')[1],
                                   polarisation))

            start = time.time()
            if 'BS.' in timescan_prefix:    # backscatter
                ts.mt_metrics(timeseries, timescan_prefix, metrics,
                              rescale_to_datatype=True,
                              to_power=to_db,
                              outlier_removal=outlier_removal)
            else:   # non-backscatter
                ts.mt_metrics(timeseries, timescan_prefix, metrics,
                              rescale_to_datatype=False,
                              to_power=False,
                              outlier_removal=outlier_removal)

            h.timer(start)

    # rename and create vrt
    # print('renaming')
    i, list_of_files = 0, []
    for product in itertools.product(product_list, metrics):

        file = glob.glob(
            opj(burst_dir, 'Timescan', '*{}.{}.tif'.format(
                product[0], product[1])))

        if file:
            i += 1
            outfile = opj(burst_dir, 'Timescan', '{}.{}.{}.tif'.format(
                i, product[0], product[1]))
            shutil.move(file[0], outfile)
            list_of_files.append(outfile)

    # create vrt
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
    gdal.BuildVRT(opj(burst_dir, 'Timescan', 'Timescan.vrt'),
                  list_of_files,
                  options=vrt_options)


def timeseries_to_timescan(burst_inventory, processing_dir, temp_dir,
                           ard_parameters):
    '''Function to create a timescan out of a OST timeseries.

    '''

    if ard_parameters['to_db_mt'] or ard_parameters['to_db']:
        to_db = True
    else:
        to_db = False

    metrics = ard_parameters['metrics']
    outlier_removal = ard_parameters['outlier_removal']

    for burst in burst_inventory.bid.unique():   # ***

        burst_dir = opj(processing_dir, burst)

        print(' INFO: Entering burst {}'.format(burst))
        _timeseries_to_timescan(burst_inventory, processing_dir, temp_dir,
                                burst_dir, to_db, metrics, outlier_removal)


def mosaic_timeseries(burst_inventory, processing_dir, temp_dir,
                      ard_parameters):

    product_list = ['BS.HH', 'BS.VV', 'BS.HV', 'BS.VH',
                    'coh.VV', 'coh.VH', 'ha_alpha.Alpha',
                    'ha_alpha.Entropy', 'ha_alpha.Anisotropy']

    os.makedirs(opj(processing_dir, 'Mosaic', 'Timeseries'), exist_ok=True)

    # we do this to get the minimum number of
    # timesteps per burst (should be actually the same)
    length = 99999
    for burst in burst_inventory.bid.unique():

        length_of_burst = len(burst_inventory[burst_inventory.bid == burst])

        if length_of_burst < length:
            length = length_of_burst

    # now we loop through each timestep and product
    for product in product_list:  # ****
        
        list_of_files = []
        for i in range(length):

            filelist = glob.glob(
                opj(processing_dir, '*_IW*_*', 'Timeseries', '{}.*{}.tif'
                    .format(i + 1, product)))
            
            if filelist:
                print(' INFO: Creating timeseries mosaic {} for {}.'.format(
                    i + 1, product))
    
                datelist = []
                
                for file in filelist:
                    if '.coh.' in file:
                        datelist.append('{}_{}'.format(
                            os.path.basename(file).split('.')[2],
                            os.path.basename(file).split('.')[1]))
                    else:
                        datelist.append(os.path.basename(file).split('.')[1])
                
                start = sorted(datelist)[0]
                end = sorted(datelist)[-1]
                
                out_dir = opj(processing_dir, 'Mosaic', 'Timeseries')
                os.makedirs(out_dir, exist_ok=True)
                
                if start == end:
                    outfile = opj(out_dir, '{}.{}.{}.tif'.format(i + 1, start, product))
                else:
                    outfile = opj(out_dir, '{}.{}-{}.{}.tif'.format(i + 1, start, end, product))
                    
                    
                list_of_files.append(outfile)
                filelist = ' '.join(filelist)

                # the command
                command = ('otbcli_Mosaic -il {} -comp.feather large '
                           '-tmpdir {} -progress 1 -out {} float'.format(
                               filelist, temp_dir, outfile))
                os.system(command)

        # create vrt
        if list_of_files:
            vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
            gdal.BuildVRT(opj(out_dir, '{}.Timeseries.vrt'.format(product)),
                          list_of_files,
                          options=vrt_options)


def mosaic_timescan(burst_inventory, processing_dir, temp_dir, ard_parameters):

    product_list = ['BS.HH', 'BS.VV', 'BS.HV', 'BS.VH',
                    'coh.VV', 'coh.VH', 'Alpha', 'Entropy', 'Anisotropy']
    metrics = ard_parameters['metrics']

    os.makedirs(opj(processing_dir, 'Mosaic', 'Timescan'), exist_ok=True)
    i, list_of_files = 0, []
    for product in itertools.product(product_list, metrics):   # ****

        filelist = ' '.join(glob.glob(
            opj(processing_dir, '*', 'Timescan', '*{}.{}.tif'.format(
                product[0], product[1]))))

        if filelist:
            i += 1
            outfile = opj(processing_dir, 'Mosaic', 'Timescan',
                          '{}.{}.{}.tif'.format(i, product[0], product[1]))
            command = ('otbcli_Mosaic -il {} -comp.feather large -tmpdir {}'
                       ' -progress 1 -out {} float'.format(
                               filelist, temp_dir, outfile))
            os.system(command)
            list_of_files.append(outfile)

    # create vrt
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
    gdal.BuildVRT(opj(processing_dir, 'Mosaic', 'Timescan', 'Timescan.vrt'),
                  list_of_files,
                  options=vrt_options)
