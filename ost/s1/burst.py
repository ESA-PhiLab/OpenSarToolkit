# -*- coding: utf-8 -*-
'''This module handles the burst inventory

'''

import os
from os.path import join as opj
import glob
import json
import itertools

import gdal
import geopandas as gpd

from ost.helpers import scihub, vector as vec 
from ost.s1 import burst_to_ard
from ost import Sentinel1_Scene as S1Scene
from ost.helpers import raster as ras
from ost.multitemporal import common_extent
from ost.multitemporal import common_ls_mask
from ost.multitemporal import ard_to_ts
from ost.multitemporal import timescan
from ost.mosaic import mosaic


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


def refine_burst_inventory(aoi, burst_gdf, outfile, coverages=None):
    '''Creates a Burst GeoDataFrame from an OST inventory file

    Args:

    Returns:


    '''

    # turn aoi into a geodataframe
    aoi_gdf = gpd.GeoDataFrame(vec.wkt_to_gdf(aoi).buffer(0.05))
    aoi_gdf.columns = ['geometry']
    # get columns of input dataframe for later return function
    cols = burst_gdf.columns

    # 1) get only intersecting footprints (double, since we do this before)
    burst_gdf = gpd.sjoin(burst_gdf, aoi_gdf, how='inner', op='intersects')

    # if aoi  gdf has an id field we need to rename the changed id_left field
    if 'id_left' in burst_gdf.columns.tolist():
        # rename id_left to id
        burst_gdf.columns = (['id' if x == 'id_left' else x
                              for x in burst_gdf.columns.tolist()])
    
    # remove duplicates
    burst_gdf.drop_duplicates(['SceneID', 'Date', 'bid'], inplace=True)
    
    # check if number of bursts align with number of coverages
    if coverages:
        for burst in burst_gdf.bid.unique():
            if len(burst_gdf[burst_gdf.bid == burst]) != coverages:
                print(' INFO. Removing burst {} because of'
                      ' unsuffcient coverage.'.format(burst))
                burst_gdf.drop(burst_gdf[burst_gdf.bid == burst].index, 
                               inplace=True)
    
    # save file to out
    burst_gdf['Date'] = burst_gdf['Date'].astype(str)
    burst_gdf['BurstNr'] = burst_gdf['BurstNr'].astype(str)
    burst_gdf['AnxTime'] = burst_gdf['AnxTime'].astype(str)
    burst_gdf['Track'] = burst_gdf['Track'].astype(str)
    burst_gdf.to_file(outfile)
    return burst_gdf[cols]


def burst_to_ard_batch(burst_inventory, download_dir, processing_dir,
                       temp_dir, proc_file, data_mount='/eodata', 
                       exec_file=None,ncores=os.cpu_count()):
    '''Handles the batch processing of a OST complinat burst inventory file

    Args:
        burst_inventory (GeoDataFrame):
        download_dir (str):
        processing_dir (str):
        temp_dir (str):
        ard_parameters (dict):

    '''

    # load ard parameters
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        ard = ard_params['single ARD']

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
            coherence = ard['coherence']

            # try to get slave date
            try:
                slave_date = dates[idx + 1]    # last burst in timeseries?
            except IndexError:
                end = True
                print(' INFO: Reached the end of the time-series.'
                      ' Therefore no coherence calculation is done.')
                if ard['product type'] == 'Coherence_only':
                    continue
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
            out_dir = opj(processing_dir, burst, date)
            os.makedirs(out_dir, exist_ok=True)

            # check if already processed
            if os.path.isfile(opj(out_dir, '.processed')):
                print(' INFO: Burst {} from {} already processed'.format(
                      burst, date))
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
    
                # just write command into a textfile
                if exec_file:
                    # remove older files in case they exist - need a better way than this as we would like to append to
                    # a text file which can then be read line by line
                    #if os.path.isfile(exec_file):
                    #    os.remove(exec_file)
                    # construct command arguments
                    '''args = ('-m {} -ms {} -mn {} -mi {} -p {} -o {} -t {} '
                            '-s {} -sn {} -si {} -c {} -r {} -nc {}').format(
                                  master_file, subswath, master_burst_nr, master_id, 
                                  proc_file, out_dir, temp_dir, 
                                  slave_file, slave_burst_nr, slave_id, 
                                  coherence, False, ncores)                                
                    
                    # get path to graph
                    rootpath = imp.find_module('ost')[1]
                    python_exe = opj(rootpath, 's1', 'burst_to_ard.py')
                    with open(exec_file, 'a') as exe:
                        exe.write('{} {} \n'.format(python_exe, args))'''

                    parallel_temp_dir=temp_dir+'/temp_'+burst+'_'+date
                    os.makedirs(parallel_temp_dir, exist_ok=True)

                    args = ('{};{};{};{};{};{};{};{};{};{};{};{};{}').format(
                                  master_file, subswath, master_burst_nr, master_id,
                                  proc_file, out_dir, parallel_temp_dir,
                                  slave_file, slave_burst_nr, slave_id,
                                  coherence, False, ncores)

                    # get path to graph
                    #rootpath = imp.find_module('ost')[1]
                    #python_exe = opj(rootpath, 's1', 'burst_to_ard.py')
                    exec_burst_to_ard=exec_file+'_burst_to_ard.txt'
                    with open(exec_burst_to_ard, 'a') as exe:
                        exe.write('{}\n'.format(args))

                
                # run the command      
                else:
                    # run routine
                    burst_to_ard.burst_to_ard(
                         master_file=master_file,
                         swath=subswath,
                         master_burst_nr=master_burst_nr,
                         master_burst_id=master_id,
                         proc_file=proc_file,
                         out_dir=out_dir,
                         temp_dir=temp_dir,
                         slave_file=slave_file,
                         slave_burst_nr=slave_burst_nr,
                         slave_burst_id=slave_id,
                         coherence=coherence,
                         remove_slave_import=False)
            
            
def burst_ards_to_timeseries(burst_inventory, processing_dir, temp_dir,
                             proc_file, exec_file=None, ncores=os.cpu_count()):

    # load ard parameters
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        ard = ard_params['single ARD']
        ard_mt = ard_params['single ARD']
        
    # create extents
    for burst in burst_inventory.bid.unique():      # ***

        # get the burst directory
        burst_dir = opj(processing_dir, burst)
        
        # get common burst extent
        list_of_bursts = glob.glob(opj(burst_dir, '20*', '*data*', '*img'))
        list_of_bursts = [x for x in list_of_bursts if 'layover' not in x]
        extent = opj(burst_dir, '{}.extent.shp'.format(burst))
        
        if os.path.isfile(extent):
            continue
        
        # placeholder for parallelisation
        if exec_file:
            parallel_temp_dir = temp_dir + '/temp_' + burst + '_mt_extent'
            os.makedirs(parallel_temp_dir, exist_ok=True)

            args = ('{};{};{};{}').format(
                list_of_bursts, extent, parallel_temp_dir, -0.0018)

            # get path to graph
            # rootpath = imp.find_module('ost')[1]
            # python_exe = opj(rootpath, 's1', 'ard_to_ts.py')
            exec_mt_extent = exec_file + '_mt_extent.txt'
            with open(exec_mt_extent, 'a') as exe:
                exe.write('{}\n'.format(args))
            #if os.path.isfile(exec_file):
            #    os.remove(exec_file)
            #print('create command')
        
        else:
            print(' INFO: Creating common extent mask for burst {}'.format(burst))
            common_extent.mt_extent(list_of_bursts, extent, temp_dir, -0.0018)
      
    if ard['create ls mask'] or ard['apply ls mask']: 
        
        # create layover
        for burst in burst_inventory.bid.unique():      # ***
    
            # get the burst directory
            burst_dir = opj(processing_dir, burst)
            
            # get common burst extent
            list_of_scenes = glob.glob(opj(burst_dir, '20*', '*data*', '*img'))
            list_of_layover = [x for x in list_of_scenes if 'layover' in x]
        
            # layover/shadow mask
            out_ls = opj(burst_dir, '{}.ls_mask.tif'.format(burst))

            if os.path.isfile(out_ls):
                continue
            if exec_file:
                parallel_temp_dir = temp_dir + '/temp_' + burst + '_ls_mask'
                os.makedirs(parallel_temp_dir, exist_ok=True)

                args = ('{};{};{};{};{}').format(
                    list_of_layover, out_ls, parallel_temp_dir,
                    extent, ard_mt['apply ls mask'])

                # get path to graph
                # rootpath = imp.find_module('ost')[1]
                # python_exe = opj(rootpath, 's1', 'ard_to_ts.py')
                exec_mt_ls = exec_file + '_mt_ls.txt'
                with open(exec_mt_ls, 'a') as exe:
                    exe.write('{}\n'.format(args))
            else:
                print(' INFO: Creating common Layover/Shadow mask'
                    ' for burst {}'.format(burst))
                common_ls_mask.mt_layover(list_of_layover, out_ls, temp_dir,
                                          extent, ard_mt['apply ls mask'])
        
    # create timeseries
    for burst in burst_inventory.bid.unique():
        
        dict_of_product_types = {'bs': 'Gamma0', 'coh': 'coh', 'pol': 'pol'}
        pols = ['VV', 'VH', 'HH', 'HV', 'Alpha', 'Entropy', 'Anisotropy']
        
        for pr, pol in itertools.product(dict_of_product_types.items(), pols):

            product = pr[0]
            product_name = pr[1]
        
            # take care of H-A-Alpha naming for file search
            if pol in ['Alpha', 'Entropy', 'Anisotropy'] and product is 'pol':
                product_name = '*'
            
            # see if there is actually any imagery in thi polarisation
            list_of_files = sorted(glob.glob(
                opj(processing_dir, burst, '20*', '*data*', '{}*{}*img'
                .format(product_name, pol))))
     
            if not len(list_of_files) > 1:
                continue
            
            # create list of dims if polarisation is present
            list_of_dims = sorted(glob.glob(
                opj(processing_dir, burst, '20*', '*{}*dim'.format(product)
                ))) 
            
            # placeholder for parallelisation
            if exec_file:
                #if os.path.isfile(exec_file):
                #    os.remove(exec_file)
                parallel_temp_dir = temp_dir + '/temp_' + burst + '_timeseries'
                os.makedirs(parallel_temp_dir, exist_ok=True)

                args = ('{};{};{};{};{};{};{};{}').format(
                    list_of_dims, processing_dir, parallel_temp_dir,
                    burst, proc_file, product, pol, ncores)

                # get path to graph
                #rootpath = imp.find_module('ost')[1]
                #python_exe = opj(rootpath, 's1', 'ard_to_ts.py')
                exec_timeseries=exec_file+'_timeseries.txt'
                with open(exec_timeseries, 'a') as exe:
                    exe.write('{}\n'.format(args))
            
            # run processing
            else:
                ard_to_ts.ard_to_ts(
                            list_of_dims, 
                            processing_dir, 
                            temp_dir, 
                            burst, 
                            proc_file, 
                            product=product, 
                            pol=pol,
                            ncores=os.cpu_count()
            )
            
# --------------------
# timescan part
# --------------------
def timeseries_to_timescan(burst_inventory, processing_dir, temp_dir, 
                           proc_file, exec_file=None):
    '''Function to create a timescan out of a OST timeseries.

    '''

    # load ard parameters
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        ard = ard_params['single ARD']
        ard_mt = ard_params['time-series ARD']
        ard_tscan = ard_params['time-scan ARD']
    
    
    # get the db scaling right
    to_db = ard['to db']
    if ard['to db'] or ard_mt['to db']:
        to_db = True
    
    # a products list
    product_list = ['bs.HH', 'bs.VV', 'bs.HV', 'bs.VH',
                    'coh.VV', 'coh.VH', 'coh.HH', 'coh.HV', 
                    'pol.Entropy', 'pol.Anisotropy', 'pol.Alpha']
    
    # get datatype right
    dtype_conversion = True if ard_mt['dtype output'] != 'float32' else False
    
    for burst in burst_inventory.bid.unique():   # ***

        print(' INFO: Entering burst {}.'.format(burst))
        # get burst directory
        burst_dir = opj(processing_dir, burst)
        # get timescan directory
        timescan_dir = opj(burst_dir, 'Timescan')
        os.makedirs(timescan_dir, exist_ok=True)
    
        for product in product_list:
        
            if os.path.isfile(
                opj(timescan_dir, '.{}.processed'.format(product))):
                print(' INFO: Timescans for burst {} already'
                      ' processed.'.format(burst))
                continue
            # get respective timeseries
            timeseries = opj(burst_dir, 
                             'Timeseries',
                             'Timeseries.{}.vrt'.format(product))
            
            if not os.path.isfile(timeseries):
                continue
            
            print(' INFO: Creating Timescans of {} for burst {}.'.format(product, burst))
            # datelist for harmonics
            scenelist = glob.glob(
                    opj(burst_dir, 'Timeseries', '*{}*tif'.format(product))
            )
            
            datelist = []
            for layer in sorted(scenelist):
                datelist.append(os.path.basename(layer).split('.')[1][:6])
    
            # define timescan prefix
            timescan_prefix = opj(timescan_dir, product)

            # get rescaling and db right (backscatter vs. polarimetry)
            if 'bs.' in timescan_prefix:    # backscatter
                rescale = dtype_conversion
                to_power = to_db                       
            else:
                to_power = False
                rescale = False
            
            # placeholder for parallelisation
            if exec_file:
                # if os.path.isfile(exec_file):
                #    os.remove(exec_file)
                #parallel_temp_dir = temp_dir + '/temp_' + burst + '_timescan'
                #os.makedirs(parallel_temp_dir, exist_ok=True)

                args = ('{};{};{};{};{};{};{}').format(
                    timeseries, timescan_prefix, ard_tscan['metrics'],
                    rescale, to_power, ard_tscan['remove outliers'], datelist)

                # get path to graph
                # rootpath = imp.find_module('ost')[1]
                # python_exe = opj(rootpath, 's1', 'timescan.py')
                exec_tscan=exec_file+'_tscan.txt'
                with open(exec_tscan, 'a') as exe:
                    exe.write('{}\n'.format(args))

            # run command
            else:
                timescan.mt_metrics(
                    timeseries, 
                    timescan_prefix, 
                    ard_tscan['metrics'],
                    rescale_to_datatype=rescale,
                    to_power=to_power,
                    outlier_removal=ard_tscan['remove outliers'], 
                    datelist=datelist
            )
        
        if not exec_file:
            ras.create_tscan_vrt(timescan_dir, proc_file)
        else:
            exec_tscan_vrt=exec_file+'_tscan_vrt.txt'
            with open(exec_tscan_vrt, 'a') as exe:
                exe.write('{};{}\n'.format(timescan_dir, proc_file))


def mosaic_timeseries(burst_inventory, processing_dir, temp_dir, 
                      cut_to_aoi=False, exec_file=None, ncores=os.cpu_count()):

    print(' ------------------------------------')
    print(' INFO: Mosaicking Time-series layers.')
    print(' ------------------------------------')
    
    # create output folder
    ts_dir = opj(processing_dir, 'Mosaic', 'Timeseries')
    os.makedirs(ts_dir, exist_ok=True)

    # a products list
    product_list = ['bs.HH', 'bs.VV', 'bs.HV', 'bs.VH',
                    'coh.VV', 'coh.VH', 'coh.HH', 'coh.HV', 
                    'pol.Entropy', 'pol.Anisotropy', 'pol.Alpha']
    
    # now we loop through each timestep and product
    for product in product_list:  # ****
        
        bursts = burst_inventory.bid.unique()
        nr_of_ts = len(glob.glob(opj(
                processing_dir, 
                bursts[0], 
                'Timeseries', 
                '*.{}.tif'.format(product)))
        )
    
        if not nr_of_ts > 1:
            continue
        
        outfiles = []
        for i in range(1, nr_of_ts + 1):

            filelist = glob.glob(opj(
                    processing_dir, '*', 'Timeseries', '{:02d}.*{}.tif'
                    .format(i, product)))
            filelist = [file for file in filelist if 'Mosaic' not in file]
            

            print(' INFO: Creating timeseries mosaic {:02d} for {}.'.format(
                    i, product))

            # create dates for timseries naming
            datelist = []
            for file in filelist:
                if '.coh.' in file:
                    datelist.append('{}_{}'.format(
                        os.path.basename(file).split('.')[2],
                        os.path.basename(file).split('.')[1]))
                else:
                    datelist.append(os.path.basename(file).split('.')[1])
                
            start, end = sorted(datelist)[0], sorted(datelist)[-1]
            filelist = ' '.join(filelist)
            
            if start == end:
                outfile = opj(ts_dir, '{:02d}.{}.{}.tif'.format(i, start, product))
                
            else:
                outfile = opj(ts_dir, '{:02d}.{}-{}.{}.tif'.format(i, start, end, product))
            
            check_file = opj(
                os.path.dirname(outfile),
                '.{}.processed'.format(os.path.basename(outfile)[:-4])
            )
            
            outfiles.append(outfile)
            
            if os.path.isfile(check_file):
                print( 'INFO: Mosaic layer {} already'
                       ' processed.'.format(outfile))
                continue
            if exec_file:
                filelist=filelist.split(" ")
                if cut_to_aoi == False:
                    cut_to_aoi = 'False'
                parallel_temp_dir = temp_dir + '/temp_' + product + '_' + str(i) + '_mosaic_timeseries'
                os.makedirs(parallel_temp_dir, exist_ok=True)
                args = ('{};{};{};{};{}').format(
                    filelist, outfile, parallel_temp_dir, cut_to_aoi, ncores)

                # get path to graph
                # rootpath = imp.find_module('ost')[1]
                # python_exe = opj(rootpath, 'mosaic', 'mosaic.py')
                exec_mosaic_timeseries = exec_file + '_mosaic_timeseries.txt'
                with open(exec_mosaic_timeseries, 'a') as exe:
                    exe.write('{}\n'.format(args))
            else:
                # the command
                print(' INFO: Mosaicking layer {}.'.format(os.path.basename(outfile)))
                mosaic.mosaic(filelist, outfile, temp_dir, cut_to_aoi)

        # create vrt
        if not exec_file:
            # create final vrt
            vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
            gdal.BuildVRT(opj(ts_dir, '{}.Timeseries.vrt'.format(product)),
                          outfiles,
                          options=vrt_options)
        else:
            # create vrt exec file

            exec_mosaic_ts_vrt = exec_file + '_mosaic_ts_vrt.txt'
            with open(exec_mosaic_ts_vrt, 'a') as exe:
                exe.write('{};{};{}\n'.format(ts_dir, product, outfiles))


def mosaic_timescan(burst_inventory, processing_dir, temp_dir, proc_file,
                    cut_to_aoi=False, exec_file=None, ncores=os.cpu_count()):

    
    # load ard parameters
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        metrics = ard_params['time-scan ARD']['metrics']

    if 'harmonics' in metrics:
        metrics.remove('harmonics')
        metrics.extend(['amplitude', 'phase', 'residuals'])
        
    if 'percentiles' in metrics:
            metrics.remove('percentiles')
            metrics.extend(['p95', 'p5'])
        
    # a products list
    product_list = ['bs.HH', 'bs.VV', 'bs.HV', 'bs.VH',
                    'coh.VV', 'coh.VH', 'coh.HH', 'coh.HV', 
                    'pol.Entropy', 'pol.Anisotropy', 'pol.Alpha']
    
    
    tscan_dir = opj(processing_dir, 'Mosaic', 'Timescan')
    os.makedirs(tscan_dir, exist_ok=True)
    outfiles = []
    
    for product, metric in itertools.product(product_list, metrics):   # ****

        filelist = glob.glob(
            opj(processing_dir, '*', 'Timescan', 
                '*{}.{}.tif'.format(product, metric))
        )

        if not len(filelist) >= 1:
            continue
        
        filelist = ' '.join(filelist)

        outfile = opj(tscan_dir, '{}.{}.tif'.format(product, metric))
        check_file = opj(
                os.path.dirname(outfile),
                '.{}.processed'.format(os.path.basename(outfile)[:-4])
        )

        if os.path.isfile(check_file):
            print(' INFO: Mosaic layer {} already '
                  ' processed.'.format(os.path.basename(outfile)))
            continue
        if exec_file:
            filelist = filelist.split(" ")
            if cut_to_aoi==False:
                cut_to_aoi = 'False'

            parallel_temp_dir = temp_dir + '/temp_' + product + '_mosaic_tscan'
            os.makedirs(parallel_temp_dir, exist_ok=True)
            args = ('{};{};{};{};{}').format(
                filelist, outfile, parallel_temp_dir, cut_to_aoi, ncores)

            # get path to graph
            # rootpath = imp.find_module('ost')[1]
            # python_exe = opj(rootpath, 'mosaic', 'mosaic.py')
            exec_mosaic_timescan = exec_file + '_mosaic_tscan.txt'
            with open(exec_mosaic_timescan, 'a') as exe:
                exe.write('{}\n'.format(args))
        else:
            print(' INFO: Mosaicking layer {}.'.format(os.path.basename(outfile)))
            mosaic.mosaic(filelist, outfile, temp_dir, cut_to_aoi)
            outfiles.append(outfile)

    if not exec_file:
        # create vrt
        ras.create_tscan_vrt(tscan_dir, proc_file)

    else:
        #create vrt exec file
        exec_mosaic_tscan_vrt = exec_file + '_mosaic_tscan_vrt.txt'
        with open(exec_mosaic_tscan_vrt, 'a') as exe:
            exe.write('{};{}\n'.format(tscan_dir, proc_file))

