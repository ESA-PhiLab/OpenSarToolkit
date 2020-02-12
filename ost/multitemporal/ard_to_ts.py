# -*- coding: utf-8 -*-
import os
from os.path import join as opj

import importlib
import glob
import json
import datetime

import gdal

from ost.helpers import raster as ras, helpers as h

def create_stack(filelist, out_stack, logfile,
                 polarisation=None, pattern=None):
    '''

    :param filelist: list of single Files (space separated)
    :param outfile: the stack that is generated
    :return:
    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]

    if pattern:
        graph = opj(rootpath, 'graphs', 'S1_TS', '1_BS_Stacking_HAalpha.xml')
        command = '{} {} -x -q {} -Pfilelist={} -PbandPattern=\'{}.*\' \
               -Poutput={}'.format(gpt_file, graph, 2 * os.cpu_count(),
                                   filelist, pattern, out_stack)
    else:
        graph = opj(rootpath, 'graphs', 'S1_TS', '1_BS_Stacking.xml')
        command = '{} {} -x -q {} -Pfilelist={} -Ppol={} \
               -Poutput={}'.format(gpt_file, graph, 2 * os.cpu_count(),
                                   filelist, polarisation, out_stack)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully created multi-temporal stack')
    else:
        print(' ERROR: Stack creation exited with an error.'
              ' See {} for Snap Error output'.format(logfile))

    return return_code


def mt_speckle_filter(in_stack, out_stack, logfile, speckle_dict):
    '''
    '''

    # get gpt file
    gpt_file = h.gpt_path()

#    # get path to graph
#    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
#    graph = opj(rootpath, 'graphs', 'S1_TS', '2_MT_Speckle.xml')
#
#    command = '{} {} -x -q {} -Pinput={} \
#                   -Poutput={}'.format(gpt_file, graph, 2 * os.cpu_count(),
#                                       in_stack, out_stack)

    print(' INFO: Applying multi-temporal speckle filtering.')
    # contrcut command string
    command = ('{} Multi-Temporal-Speckle-Filter -x -q {}'
                  ' -PestimateENL={}'
                  ' -PanSize={}'
                  ' -PdampingFactor={}'
                  ' -Penl={}'
                  ' -Pfilter={}'
                  ' -PfilterSizeX={}'
                  ' -PfilterSizeY={}'
                  ' -PnumLooksStr={}'
                  ' -PsigmaStr={}'
                  ' -PtargetWindowSizeStr={}'
                  ' -PwindowSize={}'
                  '-t \'{}\' \'{}\''.format(
                      gpt_file, 2 * os.cpu_count(),
                      speckle_dict['estimate ENL'],
                      speckle_dict['pan size'],
                      speckle_dict['damping'],
                      speckle_dict['ENL'],
                      speckle_dict['filter'],
                      speckle_dict['filter x size'],
                      speckle_dict['filter y size'],
                      speckle_dict['num of looks'],
                      speckle_dict['sigma'],
                      speckle_dict['target window size'],
                      speckle_dict['window size'],
                      out_stack, in_stack
                      )
    )
                  
    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully applied multi-temporal speckle filtering')
    else:
        print(' ERROR: Multi-temporal speckle filtering exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code

  
def ard_to_ts(list_of_files, processing_dir, temp_dir, 
              burst, proc_file, product, pol):

    # get the burst directory
    burst_dir = opj(processing_dir, burst)
    
    # check routine if timeseries has already been processed
    check_file = opj(burst_dir, 'Timeseries', '.{}.{}.processed'.format(product, pol))
    if os.path.isfile(check_file):
        print(' INFO: Timeseries of {} for {} in {} polarisation already'
              ' processed'.format(burst, product, pol))
        return
    
    # load ard parameters
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        ard = ard_params['single ARD']
        ard_mt = ard_params['time-series ARD']
    
    # get the db scaling right
    to_db = ard['to db']
    if to_db or product is not 'bs':
        to_db = False
    else:
        to_db = ard_mt['to db']
    
    if ard['apply ls mask']:
        extent = opj(burst_dir, '{}.extent.masked.shp'.format(burst))
    else:
        extent = opj(burst_dir, '{}.extent.shp'.format(burst))
        
    # min max dict for stretching in case of 16 or 8 bit datatype
    mm_dict = {'bs': {'min': -30, 'max': 5},
               'coh': {'min': 0.000001, 'max': 1},
               'Alpha': {'min': 0.000001, 'max': 90},
               'Anisotropy': {'min': 0.000001, 'max': 1},
               'Entropy': {'min': 0.000001, 'max': 1}
              }
    
    stretch = pol if pol in ['Alpha', 'Anisotropy', 'Entropy'] else product
    
    # define out_dir for stacking routine
    out_dir = opj(processing_dir, '{}'.format(burst), 'Timeseries')
    os.makedirs(out_dir, exist_ok=True)

    # create namespaces
    temp_stack = opj(temp_dir, '{}_{}_{}'.format(burst, product, pol))
    out_stack = opj(temp_dir, '{}_{}_{}_mt'.format(burst, product, pol))
    stack_log = opj(out_dir, '{}_{}_{}_stack.err_log'.format(burst, product, pol))

    # run stacking routines
    # convert list of files readable for snap
    list_of_files = '\'{}\''.format(','.join(list_of_files))
  
    if pol in ['Alpha', 'Anisotropy', 'Entropy']:
        print(' INFO: Creating multi-temporal stack of images of burst/track {} for'
              ' the {} band of the polarimetric H-A-Alpha'
              ' decomposition.'.format(burst, pol))
        create_stack(list_of_files, temp_stack, stack_log, pattern=pol)
    else:
        print(' INFO: Creating multi-temporal stack of images of burst/track {} for'
              ' {} product in {} polarization.'.format(burst, product, pol))
        create_stack(list_of_files, temp_stack, stack_log, polarisation=pol)

    # run mt speckle filter
    if ard_mt['remove mt speckle'] is True:
        speckle_log = opj(out_dir, '{}_{}_{}_mt_speckle.err_log'.format(
            burst, product, pol))

        print(' INFO: Applying multi-temporal speckle filter')
        mt_speckle_filter('{}.dim'.format(temp_stack), 
                             out_stack, speckle_log)
        # remove tmp files
        h.delete_dimap(temp_stack)
    else:
        out_stack = temp_stack

    
    if product == 'coh':

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

            outMst = datetime.datetime.strftime(inMst, '%y%m%d')
            outSlv = datetime.datetime.strftime(inSlv, '%y%m%d')
            infile = glob.glob(opj('{}.data'.format(out_stack),
                                   '*{}*{}_{}*img'.format(pol, mst, slv)))[0]
            
            outfile = opj(out_dir, '{}.{}.{}.{}.{}.tif'.format(
                i, outMst, outSlv, product, pol))
            
            ras.mask_by_shape(infile, outfile, extent, 
                              to_db=to_db, 
                              datatype=ard_mt['dtype output'],
                              min_value=mm_dict[stretch]['min'], 
                              max_value=mm_dict[stretch]['max'],
                              ndv=0.0,
                              description=True)
            # add ot a list for subsequent vrt creation
            outfiles.append(outfile)
            i += 1
            
            
    else:
        # get the dates of the files
        dates = [datetime.datetime.strptime(x.split('_')[-1][:-4], '%d%b%Y')
                 for x in glob.glob(opj('{}.data'.format(out_stack), '*img'))]
        # sort them
        dates.sort()
        # write them back to string for following loop
        sortedDates = [datetime.datetime.strftime(ts, "%d%b%Y") 
                       for ts in dates]
    
        i, outfiles = 1, []
        for date in sortedDates:
        
            # restructure date to YYMMDD
            inDate = datetime.datetime.strptime(date, '%d%b%Y')
            outDate = datetime.datetime.strftime(inDate, '%y%m%d')
        
            infile = glob.glob(opj('{}.data'.format(out_stack),
                                   '*{}*{}*img'.format(pol, date)))[0]
        
            # create outfile
            outfile = opj(out_dir, '{}.{}.{}.{}.tif'.format(
                i, outDate, product, pol))
    
            ras.mask_by_shape(infile, outfile, extent,
                              to_db=to_db, 
                              datatype=ard_mt['dtype output'],
                              min_value=mm_dict[stretch]['min'], 
                              max_value=mm_dict[stretch]['max'],
                              ndv=0.0)
            
            # add ot a list for subsequent vrt creation
            outfiles.append(outfile)
            i += 1
            
    for file in outfiles:
        return_code = h.check_out_tiff(file)
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            os.remove(file)
            return return_code
    
    # write file, so we know this ts has been succesfully processed
    if return_code == 0:
        with open(str(check_file), 'w') as file:
            file.write('passed all tests \n')
            
    # build vrt of timeseries
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
    gdal.BuildVRT(opj(out_dir, 'Timeseries.{}.{}.vrt'.format(product, pol)),
                  outfiles,
                  options=vrt_options)

    # remove tmp files
    h.delete_dimap(out_stack)
    