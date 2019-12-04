import os
from os.path import join as opj
import imp
import sys
import glob
import time
import logging
from datetime import datetime

import gdal
import rasterio
import numpy as np
from scipy import stats

from ost.helpers import helpers as h, raster as ras, vector as vec

logger = logging.getLogger(__name__)


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
    rootpath = imp.find_module('ost')[1]

    logger.debug("INFO: Creating multi-temporal stack of images")
    if pattern:
        graph = opj(rootpath, 'graphs', 'S1_TS', '1_BS_Stacking_HAalpha.xml')
        command = '{} {} -x -q {} -Pfilelist={} -PbandPattern=\'{}.*\'\
               -Poutput={}'.format(gpt_file, graph, 2 * os.cpu_count(),
                                   filelist, pattern, out_stack)
    else:
        graph = opj(rootpath, 'graphs', 'S1_TS', '1_BS_Stacking.xml')
        command = '{} {} -x -q {} -Pfilelist={} -Ppol={} \
               -Poutput={}'.format(gpt_file, graph, 2 * os.cpu_count(),
                                   filelist, polarisation, out_stack)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug('INFO: Succesfully created multi-temporal stack')
    else:
        logger.debug('ERROR: Stack creation exited with an error.'
              'See {} for Snap Error output'.format(logfile))
        sys.exit(201)

    return return_code


def mt_speckle_filter(in_stack, out_stack, logfile):
    '''
    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootpath = imp.find_module('ost')[1]
    graph = opj(rootpath, 'graphs', 'S1_TS', '2_MT_Speckle.xml')

    logger.debug("INFO: Applying the multi-temporal speckle-filtering")
    command = '{} {} -x -q {} -Pinput={} \
                   -Poutput={}'.format(gpt_file, graph, 2 * os.cpu_count(),
                                       in_stack, out_stack)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug('INFO: Succesfully applied multi-temporal speckle filtering')
    else:
        logger.debug('ERROR: Multi-temporal speckle filtering exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(202)

    return return_code


def mt_layover(filelist, outfile, temp_dir, extent):
    '''
    This function is usally used in the time-series workflow of OST. A list
    of the filepaths layover/shadow masks

    :param filelist - list of files
    :param out_dir - directory where the output file will be stored
    :return path to the multi-temporal layover/shadow mask file generated
    '''

    # get the start time for Info on processing time
    start = time.time()
    # create path to out file
    ls_layer = opj(temp_dir, os.path.basename(outfile))

    # create a vrt-stack out of
    logger.debug('INFO: Creating common Layover/Shadow Mask')
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
    gdal.BuildVRT(opj(temp_dir, 'ls.vrt'), filelist, options=vrt_options)

    with rasterio.open(opj(temp_dir, 'ls.vrt')) as src:

        # get metadata
        meta = src.meta
        # update driver and reduced band count
        meta.update(driver='GTiff', count=1, dtype='uint8')

        # create outfiles
        with rasterio.open(ls_layer, 'w', **meta) as out_min:

            # loop through blocks
            for _, window in src.block_windows(1):

                # read array with all bands
                stack = src.read(range(1, src.count + 1), window=window)

                # get stats
                arr_max = np.nanmax(stack, axis=0)
                arr = arr_max / arr_max

                out_min.write(np.uint8(arr), window=window, indexes=1)

    ras.mask_by_shape(ls_layer, outfile, extent, to_db=False,
                      datatype='uint8', rescale=False, ndv=0)
    # os.remove(ls_layer)
    h.timer(start)

    return outfile


def mt_extent(list_of_scenes, out_file, temp_dir, buffer=None):

    out_dir = os.path.dirname(out_file)
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)

    # build vrt stack from all scenes
    gdal.BuildVRT(opj(out_dir, 'extent.vrt'),
                  list_of_scenes,
                  options=vrt_options)

    logger.debug('INFO: Creating shapefile of common extent.')
    start = time.time()

    outline_file = opj(temp_dir, os.path.basename(out_file))
    ras.outline(opj(out_dir, 'extent.vrt'), outline_file, 0, False)

    vec.exterior(outline_file, out_file, buffer)
    h.delete_shapefile(outline_file)

    os.remove(opj(out_dir, 'extent.vrt'))
    h.timer(start)

    return out_file


def remove_outliers(arrayin, stddev=3, z_threshold=None):

    if z_threshold:
        z_score = np.abs(stats.zscore(arrayin))
        array_out = np.ma.MaskedArray(
            arrayin,
            mask=z_score > z_threshold)
    else:

        # calculate percentiles
        perc95 = np.percentile(arrayin, 95, axis=0)
        perc5 = np.percentile(arrayin, 5, axis=0)

        # we mask out the percetile outliers for std dev calculation
        masked_array = np.ma.MaskedArray(
            arrayin,
            mask=np.logical_or(
                arrayin > perc95,
                arrayin < perc5
                )
            )

        # we calculate new std and mean
        masked_std = np.std(masked_array, axis=0)
        masked_mean = np.mean(masked_array, axis=0)

        # we mask based on mean +- 3 * stddev
        array_out = np.ma.MaskedArray(
            arrayin,
            mask=np.logical_or(
                arrayin > masked_mean + masked_std * stddev,
                arrayin < masked_mean - masked_std * stddev,
                )
            )

    return array_out


def mt_metrics(stack, out_prefix, metrics, rescale_to_datatype=False,
               to_power=False, outlier_removal=False):

    with rasterio.open(stack) as src:

        # get metadata
        meta = src.profile

        # update driver and reduced band count
        meta.update({'driver': 'GTiff'})
        meta.update({'count': 1})

        # write all different output files into a dictionary
        metric_dict = {}
        for metric in metrics:
            metric_dict[metric] = rasterio.open(
                out_prefix + '.'+ metric + '.tif', 'w', **meta)

        # scaling factors in case we have to rescale to integer
        minimums = {'avg': -30, 'max': -30, 'min': -30,
                    'std': 0.00001, 'cov': 0.00001}
        maximums = {'avg': 5, 'max': 5, 'min': 5, 'std': 15, 'cov': 1}

        # loop through blocks
        for _, window in src.block_windows(1):

            # read array with all bands
            stack = src.read(range(1, src.count + 1), window=window)

            if rescale_to_datatype is True and meta['dtype'] != 'float32':
                stack = ras.rescale_to_float(stack, meta['dtype'])

            # transform to power
            if to_power is True:
                stack = ras.convert_to_power(stack)

            # outlier removal (only applies if there are more than 5 bands)
            if outlier_removal is True and src.count >= 5:
                stack = remove_outliers(stack)

            # get stats
            arr = {}
            arr['avg'] = (np.nan_to_num(np.nanmean(stack, axis=0))
                          if 'avg'in metrics else False)
            arr['max'] = (np.nan_to_num(np.nanmax(stack, axis=0))
                          if 'max'in metrics else False)
            arr['min'] = (np.nan_to_num(np.nanmin(stack, axis=0))
                          if 'min'in metrics else False)
            arr['std'] = (np.nan_to_num(np.nanstd(stack, axis=0))
                          if 'std'in metrics else False)
            arr['cov'] = (np.nan_to_num(stats.variation(stack, axis=0,
                                                        nan_policy='omit'))
                          if 'cov'in metrics else False)

            # the metrics to be re-turned to dB, in case to_power is True
            metrics_to_convert = ['avg', 'min', 'max']

            # do the back conversions and write to disk loop
            for metric in metrics:

                if to_power is True and metric in metrics_to_convert:
                    arr[metric] = ras.convert_to_db(arr[metric])

                if rescale_to_datatype is True and meta['dtype'] != 'float32':
                    arr[metric] = ras.scale_to_int(arr[metric], meta['dtype'],
                                                   minimums[metric],
                                                   maximums[metric])

                # write to dest
                metric_dict[metric].write(
                    np.float32(arr[metric]), window=window, indexes=1)

    # close the output files
    for metric in metrics:
        metric_dict[metric].close()


def create_datelist(path_to_timeseries):
    '''Create a text file of acquisition dates within your time-series

    Args:
        path_to_timeseries (str): path to an OST time-series directory
    '''

    files = glob.glob('{}/*VV*tif'.format(path_to_timeseries))
    dates = sorted([os.path.basename(file).split('.')[1] for file in files])

    with open('{}/datelist.txt'.format(path_to_timeseries), 'w') as file:
        for date in dates:
            file.write(str(datetime.strftime(datetime.strptime(
                date, '%y%m%d'), '%Y-%m-%d')) + '\n')


def create_ts_animation(ts_dir, temp_dir, outfile, shrink_factor):

    for file in sorted(glob.glob(opj(ts_dir, '*VV.tif'))):

        file_index = os.path.basename(file).split('.')[0]
        date = os.path.basename(file).split('.')[1]
        file_vv = file
        file_vh = glob.glob(opj(ts_dir, '{}.*VH.tif'.format(file_index)))[0]

        out_temp = opj(temp_dir, '{}.jpg'.format(date))

        with rasterio.open(file_vv) as vv_pol:

            # get metadata
            out_meta = vv_pol.meta.copy()

            # !!!assure that dimensions match ####
            new_height = int(vv_pol.height/shrink_factor)
            new_width = int(vv_pol.width/shrink_factor)
            out_shape = (vv_pol.count, new_height, new_width)

            out_meta.update(height=new_height, width=new_width)

            # create empty array
            arr = np.zeros((int(out_meta['height']),
                            int(out_meta['width']),
                            int(3)))
            # read vv array
            arr[:, :, 0] = vv_pol.read(out_shape=out_shape, resampling=5)

        with rasterio.open(file_vh) as vh_pol:
            # read vh array
            arr[:, :, 1] = vh_pol.read(out_shape=out_shape, resampling=5)

        # create ratio
        arr[:, :, 2] = np.subtract(arr[:, :, 0], arr[:, :, 1])

        # rescale_to_datatype to uint8
        arr[:, :, 0] = ras.scale_to_int(arr[:, :, 0], -20., 0., 'uint8')
        arr[:, :, 1] = ras.scale_to_int(arr[:, :, 1], -25., -5., 'uint8')
        arr[:, :, 2] = ras.scale_to_int(arr[:, :, 2], 1., 15., 'uint8')

        # update outfile's metadata
        out_meta.update({'driver': 'JPEG',
                         'dtype': 'uint8',
                         'count': 3})

        # transpose array to gdal format
        arr = np.transpose(arr, [2, 0, 1])

        # write array to disk
        with rasterio.open(out_temp, 'w', **out_meta) as out:
            out.write(arr.astype('uint8'))

        # add date
        label_height = np.floor(np.divide(int(out_meta['height']), 15))
        cmd = 'convert -background \'#0008\' -fill white -gravity center \
              -size {}x{} caption:\"{}\" {} +swap -gravity north \
              -composite {}'.format(out_meta['width'], label_height,
                                    date, out_temp, out_temp)
        os.system(cmd)

    # create gif
    lst_of_files = ' '.join(sorted(glob.glob(opj(temp_dir, '*jpg'))))
    cmd = 'convert -delay 200 -loop 20 {} {}'.format(lst_of_files, outfile)
    os.system(cmd)

    for file in glob.glob(opj(temp_dir, '*jpg')):
        os.remove(file)
