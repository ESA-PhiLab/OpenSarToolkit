# -*- coding: utf-8 -*-
# import stdlib modules
import os
from os.path import join as opj

from datetime import datetime
from datetime import timedelta
from calendar import isleap

import rasterio
import numpy as np
from scipy import stats

from ost.helpers import raster as ras
from ost.helpers import helpers as h


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


def date_as_float(date):
    size_of_day = 1. / 366.
    size_of_second = size_of_day / (24. * 60. * 60.)
    days_from_jan1 = date - datetime(date.year, 1, 1)
    if not isleap(date.year) and days_from_jan1.days >= 31+28:
        days_from_jan1 += timedelta(1)
    return date.year + days_from_jan1.days * size_of_day + days_from_jan1.seconds * size_of_second


def difference_in_years(start, end):
    return date_as_float(end) - date_as_float(start)


def deseasonalize(stack):
    
    percentiles = np.percentile(stack, 95, axis=[1,2])
    deseasoned = np.subtract(percentiles[:,np.newaxis], stack.reshape(stack.shape[0], -1))
    return deseasoned.reshape(stack.shape)


def _zvalue_from_index(arr, ind):
    """private helper function to work around the limitation of np.choose() by employing np.take()
    arr has to be a 3D array
    ind has to be a 2D array containing values for z-indicies to take from arr
    See: http://stackoverflow.com/a/32091712/4169585
    This is faster and more memory efficient than using the ogrid based solution with fancy indexing.
    """
    # get number of columns and rows
    _, nC, nR = arr.shape

    # get linear indices and extract elements with np.take()
    idx = nC * nR * ind + np.arange(nC*nR).reshape((nC,nR))
    return np.take(arr, idx)


def nan_percentile(arr, q):
    # taken from: https://krstn.eu/np.nanpercentile()-there-has-to-be-a-faster-way/
    # valid (non NaN) observations along the first axis
    valid_obs = np.sum(np.isfinite(arr), axis=0)
    # replace NaN with maximum
    max_val = np.nanmax(arr)
    arr[np.isnan(arr)] = max_val
    # sort - former NaNs will move to the end
    arr = np.sort(arr, axis=0)

    # loop over requested quantiles
    if type(q) is list:
        qs = []
        qs.extend(q)
    else:
        qs = [q]
    if len(qs) < 2:
        quant_arr = np.zeros(shape=(arr.shape[1], arr.shape[2]))
    else:
        quant_arr = np.zeros(shape=(len(qs), arr.shape[1], arr.shape[2]))

    result = []
    for i in range(len(qs)):
        quant = qs[i]
        # desired position as well as floor and ceiling of it
        k_arr = (valid_obs - 1) * (quant / 100.0)
        f_arr = np.floor(k_arr).astype(np.int32)
        c_arr = np.ceil(k_arr).astype(np.int32)
        fc_equal_k_mask = f_arr == c_arr

        # linear interpolation (like numpy percentile) takes the fractional part of desired position
        floor_val = _zvalue_from_index(arr=arr, ind=f_arr) * (c_arr - k_arr)
        ceil_val = _zvalue_from_index(arr=arr, ind=c_arr) * (k_arr - f_arr)

        quant_arr = floor_val + ceil_val
        quant_arr[fc_equal_k_mask] = _zvalue_from_index(arr=arr, ind=k_arr.astype(np.int32))[fc_equal_k_mask]  # if floor == ceiling take floor value

        result.append(quant_arr)

    return result


def mt_metrics(stack, out_prefix, metrics, rescale_to_datatype=False,
               to_power=False, outlier_removal=False, datelist=None):
    if type(rescale_to_datatype) == str:
        if rescale_to_datatype == 'True':
            rescale_to_datatype = True
        elif rescale_to_datatype == 'False':
            rescale_to_datatype = False
    if type(to_power) == str:
        if to_power == 'True':
            to_power = True
        elif to_power == 'False':
            to_power = False
    if type(outlier_removal) == str:
        if outlier_removal == 'True':
            outlier_removal = True
        elif outlier_removal == 'False':
            outlier_removal = False
    if type(metrics) == str:
        metrics = metrics.replace("'", '').strip('][').split(', ')
    if type(datelist) == str:
        datelist = datelist.replace("'", '').strip('][').split(', ')

    # from datetime import datetime
    with rasterio.open(stack) as src:

        harmonics = False
        if 'harmonics' in metrics:
            print(' INFO: Calculating harmonics')
            if not datelist:
                print(' WARNING: Harmonics need the datelist. Harmonics will not be calculated')
            else:
                harmonics = True
                metrics.remove('harmonics')
                metrics.extend(['amplitude', 'phase', 'residuals'])
        
        if 'percentiles' in metrics:
            metrics.remove('percentiles')
            metrics.extend(['p95', 'p5'])
            
        # get metadata
        meta = src.profile

        # update driver and reduced band count
        meta.update({'driver': 'GTiff'})
        meta.update({'count': 1})

        # write all different output files into a dictionary
        metric_dict = {}
        for metric in metrics:
            filename = '{}.{}.tif'.format(out_prefix, metric)
            metric_dict[metric] = rasterio.open(
                filename, 'w', **meta)

        # scaling factors in case we have to rescale to integer
        minimums = {'avg': -30, 'max': -30, 'min': -30,
                    'std': 0.00001, 'cov': 0.00001}
        maximums = {'avg': 5, 'max': 5, 'min': 5, 'std': 15, 'cov': 1}

        
        if harmonics:
            # construct independent variables
            dates, sines, cosines = [], [], []
            two_pi = np.multiply(2, np.pi)
            for date in sorted(datelist):
                
                delta = difference_in_years(datetime.strptime('700101',"%y%m%d"), datetime.strptime(date,"%y%m%d"))
                dates.append(delta)
                sines.append(np.sin(np.multiply(two_pi, delta - 0.5)))
                cosines.append(np.cos(np.multiply(two_pi, delta - 0.5)))
            
            X = np.array([dates, cosines, sines])
    
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
            arr['p95'], arr['p5'] = (np.nan_to_num(nan_percentile(stack, [95, 5]))
                                     if 'p95' in metrics else (False, False))
            arr['median'] = (np.nan_to_num(np.nanmedian(stack, axis=0))
                          if 'median' in metrics else False)
            arr['avg'] = (np.nan_to_num(np.nanmean(stack, axis=0))
                          if 'avg' in metrics else False)
            arr['max'] = (np.nan_to_num(np.nanmax(stack, axis=0))
                          if 'max' in metrics else False)
            arr['min'] = (np.nan_to_num(np.nanmin(stack, axis=0))
                          if 'min' in metrics else False)
            arr['std'] = (np.nan_to_num(np.nanstd(stack, axis=0))
                          if 'std' in metrics else False)
            arr['cov'] = (np.nan_to_num(stats.variation(stack, axis=0,
                                                        nan_policy='omit'))
                          if 'cov' in metrics else False)
            
            if harmonics:
                
                stack_size = (stack.shape[1], stack.shape[2])
                if to_power is True:
                    y = ras.convert_to_db(stack).reshape(stack.shape[0], -1)
                else:
                    y = stack.reshape(stack.shape[0], -1)
                    
                x, residuals, _, _ = np.linalg.lstsq(X.T, y)
                arr['amplitude'] = np.hypot(x[1], x[2]).reshape(stack_size)
                arr['phase'] = np.arctan2(x[2], x[1]).reshape(stack_size)
                arr['residuals'] = np.sqrt(np.divide(residuals, stack.shape[0])).reshape(stack_size)
                
                
            # the metrics to be re-turned to dB, in case to_power is True
            metrics_to_convert = ['avg', 'min', 'max', 'p95', 'p5', 'median']

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
                metric_dict[metric].update_tags(1, 
                    BAND_NAME='{}_{}'.format(os.path.basename(out_prefix), metric))
                metric_dict[metric].set_band_description(1, 
                    '{}_{}'.format(os.path.basename(out_prefix), metric))

    # close the output files
    for metric in metrics:
        # close rio opening
        metric_dict[metric].close()
        # construct filename
        filename = '{}.{}.tif'.format(out_prefix, metric)
        return_code = h.check_out_tiff(filename)
        if return_code != 0:
            # remove all files and return
            for metric in metrics:
                filename = '{}.{}.tif'.format(out_prefix, metric)
                os.remove(filename)
            
            return return_code
        
    if return_code == 0:
        dirname = os.path.dirname(out_prefix)
        check_file = opj(dirname, '.{}.processed'.format(os.path.basename(out_prefix)))
        with open(str(check_file), 'w') as file:
            file.write('passed all tests \n')
    
        
        