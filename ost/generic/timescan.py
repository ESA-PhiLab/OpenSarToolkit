# -*- coding: utf-8 -*-
# import stdlib modules

import logging
import warnings
from pathlib import Path
from datetime import datetime
from datetime import timedelta
from calendar import isleap

import rasterio
import numpy as np
from scipy import stats
from retrying import retry

from ost.helpers import raster as ras
from ost.helpers import helpers as h

logger = logging.getLogger(__name__)


def remove_outliers(arrayin, stddev=2, z_threshold=None):

    warnings.filterwarnings("ignore", "invalid value", RuntimeWarning)

    if z_threshold:
        z_score = np.abs(stats.zscore(arrayin))
        array_out = np.ma.MaskedArray(arrayin, mask=z_score > z_threshold)
    else:

        # calculate percentiles
        perc95 = np.percentile(arrayin, 95, axis=0)
        perc5 = np.percentile(arrayin, 5, axis=0)

        # we mask out the percentile outliers for std dev calculation
        masked_array = np.ma.MaskedArray(
            arrayin, mask=np.logical_or(arrayin > perc95, arrayin < perc5)
        )

        # we calculate new std and mean
        masked_std = np.std(masked_array, axis=0)
        masked_mean = np.mean(masked_array, axis=0)

        # we mask based on mean +- x * stddev
        array_out = np.ma.MaskedArray(
            arrayin,
            mask=np.logical_or(
                arrayin > masked_mean + masked_std * stddev,
                arrayin < masked_mean - masked_std * stddev,
            ),
        )

    return array_out


def date_as_float(date):
    size_of_day = 1.0 / 366.0
    size_of_second = size_of_day / (24.0 * 60.0 * 60.0)
    days_from_jan1 = date - datetime(date.year, 1, 1)

    if not isleap(date.year) and days_from_jan1.days >= 31 + 28:
        days_from_jan1 += timedelta(1)

    return (
        date.year
        + days_from_jan1.days * size_of_day
        + days_from_jan1.seconds * size_of_second
    )


def difference_in_years(start, end):
    return date_as_float(end) - date_as_float(start)


def deseasonalize(stack):
    percentiles = np.percentile(stack, 95, axis=[1, 2])
    deseasoned = np.subtract(
        percentiles[:, np.newaxis], stack.reshape(stack.shape[0], -1)
    )
    return deseasoned.reshape(stack.shape)


def _zvalue_from_index(arr, ind):
    """work around the limitation of np.choose() by employing np.take()

    arr has to be a 3D array
    ind has to be a 2D array containing values for z-indicies to take from arr
    See: http://stackoverflow.com/a/32091712/4169585

    This is faster and more memory efficient than using
    the ogrid based solution with fancy indexing.
    """

    # get number of columns and rows
    _, cols, rows = arr.shape

    # get linear indices and extract elements with np.take()
    idx = cols * rows * ind + np.arange(cols * rows).reshape((cols, rows))
    return np.take(arr, idx)


def nan_percentile(arr, q):
    # taken from:
    # https://krstn.eu/np.nanpercentile()-there-has-to-be-a-faster-way/

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

    result = []
    for i in range(len(qs)):
        quant = qs[i]

        # desired position as well as floor and ceiling of it
        k_arr = (valid_obs - 1) * (quant / 100.0)
        f_arr = np.floor(k_arr).astype(np.int32)
        c_arr = np.ceil(k_arr).astype(np.int32)
        fc_equal_k_mask = f_arr == c_arr

        # linear interpolation (like numpy percentile)
        # takes the fractional part of desired position
        floor_val = _zvalue_from_index(arr=arr, ind=f_arr) * (c_arr - k_arr)
        ceil_val = _zvalue_from_index(arr=arr, ind=c_arr) * (k_arr - f_arr)

        quant_arr = floor_val + ceil_val
        quant_arr[fc_equal_k_mask] = _zvalue_from_index(
            arr=arr, ind=k_arr.astype(np.int32)
        )[fc_equal_k_mask]

        result.append(quant_arr)

    return result


@retry(stop_max_attempt_number=3, wait_fixed=1)
def mt_metrics(
    stack, out_prefix, metrics, rescale_to_datatype, to_power, outlier_removal, datelist
):
    """

    :param stack:
    :param out_prefix:
    :param metrics:
    :param rescale_to_datatype:
    :param to_power:
    :param outlier_removal:
    :param datelist:
    :return:
    """

    logger.info(
        f"Creating timescan layers ({metrics}) of track/burst "
        f"{out_prefix.parent.parent.name} for {out_prefix.name}"
    )

    warnings.filterwarnings("ignore", r"All-NaN (slice|axis) encountered")
    warnings.filterwarnings("ignore", r"Mean of empty slice")
    warnings.filterwarnings("ignore", r"Degrees of freedom", RuntimeWarning)

    if "harmonics" in metrics:
        logger.info("Calculating harmonics")
        if not datelist:
            raise RuntimeWarning(
                "Harmonics need the datelist. " "Harmonics will not be calculated"
            )
        else:
            metrics.remove("harmonics")
            metrics.extend(["amplitude", "phase", "residuals", "trend", "model_mean"])

    if "percentiles" in metrics:
        metrics.remove("percentiles")
        metrics.extend(["p95", "p5"])

    with rasterio.open(stack) as src:

        # get metadata
        meta = src.profile

        # update driver and reduced band count
        meta.update({"driver": "GTiff"})
        meta.update({"count": 1})

        # write all different output files into a dictionary
        metric_dict = {}
        for metric in metrics:
            filename = f"{out_prefix}.{metric}.tif"
            metric_dict[metric] = rasterio.open(filename, "w", **meta)

        # scaling factors in case we have to rescale to integer
        minimums = {
            "avg": int(-30),
            "max": int(-30),
            "min": int(-30),
            "median": -30,
            "p5": -30,
            "p95": -30,
            "std": 0.00001,
            "cov": 0.00001,
            "amplitude": -5,
            "phase": -np.pi,
            "residuals": -10,
            "trend": -5,
            "model_mean": -30,
        }

        maximums = {
            "avg": 5,
            "max": 5,
            "min": 5,
            "median": 5,
            "p5": 5,
            "p95": 5,
            "std": 0.2,
            "cov": 1,
            "amplitude": 5,
            "phase": np.pi,
            "residuals": 10,
            "trend": 5,
            "model_mean": 5,
        }

        if "amplitude" in metrics:
            # construct independent variables
            dates, sines, cosines, intercept = [], [], [], []
            two_pi = np.multiply(2, np.pi)

            for date in sorted(datelist):
                delta = difference_in_years(
                    datetime.strptime("700101", "%y%m%d"),
                    datetime.strptime(date, "%y%m%d"),
                )
                dates.append(delta)
                sines.append(np.sin(np.multiply(two_pi, delta)))
                cosines.append(np.cos(np.multiply(two_pi, delta)))
                intercept.append(1)

            x_array = np.array([dates, cosines, sines, intercept])

        # loop through blocks
        for _, window in src.block_windows(1):

            # read array with all bands
            stack = src.read(range(1, src.count + 1), window=window)

            # rescale to float
            if rescale_to_datatype is True and meta["dtype"] != "float32":
                stack = ras.rescale_to_float(stack, meta["dtype"])

            # transform to power
            if to_power is True:
                stack = np.power(10, np.divide(stack, 10))

            # outlier removal (only applies if there are more than 5 bands)
            if outlier_removal is True and src.count >= 5:
                stack = remove_outliers(stack)

            # get stats
            arr = {
                "p95": (
                    nan_percentile(stack, [95, 5])
                    if "p95" in metrics
                    else (False, False)
                )[0],
                "p5": (
                    nan_percentile(stack, [95, 5])
                    if "p95" in metrics
                    else (False, False)
                )[1],
                "median": (
                    np.nanmedian(stack, axis=0) if "median" in metrics else False
                ),
                "avg": (np.nanmean(stack, axis=0) if "avg" in metrics else False),
                "max": (np.nanmax(stack, axis=0) if "max" in metrics else False),
                "min": (np.nanmin(stack, axis=0) if "min" in metrics else False),
                "std": (np.nanstd(stack, axis=0) if "std" in metrics else False),
                # 'cov': (stats.variation(stack, axis=0, nan_policy='omit')
                "cov": (
                    np.divide(np.nanstd(stack, axis=0), np.nanmean(stack, axis=0))
                    if "cov" in metrics
                    else False
                ),
            }

            if "amplitude" in metrics:

                stack_size = (stack.shape[1], stack.shape[2])
                if to_power is True:
                    y = ras.convert_to_db(stack).reshape(stack.shape[0], -1)
                else:
                    y = stack.reshape(stack.shape[0], -1)

                x, residuals, _, _ = np.linalg.lstsq(x_array.T, y, rcond=-1)
                arr["amplitude"] = np.hypot(x[1], x[2]).reshape(stack_size)
                arr["phase"] = np.arctan2(x[2], x[1]).reshape(stack_size)
                arr["trend"] = x[0].reshape(stack_size)
                arr["model_mean"] = x[3].reshape(stack_size)
                arr["residuals"] = np.sqrt(
                    np.divide(residuals, stack.shape[0])
                ).reshape(stack_size)

            # the metrics to be re-turned to dB, in case to_power is True
            metrics_to_convert = ["avg", "min", "max", "p95", "p5", "median"]

            # do the back conversions and write to disk loop
            for metric in metrics:

                if to_power is True and metric in metrics_to_convert:
                    arr[metric] = ras.convert_to_db(arr[metric])

                if (rescale_to_datatype is True and meta["dtype"] != "float32") or (
                    metric in ["cov", "phase"] and meta["dtype"] != "float32"
                ):
                    arr[metric] = ras.scale_to_int(
                        arr[metric], minimums[metric], maximums[metric], meta["dtype"]
                    )

                # write to dest
                metric_dict[metric].write(
                    np.nan_to_num(arr[metric]).astype(meta["dtype"]),
                    window=window,
                    indexes=1,
                )
                metric_dict[metric].update_tags(
                    1, BAND_NAME=f"{Path(out_prefix).name}_{metric}"
                )
                metric_dict[metric].set_band_description(
                    1, f"{Path(out_prefix).name}_{metric}"
                )

    # close the output files
    for metric in metrics:
        # close rio opening
        metric_dict[metric].close()

        # construct filename
        filename = f"{str(out_prefix)}.{metric}.tif"
        return_code = h.check_out_tiff(filename)

        if return_code != 0:

            for metric_ in metrics:
                # remove all files and return
                filename = f"{str(out_prefix)}.{metric_}.tif"
                Path(filename).unlink()
                if Path(f"{filename}.xml").exists():
                    Path(f"{filename}.xml").unlink()

            return None, None, None, return_code

    # write out that it's been processed
    dirname = out_prefix.parent
    check_file = dirname / f".{out_prefix.name}.processed"
    with open(str(check_file), "w") as file:
        file.write("passed all tests \n")

    target = out_prefix.parent.parent.name
    return target, out_prefix.name, metrics, None


def gd_mt_metrics(list_of_args):
    stack, out_prefix, metrics, rescale_to_datatype = list_of_args[:4]
    to_power, outlier_removal, datelist = list_of_args[4:]
    return mt_metrics(
        stack,
        out_prefix,
        metrics,
        rescale_to_datatype,
        to_power,
        outlier_removal,
        datelist,
    )
