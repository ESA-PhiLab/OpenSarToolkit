#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Batch processing routines for Sentinel-1 bursts

This module handles all the batch processing routines involved
in the full workflow from raw Sentinel-1 SLC imagery to
large-scale time-series and timescan mosaics.
"""

import os
import shutil
import json
import itertools
import logging
from pathlib import Path

import pandas as pd
from godale._concurrent import Executor

from ost.helpers import raster as ras, helpers as h
from ost.s1.burst_inventory import prepare_burst_inventory
from ost.s1.burst_to_ard import burst_to_ard
from ost.generic import ard_to_ts, ts_extent, ts_ls_mask, timescan, mosaic

# set up logger
logger = logging.getLogger(__name__)

# ---------------------------------------------------
# Global variable
PRODUCT_LIST = [
    "bs.HH",
    "bs.VV",
    "bs.HV",
    "bs.VH",
    "coh.VV",
    "coh.VH",
    "coh.HH",
    "coh.HV",
    "pol.Entropy",
    "pol.Anisotropy",
    "pol.Alpha",
]


def bursts_to_ards(burst_gdf, config_file):
    """Batch processing from single bursts to ARD format

    This function handles the burst processing based on a OST burst inventory
    file and an OST config file that contains all necessary information
    about the project (e.g. project directory) and processing steps applied
    for the ARD generation based on the JSON ARD-type templates.

    :param burst_gdf: an OST burst inventory
    :type burst_gdf: GeoDataFrame
    :param config_file: (str/Path) path to the project config file
    :param executor_type: executer type for parallel processing with godale,
                          defaults to multiprocessing
    :param max_workers: number of parallel burst processing jobs to start
    :return:
    """

    print("--------------------------------------------------------------")
    logger.info("Processing all single bursts to ARD")
    print("--------------------------------------------------------------")

    logger.info("Preparing the processing pipeline. This may take a moment.")
    proc_inventory = prepare_burst_inventory(burst_gdf, config_file)

    with open(config_file, "r") as file:
        config_dict = json.load(file)
        executor_type = config_dict["executor_type"]
        max_workers = config_dict["max_workers"]

    # we update max_workers in case we have less snap_cpu_parallelism
    # then cpus available
    if max_workers == 1 and config_dict["snap_cpu_parallelism"] < os.cpu_count():
        max_workers = int(os.cpu_count() / config_dict["snap_cpu_parallelism"])

    # now we run with godale, which works also with 1 worker
    out_dict = {
        "burst": [],
        "acq_date": [],
        "out_bs": [],
        "out_ls": [],
        "out_pol": [],
        "out_coh": [],
        "error": [],
    }
    executor = Executor(executor=executor_type, max_workers=max_workers)
    for task in executor.as_completed(
        func=burst_to_ard,
        iterable=proc_inventory.iterrows(),
        fargs=(
            [
                str(config_file),
            ]
        ),
    ):
        burst, date, out_bs, out_ls, out_pol, out_coh, error = task.result()
        out_dict["burst"].append(burst)
        out_dict["acq_date"].append(date)
        out_dict["out_bs"].append(out_bs)
        out_dict["out_ls"].append(out_ls)
        out_dict["out_pol"].append(out_pol)
        out_dict["out_coh"].append(out_coh)
        out_dict["error"].append(error)

    return pd.DataFrame.from_dict(out_dict)


def _create_extents(burst_gdf, config_file):
    """Batch processing for multi-temporal Layover7Shadow mask

    This function handles the organization of the

    :param burst_gdf:
    :param config_file:
    :return:
    """

    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict["processing_dir"])

    # create extent iterable
    iter_list = []
    for burst in burst_gdf.bid.unique():

        # get the burst directory
        burst_dir = processing_dir / burst

        list_of_extents = list(burst_dir.glob("*/*/*bounds.json"))

        # if extent does not already exist, add to iterable
        if not (burst_dir / f"{burst}.min_bounds.json").exists():
            iter_list.append(list_of_extents)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=os.cpu_count()
    )

    out_dict = {"burst": [], "list_of_scenes": [], "extent": []}
    for task in executor.as_completed(
        func=ts_extent.mt_extent,
        iterable=iter_list,
        fargs=(
            [
                str(config_file),
            ]
        ),
    ):
        track, list_of_scenes, extent = task.result()
        out_dict["burst"].append(track)
        out_dict["list_of_scenes"].append(list_of_scenes)
        out_dict["extent"].append(extent)


def _create_extents_old(burst_gdf, config_file):
    """Batch processing for multi-temporal Layover7Shadow mask

    This function handles the organization of the

    :param burst_gdf:
    :param config_file:
    :return:
    """

    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict["processing_dir"])

    # create extent iterable
    iter_list = []
    for burst in burst_gdf.bid.unique():

        # get the burst directory
        burst_dir = processing_dir / burst

        # get common burst extent
        list_of_bursts = list(burst_dir.glob("**/*img"))
        list_of_bursts = [str(x) for x in list_of_bursts if "layover" not in str(x)]

        # if the file does not already exist, add to iterable
        extent = burst_dir / f"{burst}.extent.gpkg"
        if not extent.exists():
            iter_list.append(list_of_bursts)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    for task in executor.as_completed(
        func=ts_extent.mt_extent,
        iterable=iter_list,
        fargs=(
            [
                str(config_file),
            ]
        ),
    ):
        task.result()


def _create_mt_ls_mask(burst_gdf, config_file):
    """Helper function to union the Layover/Shadow masks of a Time-series

    This function creates a

    :param inventory_df:
    :param config_file:
    :return:
    """
    # read config file
    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = config_dict["processing_dir"]

    # create layover
    iter_list = []
    for burst in burst_gdf.bid.unique():  # ***

        # get the burst directory
        burst_dir = Path(processing_dir) / burst

        # get common burst extent
        list_of_masks = list(burst_dir.glob("*/*/*_ls_mask.json"))
        if not (burst_dir / f"{burst}.ls_mask.json").exists():
            iter_list.append(list_of_masks)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=os.cpu_count()
    )

    for task in executor.as_completed(func=ts_ls_mask.mt_layover, iterable=iter_list):
        task.result()


def _create_mt_ls_mask_old(burst_gdf, config_file):
    """Batch processing for multi-temporal Layover/Shadow mask

    This function handles the organization of the

    :param burst_gdf:
    :param config_file:
    :return:
    """

    # read config file
    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = config_dict["processing_dir"]

    # create layover
    iter_list = []
    for burst in burst_gdf.bid.unique():  # ***

        # get the burst directory
        burst_dir = Path(processing_dir) / burst

        # get layover scenes
        list_of_scenes = list(burst_dir.glob("20*/*data*/*img"))
        list_of_layover = [str(x) for x in list_of_scenes if "layover" in str(x)]

        # we need to redefine the namespace of the already created extents
        extent = burst_dir / f"{burst}.extent.gpkg"
        if not extent.exists():
            raise FileNotFoundError(f"Extent file for burst {burst} not found.")

        # layover/shadow mask
        out_ls = burst_dir / f"{burst}.ls_mask.tif"

        # if the file does not already exists, then put into list to process
        if not out_ls.exists():
            iter_list.append(list_of_layover)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    for task in executor.as_completed(
        func=ts_ls_mask.mt_layover,
        iterable=iter_list,
        fargs=(
            [
                str(config_file),
            ]
        ),
    ):
        task.result()


def _create_timeseries(burst_gdf, config_file):
    # we need a
    # dict_of_product_types = {'bs': 'Gamma0', 'coh': 'coh', 'pol': 'pol'}
    list_of_product_types = {
        ("bs", "Gamma0"),
        ("bs", "Sigma0"),
        ("coh", "coh"),
        ("pol", "pol"),
    }
    pols = ["VV", "VH", "HH", "HV", "Alpha", "Entropy", "Anisotropy"]

    # read config file
    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = config_dict["processing_dir"]

    # create iterable
    iter_list = []
    for burst in burst_gdf.bid.unique():

        burst_dir = Path(processing_dir) / burst

        # for pr, pol in itertools.product(dict_of_product_types.items(), pols):
        for pr, pol in itertools.product(list_of_product_types, pols):

            # unpack items
            product, product_name = list(pr)

            # take care of H-A-Alpha naming for file search
            if pol in ["Alpha", "Entropy", "Anisotropy"] and product == "pol":
                list_of_files = sorted(list(burst_dir.glob(f"20*/*data*/*{pol}*img")))
            else:
                # see if there is actually any imagery for this
                # combination of product and polarisation
                list_of_files = sorted(
                    list(burst_dir.glob(f"20*/*data*/*{product_name}*{pol}*img"))
                )

            if len(list_of_files) <= 1:
                continue

            # create list of dims if polarisation is present
            list_of_dims = sorted(
                str(dim) for dim in list(burst_dir.glob(f"20*/*{product}*dim"))
            )

            iter_list.append([list_of_dims, burst, product, pol])

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    out_dict = {
        "burst": [],
        "list_of_dims": [],
        "out_files": [],
        "out_vrt": [],
        "product": [],
        "error": [],
    }
    for task in executor.as_completed(
        func=ard_to_ts.gd_ard_to_ts,
        iterable=iter_list,
        fargs=(
            [
                str(config_file),
            ]
        ),
    ):
        burst, list_of_dims, out_files, out_vrt, product, error = task.result()
        out_dict["burst"].append(burst)
        out_dict["list_of_dims"].append(list_of_dims)
        out_dict["out_files"].append(out_files)
        out_dict["out_vrt"].append(out_vrt)
        out_dict["product"].append(product)
        out_dict["error"].append(error)

    return pd.DataFrame.from_dict(out_dict)


def ards_to_timeseries(burst_gdf, config_file):
    print("--------------------------------------------------------------")
    logger.info("Processing all burst ARDs time-series")
    print("--------------------------------------------------------------")

    # load ard parameters
    with open(config_file, "r") as ard_file:
        ard_params = json.load(ard_file)["processing"]
        ard = ard_params["single_ARD"]
        ard_mt = ard_params["time-series_ARD"]

    # create all extents
    _create_extents(burst_gdf, config_file)

    # update extents in case of ls_mask
    if ard["create_ls_mask"] or ard_mt["apply_ls_mask"]:
        _create_mt_ls_mask(burst_gdf, config_file)

    # finally create time-series
    df = _create_timeseries(burst_gdf, config_file)
    return df


# --------------------
# timescan part
# --------------------
def timeseries_to_timescan(burst_gdf, config_file):
    """Function to create a timescan out of a OST timeseries."""

    print("--------------------------------------------------------------")
    logger.info("Processing all burst ARDs time-series to ARD timescans")
    print("--------------------------------------------------------------")

    # -------------------------------------
    # 1 load project config
    with open(config_file, "r") as ard_file:
        config_dict = json.load(ard_file)
        processing_dir = config_dict["processing_dir"]
        ard = config_dict["processing"]["single_ARD"]
        ard_mt = config_dict["processing"]["time-series_ARD"]
        ard_tscan = config_dict["processing"]["time-scan_ARD"]

    # get the db scaling right
    to_db = True if ard["to_db"] or ard_mt["to_db"] else False

    # get datatype right
    dtype_conversion = True if ard_mt["dtype_output"] != "float32" else False

    # -------------------------------------
    # 2 create iterable for parallel processing
    iter_list, vrt_iter_list = [], []
    for burst in burst_gdf.bid.unique():

        # get relevant directories
        burst_dir = Path(processing_dir) / burst
        timescan_dir = burst_dir / "Timescan"
        timescan_dir.mkdir(parents=True, exist_ok=True)

        for product in PRODUCT_LIST:

            # check if already processed
            if (timescan_dir / f".{product}.processed").exists():
                logger.debug(f"Timescans for burst {burst} already processed.")
                continue

            # get respective timeseries
            timeseries = burst_dir / "Timeseries" / f"Timeseries.{product}.vrt"

            # che if this timsereis exists ( since we go through all products
            if not timeseries.exists():
                continue

            # datelist for harmonics
            scenelist = list(burst_dir.glob(f"Timeseries/*{product}*tif"))
            datelist = [file.name.split(".")[1][:6] for file in sorted(scenelist)]

            # define timescan prefix
            timescan_prefix = timescan_dir / product

            # get rescaling and db right (backscatter vs. coh/pol)
            if "bs." in str(timescan_prefix):
                to_power, rescale = to_db, dtype_conversion
            else:
                to_power, rescale = False, False

            iter_list.append(
                [
                    timeseries,
                    timescan_prefix,
                    ard_tscan["metrics"],
                    rescale,
                    to_power,
                    ard_tscan["remove_outliers"],
                    datelist,
                ]
            )

        vrt_iter_list.append(timescan_dir)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    # run timescan creation
    out_dict = {"burst": [], "prefix": [], "metrics": [], "error": []}
    for task in executor.as_completed(func=timescan.gd_mt_metrics, iterable=iter_list):
        burst, prefix, metrics, error = task.result()
        out_dict["burst"].append(burst)
        out_dict["prefix"].append(prefix)
        out_dict["metrics"].append(metrics)
        out_dict["error"].append(error)

    df = pd.DataFrame.from_dict(out_dict)

    # run vrt creation
    for task in executor.as_completed(
        func=ras.create_tscan_vrt,
        iterable=vrt_iter_list,
        fargs=(
            [
                str(config_file),
            ]
        ),
    ):
        task.result()

    return df


def mosaic_timeseries(burst_inventory, config_file):
    print(" -----------------------------------------------------------------")
    logger.info("Mosaicking time-series layers.")
    print(" -----------------------------------------------------------------")

    # -------------------------------------
    # 1 load project config
    with open(config_file, "r") as ard_file:
        config_dict = json.load(ard_file)
        processing_dir = Path(config_dict["processing_dir"])

    # create output folder
    ts_dir = processing_dir / "Mosaic" / "Timeseries"
    ts_dir.mkdir(parents=True, exist_ok=True)

    temp_mosaic = processing_dir / "Mosaic" / "temp"
    temp_mosaic.mkdir(parents=True, exist_ok=True)
    # -------------------------------------
    # 2 create iterable
    # loop through each product
    iter_list, vrt_iter_list = [], []
    for product in PRODUCT_LIST:

        for track in burst_inventory.Track.unique():

            dates = [
                date[2:]
                for date in sorted(
                    burst_inventory.Date[burst_inventory.Track == track].unique()
                )
            ]

            for i, date in enumerate(dates):

                if "coh" in product:
                    # we do the try, since for the last date
                    # there is no dates[i+1] for coherence
                    try:
                        temp_acq = (
                            temp_mosaic
                            / f"{i}.{date}.{dates[i + 1]}.{track}.{product}.tif"
                        )
                    except IndexError:
                        temp_acq = None
                else:
                    temp_acq = temp_mosaic / f"{i}.{date}.{track}.{product}.tif"

                if temp_acq:
                    iter_list.append([track, date, product, temp_acq, config_file])

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    # run vrt creation
    for task in executor.as_completed(
        func=mosaic.gd_mosaic_slc_acquisition, iterable=iter_list
    ):
        task.result()

    # mosaic the acquisitions
    iter_list, vrt_iter_list = [], []
    for product in PRODUCT_LIST:

        outfiles = []
        for i in range(len(dates)):

            list_of_files = list(temp_mosaic.glob(f"{i}.*{product}.tif"))

            if not list_of_files:
                continue

            datelist = []
            for file in list_of_files:
                if "coh" in product:
                    datelist.append(
                        f"{file.name.split('.')[2]}_{file.name.split('.')[1]}"
                    )
                else:
                    datelist.append(file.name.split(".")[1])

            # get start and endate of mosaic
            start, end = sorted(datelist)[0], sorted(datelist)[-1]
            list_of_files = " ".join([str(file) for file in list_of_files])

            # create namespace for output file
            if start == end:
                outfile = ts_dir / f"{i + 1:02d}.{start}.{product}.tif"

                # with the above operation, the list automatically
                # turns into string, so we can call directly list_of_files
                shutil.move(list_of_files, outfile)
                outfiles.append(outfile)
                continue

            else:
                outfile = ts_dir / f"{i + 1:02d}.{start}-{end}.{product}.tif"

            # create namespace for check_file
            check_file = outfile.parent / f".{outfile.name[:-4]}.processed"

            if check_file.exists():
                logger.info(f"Mosaic layer {outfile} already processed.")
                continue

            # append to list of outfile for vrt creation
            outfiles.append(outfile)
            iter_list.append([list_of_files, outfile, config_file])

        vrt_iter_list.append([ts_dir, product, outfiles])

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    # run mosaicking
    for task in executor.as_completed(func=mosaic.gd_mosaic, iterable=iter_list):
        task.result()

    # run mosaicking vrts
    for task in executor.as_completed(
        func=mosaic.create_timeseries_mosaic_vrt, iterable=vrt_iter_list
    ):
        task.result()

    # remove temp folder
    h.remove_folder_content(temp_mosaic)


def mosaic_timescan(burst_inventory, config_file):
    """

    :param burst_inventory:
    :param config_file:
    :return:
    """

    print(" -----------------------------------------------------------------")
    logger.info("Mosaicking time-scan layers.")
    print(" -----------------------------------------------------------------")

    # -------------------------------------
    # 1 load project config
    with open(config_file, "r") as ard_file:
        config_dict = json.load(ard_file)
        processing_dir = Path(config_dict["processing_dir"])
        metrics = config_dict["processing"]["time-scan_ARD"]["metrics"]

    if "harmonics" in metrics:
        metrics.remove("harmonics")
        metrics.extend(["amplitude", "phase", "residuals", "model_mean"])

    if "percentiles" in metrics:
        metrics.remove("percentiles")
        metrics.extend(["p95", "p5"])

    # create output folder
    ts_dir = processing_dir / "Mosaic" / "Timescan"
    ts_dir.mkdir(parents=True, exist_ok=True)

    temp_mosaic = processing_dir / "Mosaic" / "temp"
    temp_mosaic.mkdir(parents=True, exist_ok=True)
    # -------------------------------------
    # 2 create iterable
    # loop through each product
    iter_list = []
    for product, metric in itertools.product(PRODUCT_LIST, metrics):

        for track in burst_inventory.Track.unique():

            filelist = list(
                processing_dir.glob(
                    f"[A,D]{track}_IW*/Timescan/*{product}.{metric}.tif"
                )
            )

            if not len(filelist) >= 1:
                continue

            temp_acq = temp_mosaic / f"{track}.{product}.{metric}.tif"

            if temp_acq:
                iter_list.append([track, metric, product, temp_acq, config_file])

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    # run vrt creation
    for task in executor.as_completed(
        func=mosaic.gd_mosaic_slc_acquisition, iterable=iter_list
    ):
        task.result()

    iter_list = []
    for product, metric in itertools.product(PRODUCT_LIST, metrics):

        list_of_files = list(temp_mosaic.glob(f"*{product}.{metric}.tif"))

        if not list_of_files:
            continue

        # turn to OTB readable format
        list_of_files = " ".join([str(file) for file in list_of_files])

        # create namespace for outfile
        outfile = ts_dir / f"{product}.{metric}.tif"
        check_file = outfile.parent / f".{outfile.name[:-4]}.processed"

        if check_file.exists():
            logger.info(f"Mosaic layer {outfile.name} already processed.")
            continue

        logger.info(f"Mosaicking layer {outfile.name}.")

        iter_list.append([list_of_files, outfile, config_file])

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    # run mosaicking
    for task in executor.as_completed(func=mosaic.gd_mosaic, iterable=iter_list):
        task.result()

    ras.create_tscan_vrt(ts_dir, config_file)

    # remove temp folder
    h.remove_folder_content(temp_mosaic)


def mosaic_timescan_old(config_file):
    print(" -----------------------------------------------------------------")
    logger.info("Mosaicking time-scan layers.")
    print(" -----------------------------------------------------------------")

    with open(config_file, "r") as ard_file:
        config_dict = json.load(ard_file)
        processing_dir = Path(config_dict["processing_dir"])
        metrics = config_dict["processing"]["time-scan_ARD"]["metrics"]

    if "harmonics" in metrics:
        metrics.remove("harmonics")
        metrics.extend(["amplitude", "phase", "residuals"])

    if "percentiles" in metrics:
        metrics.remove("percentiles")
        metrics.extend(["p95", "p5"])

    tscan_dir = processing_dir / "Mosaic" / "Timescan"
    tscan_dir.mkdir(parents=True, exist_ok=True)

    iter_list = []
    for product, metric in itertools.product(PRODUCT_LIST, metrics):

        filelist = list(processing_dir.glob(f"*/Timescan/*{product}.{metric}.tif"))

        if not len(filelist) >= 1:
            continue

        filelist = " ".join([str(file) for file in filelist])

        outfile = tscan_dir / f"{product}.{metric}.tif"
        check_file = outfile.parent / f".{outfile.name[:-4]}.processed"

        if check_file.exists():
            logger.info(f"Mosaic layer {outfile.name} already processed.")
            continue

        logger.info(f"Mosaicking layer {outfile.name}.")

        iter_list.append([filelist, outfile, config_file])

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    # run mosaicking
    for task in executor.as_completed(func=mosaic.gd_mosaic, iterable=iter_list):
        task.result()

    ras.create_tscan_vrt(tscan_dir, config_file)
