# -*- coding: utf-8 -*-

import json
import logging
from pathlib import Path
from datetime import datetime as dt
from tempfile import TemporaryDirectory

from retrying import retry
from osgeo import gdal

from ost.generic.common_wrappers import create_stack, mt_speckle_filter
from ost.helpers import raster as ras, helpers as h
from ost.helpers.errors import GPTRuntimeError, NotValidFileError

logger = logging.getLogger(__name__)

SNAP_DATEFORMAT = "%d%b%Y"


@retry(stop_max_attempt_number=3, wait_fixed=1)
def ard_to_ts(list_of_files, burst, product, pol, config_file):

    # -------------------------------------------
    # 1 unpack list of args
    # convert list of files readable for snap
    list_of_files = f"'{','.join(str(x) for x in list_of_files)}'"

    # -------------------------------------------
    # 2 read config file
    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict["processing_dir"])
        ard = config_dict["processing"]["single_ARD"]
        ard_mt = config_dict["processing"]["time-series_ARD"]

    # -------------------------------------------
    # 3 get namespace of directories and check if already processed
    # get the burst directory
    burst_dir = processing_dir / burst

    # get timeseries directory and create if non existent
    out_dir = burst_dir / "Timeseries"
    Path.mkdir(out_dir, parents=True, exist_ok=True)

    # in case some processing has been done before, check if already processed
    check_file = out_dir / f".{product}.{pol}.processed"
    if Path.exists(check_file):
        logger.info(
            f"Timeseries of {burst} for {product} in {pol} "
            f"polarisation already processed."
        )

        out_files = "already_processed"
        out_vrt = "already_processed"

        return (burst, list_of_files, out_files, out_vrt, f"{product}.{pol}", None)

    # -------------------------------------------
    # 4 adjust processing parameters according to config
    # get the db scaling right
    to_db = ard["to_db"]
    if to_db or product != "bs":
        to_db = False
        logger.debug(f"Not converting to dB for {product}")
    else:
        to_db = ard_mt["to_db"]
        logger.debug(f"Converting to dB for {product}")

    if ard_mt["apply_ls_mask"]:
        extent = burst_dir / f"{burst}.valid.json"
    else:
        extent = burst_dir / f"{burst}.min_bounds.json"

    # -------------------------------------------
    # 5 SNAP processing
    with TemporaryDirectory(prefix=f"{config_dict['temp_dir']}/") as temp:

        # turn to Path object
        temp = Path(temp)

        # create namespaces
        temp_stack = temp / f"{burst}_{product}_{pol}"
        out_stack = temp / f"{burst}_{product}_{pol}_mt"
        stack_log = out_dir / f"{burst}_{product}_{pol}_stack.err_log"

        # run stacking routine
        if pol in ["Alpha", "Anisotropy", "Entropy"]:
            logger.info(
                f"Creating multi-temporal stack of images of burst/track "
                f"{burst} for the {pol} band of the polarimetric "
                f"H-A-Alpha decomposition."
            )
            try:
                create_stack(
                    list_of_files, temp_stack, stack_log, config_dict, pattern=pol
                )
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, None, None, None, error
        else:
            logger.info(
                f"Creating multi-temporal stack of images of burst/track "
                f"{burst} for {product} product in {pol} polarization."
            )
            try:
                create_stack(
                    list_of_files, temp_stack, stack_log, config_dict, polarisation=pol
                )
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, None, None, None, error

        # run mt speckle filter
        if ard_mt["remove_mt_speckle"] is True:

            speckle_log = out_dir / f"{burst}_{product}_{pol}_mt_speckle.err_log"

            logger.debug("Applying multi-temporal speckle filter")
            try:
                mt_speckle_filter(
                    temp_stack.with_suffix(".dim"), out_stack, speckle_log, config_dict
                )
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, None, None, None, error

            # remove tmp files
            h.delete_dimap(temp_stack)
        else:
            out_stack = temp_stack

        # -----------------------------------------------
        # 6 Conversion to GeoTiff

        # min max dict for stretching in case of 16 or 8 bit datatype
        mm_dict = {
            "bs": {"min": -30, "max": 5},
            "coh": {"min": 0.000001, "max": 1},
            "Alpha": {"min": 0.000001, "max": 90},
            "Anisotropy": {"min": 0.000001, "max": 1},
            "Entropy": {"min": 0.000001, "max": 1},
        }
        stretch = pol if pol in ["Alpha", "Anisotropy", "Entropy"] else product

        if product == "coh":

            # get slave and master dates from file names and sort them
            mst_dates = sorted(
                [
                    dt.strptime(file.name.split("_")[3].split(".")[0], SNAP_DATEFORMAT)
                    for file in list(out_stack.with_suffix(".data").glob("*.img"))
                ]
            )

            slv_dates = sorted(
                [
                    dt.strptime(file.name.split("_")[4].split(".")[0], SNAP_DATEFORMAT)
                    for file in list(out_stack.with_suffix(".data").glob("*.img"))
                ]
            )

            # write them back to string for following loop
            mst_dates = [dt.strftime(ts, SNAP_DATEFORMAT) for ts in mst_dates]
            slv_dates = [dt.strftime(ts, SNAP_DATEFORMAT) for ts in slv_dates]

            out_files = []
            for i, (mst, slv) in enumerate(zip(mst_dates, slv_dates)):

                # re-construct namespace for input file
                infile = list(
                    out_stack.with_suffix(".data").glob(f"*{pol}*{mst}_{slv}*img")
                )[0]

                # rename dates to YYYYMMDD format
                mst = dt.strftime(dt.strptime(mst, SNAP_DATEFORMAT), "%y%m%d")
                slv = dt.strftime(dt.strptime(slv, SNAP_DATEFORMAT), "%y%m%d")

                # create namespace for output file with renamed dates
                outfile = out_dir / f"{i+1:02d}.{mst}.{slv}.{product}.{pol}.tif"

                # fill internal values if any
                # with rasterio.open(str(infile), 'r') as src:
                #    meta = src.meta.copy()
                #    filled = ras.fill_internal_nans(src.read())

                # with rasterio.open(str(infile), 'w', **meta) as dest:
                #    dest.write(filled)

                # print('filled')
                # produce final outputfile,
                # including dtype conversion and ls mask
                ras.mask_by_shape(
                    infile,
                    outfile,
                    extent,
                    to_db=to_db,
                    datatype=ard_mt["dtype_output"],
                    min_value=mm_dict[stretch]["min"],
                    max_value=mm_dict[stretch]["max"],
                    ndv=0.0,
                    description=True,
                )

                # add ot a list for subsequent vrt creation
                out_files.append(str(outfile))

        else:
            # get the dates of the files
            dates = sorted(
                [
                    dt.strptime(file.name.split("_")[-1][:-4], SNAP_DATEFORMAT)
                    for file in list(out_stack.with_suffix(".data").glob("*.img"))
                ]
            )

            # write them back to string for following loop
            dates = [dt.strftime(ts, "%d%b%Y") for ts in dates]

            out_files = []
            for i, date in enumerate(dates):

                # re-construct namespace for input file
                infile = list(
                    out_stack.with_suffix(".data").glob(f"*{pol}*{date}*img")
                )[0]

                # restructure date to YYMMDD
                date = dt.strftime(dt.strptime(date, SNAP_DATEFORMAT), "%y%m%d")

                # create namespace for output file
                outfile = out_dir / f"{i+1:02d}.{date}.{product}.{pol}.tif"

                # fill internal nodata
                # if ard['image_type'] == 'SLC':
                # with rasterio.open(str(infile), 'r') as src:
                #    meta = src.meta.copy()
                # filled = ras.fill_internal_nans(src.read())

                # with rasterio.open(str(infile), 'w', **meta) as dest:
                #    dest.write(filled)
                # print('filledbs')
                # run conversion routine
                ras.mask_by_shape(
                    infile,
                    outfile,
                    extent,
                    to_db=to_db,
                    datatype=ard_mt["dtype_output"],
                    min_value=mm_dict[stretch]["min"],
                    max_value=mm_dict[stretch]["max"],
                    ndv=0.0,
                )

                # add ot a list for subsequent vrt creation
                out_files.append(str(outfile))

    # -----------------------------------------------
    # 7 Filechecks
    for file in out_files:
        return_code = h.check_out_tiff(file)
        if return_code != 0:

            for file_ in out_files:
                Path(file_).unlink()
                if Path(f"{file}.xml").exists():
                    Path(f"{file}.xml").unlink()

            return (burst, list_of_files, None, None, f"{product}.{pol}", return_code)

    # write file, so we know this ts has been successfully processed
    with open(str(check_file), "w") as file:
        file.write("passed all tests \n")

    # -----------------------------------------------
    # 8 Create vrts
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
    out_vrt = str(out_dir / f"Timeseries.{product}.{pol}.vrt")
    gdal.BuildVRT(out_vrt, out_files, options=vrt_options)

    return burst, list_of_files, out_files, out_vrt, f"{product}.{pol}", None


def gd_ard_to_ts(list_of_args, config_file):

    list_of_files, burst, product, pol = list_of_args
    return ard_to_ts(list_of_files, burst, product, pol, config_file)
