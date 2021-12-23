#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import numpy as np
import logging
from retrying import retry
from pathlib import Path
from osgeo import gdal

from ost.helpers import helpers as h
from ost.helpers.errors import GPTRuntimeError, NotValidFileError
from ost.helpers.settings import GPT_FILE, OST_ROOT


logger = logging.getLogger(__name__)


@retry(stop_max_attempt_number=3, wait_fixed=1)
def grd_frame_import(infile, outfile, logfile, config_dict):
    """A wrapper of SNAP import of a single Sentinel-1 GRD product

    This function takes an original Sentinel-1 scene (either zip or
    SAFE format), updates the orbit information (does not fail if not
    available), removes the thermal noise and stores it as a SNAP
    compatible BEAM-Dimap format.

    :param infile: Sentinel-1 GRD product in zip or SAFE format
    :type infile: str/Path
    :param outfile:
    :type outfile: str/Path
    :param logfile:
    :param config_dict: an OST configuration dictionary
    :type config_dict: dict
    :return:
    """

    if isinstance(infile, str):
        infile = Path(infile)

    # get relevant config parameters
    ard = config_dict["processing"]["single_ARD"]
    polars = ard["polarisation"].replace(" ", "")
    cpus = config_dict["snap_cpu_parallelism"]
    subset = config_dict["subset"]

    try:
        aoi = config_dict["aoi"]
    except KeyError:
        aoi = ""

    logger.debug(
        f"Importing {infile.name} by applying precise orbit file and "
        f"removing thermal noise"
    )

    # get path to graph
    if subset:
        graph = OST_ROOT / "graphs" / "S1_GRD2ARD" / "1_AO_TNR_SUB.xml"
        # construct command
        command = (
            f"{GPT_FILE} {graph} -x -q {2 * cpus} "
            f"-Pinput='{str(infile)}' "
            f"-Pregion='{aoi}' "
            f"-Ppolarisation={polars} "
            f"-Poutput='{str(outfile)}'"
        )

    else:
        # construct path ot graph
        graph = OST_ROOT / "graphs" / "S1_GRD2ARD" / "1_AO_TNR.xml"
        # construct command
        command = (
            f"{GPT_FILE} {graph} -x -q {2 * cpus} "
            f"-Pinput='{str(infile)}' "
            f"-Ppolarisation={polars} "
            f"-Poutput='{str(outfile)}'"
        )

    # run command
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug("Succesfully imported GRD product")
    else:
        # read logfile
        raise GPTRuntimeError(
            f"GRD frame import exited with error {return_code}. "
            f"See {logfile} for Snap's error output."
        )

    # do check routine
    return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix(".dim"))
    else:
        raise NotValidFileError(f"Product did not pass file check: {return_code}")


@retry(stop_max_attempt_number=3, wait_fixed=1)
def slice_assembly(filelist, outfile, logfile, config_dict):
    """Wrapper function around SNAP's slice assembly routine

    :param filelist: a string of a space separated list of OST imported
                     Sentinel-1 GRD product frames to be assembled
    :type filelist: str
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    """A wrapper of SNAP's slice assembly routine

    This function assembles consecutive frames acquired at the same date.
    Can be either GRD or SLC products

    Args:
        filelist (str): a string of a space separated list of OST imported
                        Sentinel-1 product slices to be assembled
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
    """

    # get relevant config parameters
    ard = config_dict["processing"]["single_ARD"]
    polars = ard["polarisation"].replace(" ", "")
    cpus = config_dict["snap_cpu_parallelism"]

    logger.debug("Assembling consecutive frames:")

    # construct command
    command = (
        f"{GPT_FILE} SliceAssembly -x -q {2*cpus} "
        f"-PselectedPolarisations={polars} "
        f"-t '{str(outfile)}' {filelist}"
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug("Succesfully assembled products")
    else:
        raise GPTRuntimeError(
            f"ERROR: Slice Assembly exited with error {return_code}. "
            f"See {logfile} for Snap Error output"
        )

    # do check routine
    return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix(".dim"))
    else:
        raise NotValidFileError(f"Product did not pass file check: {return_code}")


@retry(stop_max_attempt_number=3, wait_fixed=1)
def grd_subset_georegion(infile, outfile, logfile, config_dict):
    """Wrapper function around SNAP's subset routine

    This function takes an OST imported/slice assembled frame and
    subsets it according to the coordinates given in the region

    :param infile:
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    cpus = config_dict["snap_cpu_parallelism"]

    try:
        aoi = config_dict["aoi"]
    except KeyError:
        aoi = ""

    logger.debug("Subsetting imported imagery.")

    # extract window from scene
    command = (
        f"{GPT_FILE} Subset -x -q {2*cpus} "
        f"-PcopyMetadata=true "
        f"-PgeoRegion='{aoi}' "
        f"-Ssource='{str(infile)}' "
        f"-t '{str(outfile)}'"
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug("Succesfully subsetted product.")
    else:
        raise GPTRuntimeError(
            f"Subsetting exited with error {return_code}. "
            f"See {logfile} for Snap's error message."
        )

    # do check routine
    return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix(".dim"))
    else:
        raise NotValidFileError(f"Product did not pass file check: {return_code}")


@retry(stop_max_attempt_number=3, wait_fixed=1)
def grd_remove_border(infile):
    """OST function to remove GRD border noise from Sentinel-1 data


    This is a custom routine to remove GRD border noise
    from Sentinel-1 GRD products. It works on the original intensity
    images.

    NOTE: For the common dimap format, the infile needs to be the
    ENVI style file inside the *data folder.

    The routine checks the outer 3000 columns for its mean value.
    If the mean value is below 100, all values will be set to 0,
    otherwise the routine will continue fpr another 150 columns setting
    the value to 0. All further columns towards the inner image are
    considered valid.

    :param infile:
    :return:
    """

    logger.debug(f"Removing the GRD Border Noise for {infile.name}.")
    currtime = time.time()

    # read raster file and get number of columns adn rows
    raster = gdal.Open(str(infile), gdal.GA_Update)
    cols = raster.RasterXSize
    rows = raster.RasterYSize

    # create 3000xrows array for the left part of the image
    array_left = np.array(raster.GetRasterBand(1).ReadAsArray(0, 0, 3000, rows))

    cols_left = 3000
    for x in range(3000):
        # if mean value of column is below 100, fil with 0s
        if np.mean(array_left[:, x]) <= 100:
            array_left[:, x].fill(0)
        else:
            z = x + 200
            if z > 3000:
                z = 3000
            for cols_left in range(x, z, 1):
                array_left[:, cols_left].fill(0)

            break

    # write array_left to disk
    logger.debug(f"Number of colums set to 0 on the left side: {cols_left}.")

    raster.GetRasterBand(1).WriteArray(array_left[:, :+cols_left], 0, 0)
    del array_left

    # create 2d array for the right part of the image (3000 columns and rows)
    cols_last = cols - 3000
    array_right = np.array(
        raster.GetRasterBand(1).ReadAsArray(cols_last, 0, 3000, rows)
    )

    # loop through the array_right columns in opposite direction
    cols_right = 3000
    for x in range(2999, 0, -1):

        if np.mean(array_right[:, x]) <= 100:
            array_right[:, x].fill(0)
        else:
            z = x - 200
            if z < 0:
                z = 0
            for cols_right in range(x, z, -1):
                array_right[:, cols_right].fill(0)

            break

    col_right_start = cols - 3000 + cols_right
    logger.debug(f"Number of columns set to 0 on the right side: {3000 - cols_right}.")
    logger.debug(f"Amount of columns kept: {col_right_start}.")
    raster.GetRasterBand(1).WriteArray(array_right[:, cols_right:], col_right_start, 0)

    logger.debug(h.timer(currtime))


@retry(stop_max_attempt_number=3, wait_fixed=1)
def calibration(infile, outfile, logfile, config_dict):
    """

    :param infile:
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    product_type = config_dict["processing"]["single_ARD"]["product_type"]
    cpus = config_dict["snap_cpu_parallelism"]

    # transform calibration parameter to snap readable
    sigma0, beta0, gamma0 = "false", "false", "false"

    if product_type == "GTC-sigma0":
        sigma0 = "true"
    elif product_type == "GTC-gamma0":
        gamma0 = "true"
    elif product_type == "RTC-gamma0":
        beta0 = "true"
    else:
        raise TypeError("Wrong product type selected.")

    logger.debug(f"Calibrating the product to {product_type}.")

    # construct command string
    command = (
        f"{GPT_FILE} Calibration -x -q {2*cpus} "
        f" -PoutputBetaBand='{beta0}' "
        f" -PoutputGammaBand='{gamma0}' "
        f" -PoutputSigmaBand='{sigma0}' "
        f" -t '{str(outfile)}' '{str(infile)}'"
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug(f"Calibration to {product_type} successful.")
    else:
        raise GPTRuntimeError(
            f"Calibration exited with error {return_code}. "
            f"See {logfile} for Snap's error message."
        )

    # do check routine
    return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix(".dim"))
    else:
        raise NotValidFileError(f"Product did not pass file check: {return_code}")


@retry(stop_max_attempt_number=3, wait_fixed=1)
def multi_look(infile, outfile, logfile, config_dict):
    """

    :param infile:
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    ard = config_dict["processing"]["single_ARD"]
    cpus = config_dict["snap_cpu_parallelism"]
    ml_factor = int(int(ard["resolution"]) / 10)

    logger.debug(
        "Multi-looking the image with {az_looks} looks in "
        "azimuth and {rg_looks} looks in range."
    )

    # construct command string
    command = (
        f"{GPT_FILE} Multilook -x -q {2*cpus} "
        f"-PnAzLooks={ml_factor} "
        f"-PnRgLooks={ml_factor} "
        f"-t '{str(outfile)}' {str(infile)}"
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug("Succesfully multi-looked product.")
    else:
        raise GPTRuntimeError(
            f" ERROR: Multi-look exited with error {return_code}. "
            f"See {logfile} for Snap's error message."
        )

    # do check routine
    return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix(".dim"))
    else:
        raise NotValidFileError(f"Product did not pass file check: {return_code}")
