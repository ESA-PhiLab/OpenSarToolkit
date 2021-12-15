#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from retrying import retry

from ost.helpers import helpers as h
from ost.helpers.settings import GPT_FILE, OST_ROOT
from ost.helpers.errors import GPTRuntimeError, NotValidFileError


logger = logging.getLogger(__name__)


@retry(stop_max_attempt_number=3, wait_fixed=1)
def speckle_filter(infile, outfile, logfile, config_dict):
    """Wrapper function around SNAP's Speckle Filter function

    This function takes OST imported Sentinel-1 product and applies
    the Speckle Filter as defind within the config dictionary.

    :param infile:
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    cpus = config_dict["snap_cpu_parallelism"]
    speckle_dict = config_dict["processing"]["single_ARD"]["speckle_filter"]

    logger.debug("Applying speckle filtering.")

    # construct command string
    command = (
        f"{GPT_FILE} Speckle-Filter -x -q {2*cpus} "
        f"-PestimateENL='{speckle_dict['estimate_ENL']}' "
        f"-PanSize='{speckle_dict['pan_size']}' "
        f"-PdampingFactor='{speckle_dict['damping']}' "
        f"-Penl='{speckle_dict['ENL']}' "
        f"-Pfilter='{speckle_dict['filter']}' "
        f"-PfilterSizeX='{speckle_dict['filter_x_size']}' "
        f"-PfilterSizeY='{speckle_dict['filter_y_size']}' "
        f"-PnumLooksStr='{speckle_dict['num_of_looks']}' "
        f"-PsigmaStr='{speckle_dict['sigma']}' "
        f"-PtargetWindowSizeStr=\"{speckle_dict['target_window_size']}\" "
        f"-PwindowSize=\"{speckle_dict['window_size']}\" "
        f"-t '{str(outfile)}' '{str(infile)}' "
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug("Successfully applied speckle filtering.")
    else:
        raise GPTRuntimeError(
            f"Speckle filtering exited with error {return_code}. "
            f"See {logfile} for Snap's error message."
        )

    # do check routine
    return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix(".dim"))
    else:
        raise NotValidFileError(f"Product did not pass file check: {return_code}")


@retry(stop_max_attempt_number=3, wait_fixed=1)
def linear_to_db(infile, outfile, logfile, config_dict):
    """Wrapper function around SNAP's linear to db routine

    This function takes an OST calibrated Sentinel-1 product
    and converts it to dB.

    :param infile:
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    cpus = config_dict["snap_cpu_parallelism"]

    logger.debug("Converting calibrated power image to dB scale.")

    # construct command string
    command = (
        f"{GPT_FILE} LinearToFromdB -x -q {2*cpus} "
        f"-t '{str(outfile)}' {str(infile)}"
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug("Succesfully converted product to dB-scale.")
    else:
        raise GPTRuntimeError(
            f"dB Scaling exited with error {return_code}. "
            f"See {logfile} for Snap's error message."
        )

    # do check routine
    return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix(".dim"))
    else:
        raise NotValidFileError(f"Product did not pass file check: {return_code}")


@retry(stop_max_attempt_number=3, wait_fixed=1)
def terrain_flattening(infile, outfile, logfile, config_dict):
    """Wrapper function to Snap's Terrain Flattening routine

    :param infile:
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    cpus = config_dict["snap_cpu_parallelism"]
    dem_dict = config_dict["processing"]["single_ARD"]["dem"]

    logger.debug("Applying terrain flattening to calibrated product.")

    command = (
        f"{GPT_FILE} Terrain-Flattening -x -q {2*cpus} "
        f"-PdemName='{dem_dict['dem_name']}' "
        f"-PdemResamplingMethod='{dem_dict['dem_resampling']}' "
        f"-PexternalDEMFile='{dem_dict['dem_file']}' "
        f"-PexternalDEMNoDataValue={dem_dict['dem_nodata']} "
        f"-t '{str(outfile)}' '{str(infile)}'"
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug("Succesfully terrain flattened product")
    else:
        raise GPTRuntimeError(
            f"Terrain Flattening exited with error {return_code}. "
            f"See {logfile} for Snap's error message."
        )

    # do check routine
    return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix(".dim"))
    else:
        raise NotValidFileError(f"Product did not pass file check: {return_code}")


@retry(stop_max_attempt_number=3, wait_fixed=1)
def terrain_correction(infile, outfile, logfile, config_dict):
    """Wrapper function around Snap's terrain or ellipsoid correction

    Based on the configuration parameters either the
    Range-Doppler terrain correction or an Ellisoid correction
    is applied for geocoding a calibrated Sentinel-1 product.

    :param infile:
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    ard = config_dict["processing"]["single_ARD"]
    dem_dict = ard["dem"]
    cpus = config_dict["snap_cpu_parallelism"]

    # auto projections of snap
    if 42001 <= dem_dict["out_projection"] <= 97002:
        projection = f"AUTO:{dem_dict['out_projection']}"
    # epsg codes
    elif int(dem_dict["out_projection"]) == 4326:
        projection = "WGS84(DD)"
    else:
        projection = f"EPSG:{dem_dict['out_projection']}"

    #
    logger.debug("Geocoding product.")

    if ard["geocoding"] == "terrain":
        command = (
            f"{GPT_FILE} Terrain-Correction -x -q {2*cpus} "
            f"-PdemName='{dem_dict['dem_name']}' "
            f"-PdemResamplingMethod='{dem_dict['dem_resampling']}' "
            f"-PexternalDEMFile='{dem_dict['dem_file']}' "
            f"-PexternalDEMNoDataValue={dem_dict['dem_nodata']} "
            f"-PexternalDEMApplyEGM="
            f"'{str(dem_dict['egm_correction']).lower()}' "
            f"-PimgResamplingMethod='{dem_dict['image_resampling']}' "
            f"-PpixelSpacingInMeter={ard['resolution']} "
            f"-PalignToStandardGrid=true "
            f"-PmapProjection='{projection}' "
            f"-t '{str(outfile)}' '{str(infile)}' "
        )
    elif ard["geocoding"] == "ellipsoid":
        command = (
            f"{GPT_FILE} Ellipsoid-Correction-RD -x -q {2*cpus} "
            f"-PdemName='{dem_dict['dem_name']}' "
            f"-PdemResamplingMethod='{dem_dict['dem_resampling']}' "
            f"-PexternalDEMFile='{dem_dict['dem_file']}' "
            f"-PexternalDEMNoDataValue={dem_dict['dem_nodata']} "
            f"-PexternalDEMApplyEGM="
            f"'{str(dem_dict['egm_correction']).lower()}' "
            f"-PimgResamplingMethod='{dem_dict['image_resampling']}' "
            f"-PpixelSpacingInMeter={ard['resolution']} "
            f"-PalignToStandardGrid=true "
            f"-PmapProjection='{projection}' "
            f"-t '{str(outfile)}' '{str(infile)}' "
        )
    else:
        raise ValueError("Geocoding method should be either 'terrain' or 'ellipsoid'.")

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug("Succesfully geocoded product")
    else:
        raise GPTRuntimeError(
            f"Geocoding exited with error {return_code}. "
            f"See {logfile} for Snap's error message."
        )

    # do check routine
    return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix(".dim"))
    else:
        raise NotValidFileError(f"Product did not pass file check: {return_code}")


@retry(stop_max_attempt_number=3, wait_fixed=1)
def ls_mask(infile, outfile, logfile, config_dict):
    """Wrapper function of a Snap graph for Layover/Shadow mask creation

    :param infile:
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    ard = config_dict["processing"]["single_ARD"]
    dem_dict = ard["dem"]
    cpus = config_dict["snap_cpu_parallelism"]

    # auto projections of snap
    if 42001 <= dem_dict["out_projection"] <= 97002:
        projection = f"AUTO:{dem_dict['out_projection']}"
    # epsg codes
    elif int(dem_dict["out_projection"]) == 4326:
        projection = "WGS84(DD)"
    else:
        projection = f"EPSG:{dem_dict['out_projection']}"

    logger.debug("Creating the Layover/Shadow mask")

    # get path to workflow xml
    graph = OST_ROOT / "graphs/S1_GRD2ARD/3_LSmap.xml"

    command = (
        f"{GPT_FILE} {graph} -x -q {2 * cpus} "
        f"-Pinput='{str(infile)}' "
        f'-Presol={ard["resolution"]} '
        f'-Pdem=\'{dem_dict["dem_name"]}\' '
        f'-Pdem_file=\'{dem_dict["dem_file"]}\' '
        f'-Pdem_nodata=\'{dem_dict["dem_nodata"]}\' '
        f'-Pdem_resampling=\'{dem_dict["dem_resampling"]}\' '
        f'-Pimage_resampling=\'{dem_dict["image_resampling"]}\' '
        f'-Pegm_correction=\'{str(dem_dict["egm_correction"]).lower()}\' '
        f"-Pprojection='{projection}' "
        f"-Poutput='{str(outfile)}'"
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug("Successfully created a Layover/Shadow mask")
    else:
        raise GPTRuntimeError(
            f"Layover/Shadow mask creation exited with error {return_code}. "
            f"See {logfile} for Snap's error message."
        )

    # do check routine
    return_code = h.check_out_dimap(outfile, test_stats=False)
    if return_code == 0:
        return str(outfile.with_suffix(".dim"))
    else:
        raise NotValidFileError(f"Product did not pass file check: {return_code}")


@retry(stop_max_attempt_number=3, wait_fixed=1)
def create_stack(
    file_list,
    out_stack,
    logfile,
    config_dict,
    polarisation=None,
    pattern=None,
):
    """

    :param file_list:
    :param out_stack:
    :param logfile:
    :param config_dict:
    :param polarisation:
    :param pattern:
    :return:
    """

    # get relevant config parameters
    cpus = config_dict["snap_cpu_parallelism"]

    logger.debug("Creating multi-temporal stack.")

    if pattern:
        graph = OST_ROOT / "graphs/S1_TS/1_BS_Stacking_HAalpha.xml"

        command = (
            f"{GPT_FILE} {graph} -x -q {2*cpus} "
            f"-Pfilelist={file_list} "
            f"-PbandPattern='{pattern}.*' "
            f"-Poutput={out_stack}"
        )

    else:
        graph = OST_ROOT / "graphs/S1_TS/1_BS_Stacking.xml"

        command = (
            f"{GPT_FILE} {graph} -x -q {2*cpus} "
            f"-Pfilelist={file_list} "
            f"-Ppol={polarisation} "
            f"-Poutput={out_stack}"
        )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug("Successfully created multi-temporal stack")
    else:
        raise GPTRuntimeError(
            f"Multi-temporal stack creation exited with error {return_code}. "
            f"See {logfile} for Snap's error message."
        )

    # do check routine
    return_msg = h.check_out_dimap(out_stack)
    if return_msg == 0:
        logger.debug("Product passed validity check.")
    else:
        raise NotValidFileError(f"Product did not pass file check: {return_msg}")


@retry(stop_max_attempt_number=3, wait_fixed=1)
def mt_speckle_filter(in_stack, out_stack, logfile, config_dict):
    """

    :param in_stack:
    :param out_stack:
    :param logfile:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    cpus = config_dict["snap_cpu_parallelism"]
    speckle_dict = config_dict["processing"]["time-series_ARD"]["mt_speckle_filter"]

    # debug message
    logger.debug("Applying multi-temporal speckle filtering.")

    # construct command string
    command = (
        f"{GPT_FILE} Multi-Temporal-Speckle-Filter -x -q {2*cpus} "
        f"-PestimateENL='{speckle_dict['estimate_ENL']}' "
        f"-PanSize='{speckle_dict['pan_size']}' "
        f"-PdampingFactor='{speckle_dict['damping']}' "
        f"-Penl='{speckle_dict['ENL']}' "
        f"-Pfilter='{speckle_dict['filter']}' "
        f"-PfilterSizeX='{speckle_dict['filter_x_size']}' "
        f"-PfilterSizeY='{speckle_dict['filter_y_size']}' "
        f"-PnumLooksStr='{speckle_dict['num_of_looks']}' "
        f"-PsigmaStr='{speckle_dict['sigma']}' "
        f"-PtargetWindowSizeStr=\"{speckle_dict['target_window_size']}\" "
        f"-PwindowSize=\"{speckle_dict['window_size']}\" "
        f"-t '{out_stack}' '{in_stack}' "
    )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug("Successfully applied multi-temporal speckle filtering")
    else:
        raise GPTRuntimeError(
            f"Multi-temporal Spackle Filter exited with error {return_code}. "
            f"See {logfile} for Snap's error message."
        )

    # do check routine
    return_code = h.check_out_dimap(out_stack)
    if return_code == 0:
        return str(out_stack.with_suffix(".dim"))
    else:
        raise NotValidFileError(f"Product did not pass file check: {return_code}")
