#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import rasterio

from ost.helpers import helpers as h
from ost.s1 import slc_wrappers as slc
from ost.generic import common_wrappers as common
from ost.helpers import raster as ras
from ost.helpers.errors import GPTRuntimeError, NotValidFileError

logger = logging.getLogger(__name__)


def create_polarimetric_layers(import_file, out_dir, burst_prefix, config_dict):
    """Pipeline for Dual-polarimetric decomposition

    :param import_file:
    :param out_dir:
    :param burst_prefix:
    :param config_dict:
    :return:
    """

    # temp dir for intermediate files
    with TemporaryDirectory(prefix=f"{config_dict['temp_dir']}/") as temp:
        temp = Path(temp)
        # -------------------------------------------------------
        # 1 Polarimetric Decomposition

        # create namespace for temporary decomposed product
        out_haa = temp / f"{burst_prefix}_h"

        # create namespace for decompose log
        haa_log = out_dir / f"{burst_prefix}_haa.err_log"

        # run polarimetric decomposition
        try:
            slc.ha_alpha(import_file, out_haa, haa_log, config_dict)
        except (GPTRuntimeError, NotValidFileError) as error:
            logger.info(error)
            return None, error
        # -------------------------------------------------------
        # 2 Geocoding

        # create namespace for temporary geocoded product
        out_htc = temp / f"{burst_prefix}_pol"

        # create namespace for geocoding log
        haa_tc_log = out_dir / f"{burst_prefix}_haa_tc.err_log"

        # run geocoding
        try:
            common.terrain_correction(
                out_haa.with_suffix(".dim"), out_htc, haa_tc_log, config_dict
            )
        except (GPTRuntimeError, NotValidFileError) as error:
            logger.info(error)
            return None, error

        # set nans to 0 (issue from SNAP for polarimetric layers)
        for infile in list(out_htc.with_suffix(".data").glob("*.img")):

            with rasterio.open(str(infile), "r") as src:
                meta = src.meta.copy()
                array = src.read()
                array[np.isnan(array)] = 0

            with rasterio.open(str(infile), "w", **meta) as dest:
                dest.write(array)

        # ---------------------------------------------------------------------
        # 5 Create an outline
        ras.image_bounds(out_htc.with_suffix(".data"))

        # move to final destination
        ard = config_dict["processing"]["single_ARD"]
        h.move_dimap(out_htc, out_dir / f"{burst_prefix}_pol", ard["to_tif"])

        # write out check file for tracking that it is processed
        with (out_dir / ".pol.processed").open("w+") as file:
            file.write("passed all tests \n")

        dim_file = out_dir / f"{burst_prefix}_pol.dim"

        return (str(dim_file), None)


def create_backscatter_layers(import_file, out_dir, burst_prefix, config_dict):
    """Pipeline for backscatter processing

    :param import_file:
    :param out_dir:
    :param burst_prefix:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    ard = config_dict["processing"]["single_ARD"]

    # temp dir for intermediate files
    with TemporaryDirectory(prefix=f"{config_dict['temp_dir']}/") as temp:

        temp = Path(temp)
        # ---------------------------------------------------------------------
        # 1 Calibration

        # create namespace for temporary calibrated product
        out_cal = temp / f"{burst_prefix}_cal"

        # create namespace for calibrate log
        cal_log = out_dir / f"{burst_prefix}_cal.err_log"

        # run calibration on imported scene
        try:
            slc.calibration(import_file, out_cal, cal_log, config_dict)
        except (GPTRuntimeError, NotValidFileError) as error:
            logger.info(error)
            return None, None, error

        # ---------------------------------------------------------------------
        # 2 Speckle filtering
        if ard["remove_speckle"]:

            # create namespace for temporary speckle filtered product
            speckle_import = temp / f"{burst_prefix}_speckle_import"

            # create namespace for speckle filter log
            speckle_log = out_dir / f"{burst_prefix}_speckle.err_log"

            # run speckle filter on calibrated input
            try:
                common.speckle_filter(
                    out_cal.with_suffix(".dim"),
                    speckle_import,
                    speckle_log,
                    config_dict,
                )
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, error

            # remove input
            h.delete_dimap(out_cal)

            # reset master_import for following routine
            out_cal = speckle_import

        # ---------------------------------------------------------------------
        # 3 dB scaling
        if ard["to_db"]:

            # create namespace for temporary db scaled product
            out_db = temp / f"{burst_prefix}_cal_db"

            # create namespace for db scaling log
            db_log = out_dir / f"{burst_prefix}_cal_db.err_log"

            # run db scaling on calibrated/speckle filtered input
            try:
                common.linear_to_db(
                    out_cal.with_suffix(".dim"), out_db, db_log, config_dict
                )
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, error

            # remove tmp files
            h.delete_dimap(out_cal)

            # set out_cal to out_db for further processing
            out_cal = out_db

        # ---------------------------------------------------------------------
        # 4 Geocoding

        # create namespace for temporary geocoded product
        out_tc = temp / f"{burst_prefix}_bs"

        # create namespace for geocoding log
        tc_log = out_dir / f"{burst_prefix}_bs_tc.err_log"

        # run terrain correction on calibrated/speckle filtered/db  input
        try:
            common.terrain_correction(
                out_cal.with_suffix(".dim"), out_tc, tc_log, config_dict
            )
        except (GPTRuntimeError, NotValidFileError) as error:
            logger.info(error)
            return None, None, error

        # ---------------------------------------------------------------------
        # 5 Create an outline
        ras.image_bounds(out_tc.with_suffix(".data"))

        # ---------------------------------------------------------------------
        # 6 Layover/Shadow mask
        out_ls = None  # set to none for final return statement
        if ard["create_ls_mask"] is True:

            # create namespace for temporary ls mask product
            ls_mask = temp / f"{burst_prefix}_ls_mask"

            # create namespace for ls mask log
            logfile = out_dir / f"{burst_prefix}.ls_mask.errLog"

            # run ls mask routine
            try:
                common.ls_mask(
                    out_cal.with_suffix(".dim"), ls_mask, logfile, config_dict
                )
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, error

            # polygonize
            ls_raster = list(ls_mask.with_suffix(".data").glob("*img"))[0]
            ras.polygonize_ls(ls_raster, ls_mask.with_suffix(".json"))

            out_ls = (
                out_tc.with_suffix(".data").joinpath(ls_mask.name).with_suffix(".json")
            )

            # move to product folder
            ls_mask.with_suffix(".json").rename(out_ls)

        # move final backscatter product to actual output directory
        h.move_dimap(out_tc, out_dir / f"{burst_prefix}_bs", ard["to_tif"])

        # write out check file for tracking that it is processed
        with (out_dir / ".bs.processed").open("w+") as file:
            file.write("passed all tests \n")

        return (
            str((out_dir / f"{burst_prefix}_bs").with_suffix(".dim")),
            str(out_ls),
            None,
        )


def create_coherence_layers(
    master_import, slave_import, out_dir, master_prefix, config_dict
):
    """Pipeline for Dual-polarimetric decomposition

    :param master_import:
    :param slave_import:
    :param out_dir:
    :param master_prefix:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    ard = config_dict["processing"]["single_ARD"]

    with TemporaryDirectory(prefix=f"{config_dict['temp_dir']}/") as temp:

        temp = Path(temp)
        # ---------------------------------------------------------------
        # 1 Co-registration
        # create namespace for temporary co-registered stack
        out_coreg = temp / f"{master_prefix}_coreg"

        # create namespace for co-registration log
        coreg_log = out_dir / f"{master_prefix}_coreg.err_log"

        # run co-registration
        try:
            slc.coreg(master_import, slave_import, out_coreg, coreg_log, config_dict)
        except (GPTRuntimeError, NotValidFileError) as error:
            logger.info(error)
            h.delete_dimap(out_coreg)

            # remove imports
            h.delete_dimap(master_import)
            return None, error

        # remove imports
        h.delete_dimap(master_import)
        h.delete_dimap(slave_import)

        # ---------------------------------------------------------------
        # 2 Coherence calculation

        # create namespace for temporary coherence product
        out_coh = temp / f"{master_prefix}_coherence"

        # create namespace for coherence log
        coh_log = out_dir / f"{master_prefix}_coh.err_log"

        # run coherence estimation
        try:
            slc.coherence(out_coreg.with_suffix(".dim"), out_coh, coh_log, config_dict)
        except (GPTRuntimeError, NotValidFileError) as error:
            logger.info(error)
            return None, error

        # remove coreg tmp files
        h.delete_dimap(out_coreg)

        # ---------------------------------------------------------------
        # 3 Geocoding

        # create namespace for temporary geocoded roduct
        out_tc = temp / f"{master_prefix}_coh"

        # create namespace for geocoded log
        tc_log = out_dir / f"{master_prefix}_coh_tc.err_log"

        # run geocoding
        try:
            common.terrain_correction(
                out_coh.with_suffix(".dim"), out_tc, tc_log, config_dict
            )
        except (GPTRuntimeError, NotValidFileError) as error:
            logger.info(error)
            return None, error

        # ---------------------------------------------------------------
        # 4 Checks and Clean-up

        # remove tmp files
        h.delete_dimap(out_coh)

        # ---------------------------------------------------------------------
        # 5 Create an outline
        ras.image_bounds(out_tc.with_suffix(".data"))

        # move to final destination
        h.move_dimap(out_tc, out_dir / f"{master_prefix}_coh", ard["to_tif"])

        # write out check file for tracking that it is processed
        with (out_dir / ".coh.processed").open("w+") as file:
            file.write("passed all tests \n")

        dim_file = out_dir / f"{master_prefix}_coh.dim"
        return (str(dim_file), None)


def burst_to_ard(burst, config_file):

    # this is a godale thing
    if isinstance(burst, tuple):
        i, burst = burst

    # load relevant config parameters
    with open(config_file, "r") as file:
        config_dict = json.load(file)
        ard = config_dict["processing"]["single_ARD"]
        temp_dir = Path(config_dict["temp_dir"])

    # creation of out_directory
    out_dir = Path(burst.out_directory)
    out_dir.mkdir(parents=True, exist_ok=True)

    # existence of processed files
    pol_file = (out_dir / ".pol.processed").exists()
    bs_file = (out_dir / ".bs.processed").exists()
    coh_file = (out_dir / ".coh.processed").exists()

    # set all return values initially to None
    out_bs, out_ls, out_pol, out_coh = None, None, None, None

    # check if we need to produce coherence
    if ard["coherence"]:
        # we check if there is actually a slave file or
        # if it is the end of the time-series
        coherence = True if burst.slave_file else False
    else:
        coherence = False

    # get info on master from GeoSeries
    master_prefix = burst["master_prefix"]
    master_file = burst["file_location"]
    master_burst_nr = burst["BurstNr"]
    swath = burst["SwathID"]

    logger.info(f"Processing burst {burst.bid} acquired at {burst.Date}")
    # check if somethings already processed
    if (
        (ard["H-A-Alpha"] and not pol_file)
        or (ard["backscatter"] and not bs_file)
        or (coherence and not coh_file)
    ):

        # ---------------------------------------------------------------------
        # 1 Master import
        # create namespace for master import
        master_import = temp_dir / f"{master_prefix}_import"

        if not master_import.with_suffix(".dim").exists():

            # create namespace for log file
            import_log = out_dir / f"{master_prefix}_import.err_log"

            # run import
            try:
                slc.burst_import(
                    master_file,
                    master_import,
                    import_log,
                    swath,
                    master_burst_nr,
                    config_dict,
                )
            except (GPTRuntimeError, NotValidFileError) as error:
                if master_import.with_suffix(".dim").exists():
                    h.delete_dimap(master_import)

                logger.info(error)
                return burst.bid, burst.Date, None, None, None, None, error

        # ---------------------------------------------------------------------
        # 2 Product Generation
        if ard["H-A-Alpha"] and not pol_file:
            out_pol, error = create_polarimetric_layers(
                master_import.with_suffix(".dim"), out_dir, master_prefix, config_dict
            )
        elif ard["H-A-Alpha"] and pol_file:
            # construct namespace for existing pol layer
            out_pol = str(out_dir / f"{master_prefix}_pol.dim")

        if ard["backscatter"] and not bs_file:
            out_bs, out_ls, error = create_backscatter_layers(
                master_import.with_suffix(".dim"), out_dir, master_prefix, config_dict
            )
        elif ard["backscatter"] and bs_file:
            out_bs = str(out_dir / f"{master_prefix}_bs.dim")

            if ard["create_ls_mask"] and bs_file:
                out_ls = str(out_dir / f"{master_prefix}_LS.dim")

        if coherence and not coh_file:

            # get info on slave from GeoSeries
            slave_prefix = burst["slave_prefix"]
            slave_file = burst["slave_file"]
            slave_burst_nr = burst["slave_burst_nr"]

            with TemporaryDirectory(prefix=f"{str(temp_dir)}/") as temp:

                # convert temp to Path object
                temp = Path(temp)

                # import slave burst
                slave_import = temp / f"{slave_prefix}_import"
                import_log = out_dir / f"{slave_prefix}_import.err_log"

                try:
                    slc.burst_import(
                        slave_file,
                        slave_import,
                        import_log,
                        swath,
                        slave_burst_nr,
                        config_dict,
                    )
                except (GPTRuntimeError, NotValidFileError) as error:
                    if slave_import.with_suffix(".dim").exists():
                        h.delete_dimap(slave_import)

                    logger.info(error)
                    return burst.bid, burst.Date, None, None, None, None, error

                out_coh, error = create_coherence_layers(
                    master_import.with_suffix(".dim"),
                    slave_import.with_suffix(".dim"),
                    out_dir,
                    master_prefix,
                    config_dict,
                )

                # remove master import
                h.delete_dimap(master_import)

        elif coherence and coh_file:
            out_coh = str(out_dir / f"{master_prefix}_coh.dim")

            # remove master import
            h.delete_dimap(master_import)
        else:
            # remove master import
            h.delete_dimap(master_import)

    # in case everything has been already processed,
    # we re-construct the out names for proper return value
    else:
        if ard["H-A-Alpha"] and pol_file:
            out_pol = str(out_dir / f"{master_prefix}_pol.dim")

        if ard["backscatter"] and bs_file:
            out_bs = str(out_dir / f"{master_prefix}_bs.dim")

        if ard["create_ls_mask"] and bs_file:
            out_ls = str(out_dir / f"{master_prefix}_LS.dim")

        if coherence and coh_file:
            out_coh = str(out_dir / f"{master_prefix}_coh.dim")

    return burst.bid, burst.Date, out_bs, out_ls, out_pol, out_coh, None


if __name__ == "__main__":

    import argparse

    # write a description
    descript = """This is a command line client for the creation of
               Sentinel-1 ARD data from Level 1 SLC bursts.
               """

    epilog = """
             Example:
             to do


             """

    # create a parser
    parser = argparse.ArgumentParser(description=descript, epilog=epilog)

    # search parameters
    parser.add_argument(
        "-b",
        "--burst",
        help=" (str) path to OST burst inventory file for" " one burst",
        required=True,
    )
    parser.add_argument(
        "-c",
        "--config_file",
        help=" (str) path to OST project configuration file",
        required=True,
    )

    args = parser.parse_args()

    # execute processing
    burst_to_ard(args.burst_inventory, args.config_file)
