#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import rasterio

# import zipfile
import numpy as np
from pathlib import Path
from tempfile import TemporaryDirectory

from ost.generic import common_wrappers as common
from ost.helpers import helpers as h, raster as ras
from ost.helpers.errors import GPTRuntimeError, NotValidFileError
from ost.s1 import grd_wrappers as grd

logger = logging.getLogger(__name__)


def grd_to_ard(filelist, config_file):
    """Main function for the grd to ard generation

    This function represents the full workflow for the generation of an
    Analysis-Ready-Data product. The standard parameters reflect the CEOS
    ARD defintion for Sentinel-1 backcsatter products.

    By changing the parameters, taking care of all parameters
    that can be given. The function can handle multiple inputs of the same
    acquisition, given that there are consecutive data takes.

    :param filelist: must be a list with one or more
                     absolute paths to GRD scene(s)
    :param config_file:
    :return:
    """

    from ost.s1.s1scene import Sentinel1Scene

    # ----------------------------------------------------
    # 1 load relevant config parameters
    with open(config_file, "r") as file:
        config_dict = json.load(file)
        ard = config_dict["processing"]["single_ARD"]
        processing_dir = Path(config_dict["processing_dir"])
        subset = config_dict["subset"]

    # ----------------------------------------------------
    # 2 define final destination dir/file and ls mask

    # get acq data and track from first scene in list
    first = Sentinel1Scene(Path(filelist[0]).stem)
    acquisition_date = first.start_date
    track = first.rel_orbit

    logger.info(f"Processing acquisition from {acquisition_date} over track {track}.")

    # construct namespace for out directory etc.
    out_dir = processing_dir / f"{track}/{acquisition_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    file_id = f"{acquisition_date}_{track}"
    out_final = out_dir / f"{file_id}_bs"
    out_ls_mask = out_dir / f"{file_id}_LS"

    suf = ".tif" if ard["to_tif"] else ".dim"

    # ----------------------------------------------------
    # 3 check if already processed
    if (out_dir / ".processed").exists() and out_final.with_suffix(suf).exists():
        logger.info(
            f"Acquisition from {acquisition_date} of track {track} "
            f"already processed"
        )

        if out_ls_mask.with_suffix(suf).exists():
            out_ls = out_ls_mask.with_suffix(suf)
        else:
            out_ls = None

        return filelist, out_final.with_suffix(suf), out_ls, None

    # ----------------------------------------------------
    # 4 run the processing routine

    # this might happen in the create_ard from s1scene class
    if not config_dict["temp_dir"]:
        temp_dir = processing_dir / "temp"
        temp_dir.mkdir(parents=True, exist_ok=True)
    else:
        temp_dir = config_dict["temp_dir"]

    with TemporaryDirectory(prefix=f"{temp_dir}/") as temp:

        # convert temp directory to Path object
        temp = Path(temp)

        # ---------------------------------------------------------------------
        # 4.1 Import
        # slice assembly if more than one scene
        if len(filelist) > 1:

            # if more than one frame import all files
            for file in filelist:

                # unzip for faster import?
                # unpack = None
                # if Path(file).suffix == ".zip":
                #    with zipfile.ZipFile(file, "r") as zip_ref:
                #        zip_ref.extractall(temp)

                #    file = temp / f"{file.stem}.SAFE"
                #    unpack = True

                # create namespace for temporary imported product
                grd_import = temp / f"{file.stem}_imported"

                # create namespace for import log
                logfile = out_dir / f"{file.stem}.Import.errLog"

                # set subset temporally to false for import routine
                config_dict["subset"] = False
                # frame import
                try:
                    grd.grd_frame_import(file, grd_import, logfile, config_dict)
                except (GPTRuntimeError, NotValidFileError) as error:
                    logger.info(error)
                    return filelist, None, None, error

                config_dict["subset"] = subset

                # if unpack:
                #     h.remove_folder_content(file)
                #     file.rmdir()

            # create list of scenes for full acquisition in
            # preparation of slice assembly
            scenelist = " ".join(
                [str(file) for file in list(temp.glob("*imported.dim"))]
            )

            # create namespace for temporary slice assembled import product
            grd_import = temp / f"{file_id}_imported"

            # create namespace for slice assembled log
            logfile = out_dir / f"{file_id}._slice_assembly.errLog"

            # run slice assembly
            try:
                grd.slice_assembly(scenelist, grd_import, logfile, config_dict)
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return filelist, None, None, error

            # delete imported frames
            for file in filelist:
                h.delete_dimap(temp / f"{file.stem}_imported")

            # subset mode after slice assembly
            if subset:

                # create namespace for temporary subset product
                grd_subset = temp / f"{file_id}_imported_subset"

                # create namespace for subset log
                logfile = out_dir / f"{file_id}._slice_assembly.errLog"

                # run subset routine
                try:
                    grd.grd_subset_georegion(
                        grd_import.with_suffix(".dim"), grd_subset, logfile, config_dict
                    )
                except (GPTRuntimeError, NotValidFileError) as error:
                    logger.info(error)
                    return filelist, None, None, error

                # delete slice assembly input to subset
                h.delete_dimap(grd_import)

                # set subset to import for subsequent functions
                grd_import = grd_subset

        # single scene case
        else:

            file = filelist[0]

            # unzip for faster import
            # unpack = None
            # if Path(file).suffix == ".zip":
            #     with zipfile.ZipFile(file, "r") as zip_ref:
            #         zip_ref.extractall(temp)

            #    file = temp / f"{file.stem}.SAFE"
            #    unpack = True

            # create namespace for temporary imported product
            grd_import = temp / f"{file_id}_imported"

            # create namespace for import log
            logfile = out_dir / f"{file_id}.Import.errLog"

            # run frame import
            try:
                grd.grd_frame_import(file, grd_import, logfile, config_dict)
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return filelist, None, None, error

            # if unpack:
            #     h.remove_folder_content(file)
            #     file.rmdir()

        # set input for next step
        infile = grd_import.with_suffix(".dim")

        # ---------------------------------------------------------------------
        # 4.2 GRD Border Noise
        if ard["remove_border_noise"] and not subset:

            # loop through possible polarisations
            for polarisation in ["VV", "VH", "HH", "HV"]:

                # get input file
                file = list(
                    temp.glob(f"{file_id}_imported*data/Intensity_{polarisation}.img")
                )

                # remove border noise
                if len(file) == 1:
                    # run grd Border Remove
                    grd.grd_remove_border(file[0])

        # ---------------------------------------------------------------------
        # 4.3 Calibration

        # create namespace for temporary calibrated product
        calibrated = temp / f"{file_id}_cal"

        # create namespace for calibration log
        logfile = out_dir / f"{file_id}.calibration.errLog"

        # run calibration
        try:
            grd.calibration(infile, calibrated, logfile, config_dict)
        except (GPTRuntimeError, NotValidFileError) as error:
            logger.info(error)
            return filelist, None, None, error

        # delete input
        h.delete_dimap(infile.with_suffix(""))

        # input for next step
        infile = calibrated.with_suffix(".dim")

        # ---------------------------------------------------------------------
        # 4.4 Multi-looking
        if int(ard["resolution"]) >= 20:

            # create namespace for temporary multi-looked product
            multi_looked = temp / f"{file_id}_ml"

            # create namespace for multi-loook log
            logfile = out_dir / f"{file_id}.multilook.errLog"

            # run multi-looking
            try:
                grd.multi_look(infile, multi_looked, logfile, config_dict)
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return filelist, None, None, error

            # delete input
            h.delete_dimap(infile.with_suffix(""))

            # define input for next step
            infile = multi_looked.with_suffix(".dim")

        # ---------------------------------------------------------------------
        # 4.5 Layover shadow mask
        out_ls = None  # set to none for final return statement
        if ard["create_ls_mask"] is True:

            # create namespace for temporary ls mask product
            ls_mask = temp / f"{file_id}_ls_mask"

            # create namespace for ls mask log
            logfile = out_dir / f"{file_id}.ls_mask.errLog"

            # run ls mask routine
            try:
                common.ls_mask(infile, ls_mask, logfile, config_dict)
                out_ls = out_ls_mask.with_suffix(".dim")
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return filelist, None, None, error

            # polygonize
            ls_raster = list(ls_mask.with_suffix(".data").glob("*img"))[0]
            ras.polygonize_ls(ls_raster, ls_mask.with_suffix(".json"))

        # ---------------------------------------------------------------------
        # 4.6 Speckle filtering
        if ard["remove_speckle"]:

            # create namespace for temporary speckle filtered product
            filtered = temp / f"{file_id}_spk"

            # create namespace for speckle filter log
            logfile = out_dir / f"{file_id}.Speckle.errLog"

            # run speckle filter
            try:
                common.speckle_filter(infile, filtered, logfile, config_dict)
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return filelist, None, None, error

            # delete input
            h.delete_dimap(infile.with_suffix(""))

            # define input for next step
            infile = filtered.with_suffix(".dim")

        # ---------------------------------------------------------------------
        # 4.7 Terrain flattening
        if ard["product_type"] == "RTC-gamma0":

            # create namespace for temporary terrain flattened product
            flattened = temp / f"{file_id}_flat"

            # create namespace for terrain flattening log
            logfile = out_dir / f"{file_id}.tf.errLog"

            # run terrain flattening
            try:
                common.terrain_flattening(infile, flattened, logfile, config_dict)
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return filelist, None, None, error

            # delete input file
            h.delete_dimap(infile.with_suffix(""))

            # define input for next step
            infile = flattened.with_suffix(".dim")

        # ---------------------------------------------------------------------
        # 4.8 Linear to db
        if ard["to_db"]:

            # create namespace for temporary db scaled product
            db_scaled = temp / f"{file_id}_db"

            # create namespace for db scaled log
            logfile = out_dir / f"{file_id}.db.errLog"

            # run db scaling routine
            try:
                common.linear_to_db(infile, db_scaled, logfile, config_dict)
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return filelist, None, None, error

            # delete input file
            h.delete_dimap(infile.with_suffix(""))

            # set input for next step
            infile = db_scaled.with_suffix(".dim")

        # ---------------------------------------------------------------------
        # 4.9 Geocoding

        # create namespace for temporary geocoded product
        geocoded = temp / f"{file_id}_bs"

        # create namespace for geocoding log
        logfile = out_dir / f"{file_id}_bs.errLog"

        # run geocoding
        try:
            common.terrain_correction(infile, geocoded, logfile, config_dict)
        except (GPTRuntimeError, NotValidFileError) as error:
            logger.info(error)
            return filelist, None, None, error

        # delete input file
        h.delete_dimap(infile.with_suffix(""))

        # define final destination
        out_final = out_dir / f"{file_id}_bs"

        # ---------------------------------------------------------------------
        # 4.11 Create an outline
        ras.image_bounds(geocoded.with_suffix(".data"))

        # ---------------------------------------------------------------------
        # 4.11 Copy LS Mask vector to data dir
        if ard["create_ls_mask"] is True:
            ls_mask.with_suffix(".json").rename(
                geocoded.with_suffix(".data")
                .joinpath(ls_mask.name)
                .with_suffix(".json")
            )

        # ---------------------------------------------------------------------
        # 4.12 Move to output directory
        h.move_dimap(geocoded, out_final, ard["to_tif"])

    # ---------------------------------------------------------------------
    # 5 write processed file to keep track of files already processed
    with (out_dir / ".processed").open("w") as file:
        file.write("passed all tests \n")

    return filelist, out_final.with_suffix(".dim"), out_ls, None


def ard_to_rgb(infile, outfile, driver="GTiff", to_db=True, shrink_factor=1):
    if infile.suffix != ".dim":
        raise TypeError("File needs to be in BEAM-DIMAP format")

    data_dir = infile.with_suffix(".data")

    if list(data_dir.glob("*VV.img")):
        co_pol = list(data_dir.glob("*VV*.img"))[0]
    elif list(data_dir.glob("*HH.img")):
        co_pol = list(data_dir.glob("*HH*.img"))[0]
    else:
        raise RuntimeError("No co-polarised band found.")

    if list(data_dir.glob("*VH.img")):
        cross_pol = list(data_dir.glob("*VH*.img"))[0]
    elif list(data_dir.glob("*HV.img")):
        cross_pol = list(data_dir.glob("*HV*.img"))[0]
    else:
        cross_pol = Path("/no/foo/no")

    if cross_pol.exists():

        with rasterio.open(co_pol) as co:

            # get meta data
            meta = co.meta

            # update meta
            meta.update(driver=driver, count=3, nodata=0)

            with rasterio.open(cross_pol) as cr:

                if co.shape != cr.shape:
                    raise RuntimeError(
                        "Dimensions of co- and cross-polarised bands " "do not match"
                    )

                new_height = int(co.height / shrink_factor)
                new_width = int(co.width / shrink_factor)
                out_shape = (co.count, new_height, new_width)

                meta.update(height=new_height, width=new_width)

                co_array = co.read(out_shape=out_shape, resampling=5)
                cr_array = cr.read(out_shape=out_shape, resampling=5)

                # turn 0s to nan
                co_array[co_array == 0] = np.nan
                cr_array[cr_array == 0] = np.nan

                # create log ratio by subtracting the dbs
                ratio_array = np.divide(co_array, cr_array)

                if to_db:
                    # turn to db
                    co_array = ras.convert_to_db(co_array)
                    cr_array = ras.convert_to_db(cr_array)

                if driver == "JPEG":
                    co_array = ras.scale_to_int(co_array, -20, 0, "uint8")
                    cr_array = ras.scale_to_int(cr_array, -25, -5, "uint8")
                    ratio_array = ras.scale_to_int(ratio_array, 1, 15, "uint8")
                    meta.update(dtype="uint8")

                with rasterio.open(outfile, "w", **meta) as dst:

                    # write file
                    for k, arr in [(1, co_array), (2, cr_array), (3, ratio_array)]:
                        dst.write(
                            arr[
                                0,
                            ],
                            indexes=k,
                        )

    # greyscale
    else:
        logger.info("No cross-polarised band found. Creating 1-band greyscale" "image.")

        with rasterio.open(co_pol) as co:

            # get meta data
            meta = co.meta

            # update meta
            meta.update(driver=driver, count=1, nodata=0)

            new_height = int(co.height / shrink_factor)
            new_width = int(co.width / shrink_factor)
            out_shape = (co.count, new_height, new_width)

            meta.update(height=new_height, width=new_width)

            co_array = co.read(out_shape=out_shape, resampling=5)

            if to_db:
                # turn to db
                co_array = ras.convert_to_db(co_array)

            if driver == "JPEG":
                co_array = ras.scale_to_int(co_array, -20, 0, "uint8")
                meta.update(dtype="uint8")

            with rasterio.open(outfile, "w", **meta) as dst:
                dst.write(co_array)
