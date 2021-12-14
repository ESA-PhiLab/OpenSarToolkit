# -*- coding: utf-8 -*-
import json
import shutil
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import rasterio
import rasterio.mask
from retrying import retry
from osgeo import gdal

from ost.helpers import vector as vec
from ost.helpers import helpers as h

logger = logging.getLogger(__name__)


def create_timeseries_mosaic_vrt(list_of_args):
    ts_dir, product, outfiles = list_of_args

    gdal.BuildVRT(
        str(ts_dir / f"{product}.Timeseries.vrt"),
        [str(outfile) for outfile in outfiles],
        options=gdal.BuildVRTOptions(srcNodata=0, separate=True),
    )


@retry(stop_max_attempt_number=3, wait_fixed=1)
def mosaic(filelist, outfile, config_file, cut_to_aoi=None, harm=None):

    if (outfile.parent / f".{outfile.name[:-4]}.processed").exists():
        logger.info(f"{outfile} already exists.")
        return

    logger.info(f"Mosaicking file {outfile}.")
    with open(config_file, "r") as ard_file:
        config_dict = json.load(ard_file)
        temp_dir = config_dict["temp_dir"]
        aoi = config_dict["aoi"]
        epsg = config_dict["processing"]["single_ARD"]["dem"]["out_projection"]

        if not harm:
            harm = config_dict["processing"]["mosaic"]["harmonization"]

        if not cut_to_aoi:
            cut_to_aoi = config_dict["processing"]["mosaic"]["cut_to_aoi"]

    logfile = outfile.parent / f"{str(outfile)[:-4]}.errLog"

    with TemporaryDirectory(prefix=f"{temp_dir}/") as temp:

        temp = Path(temp)

        # get datatype from first image in our mosaic filelist
        with rasterio.open(filelist.split(" ")[0]) as src:
            dtype = src.meta["dtype"]
            dtype = "float" if dtype == "float32" else dtype

        if cut_to_aoi:
            tempfile = (temp / outfile).name
        else:
            tempfile = outfile

        harm = "band" if harm else "none"

        cmd = (
            f"otbcli_Mosaic -ram 8192  -progress 1 "
            f"-comp.feather large "
            f"-harmo.method {harm} "
            f"-harmo.cost rmse "
            f"-tmpdir {str(temp)} "
            f"-interpolator bco"
            f" -il {filelist} "
            f" -out {str(tempfile)} {dtype}"
        )

        return_code = h.run_command(cmd, logfile)

        if return_code != 0:
            if tempfile.exists():
                tempfile.unlink()

            return

        if cut_to_aoi:

            # get aoi in a way rasterio wants it
            aoi_gdf = vec.wkt_to_gdf(aoi)
            features = vec.gdf_to_json_geometry(aoi_gdf.to_crs(epsg=epsg))

            # import raster and mask
            with rasterio.open(tempfile) as src:
                out_image, out_transform = rasterio.mask.mask(src, features, crop=True)
                out_meta = src.meta.copy()
                ndv = src.nodata
                out_image = np.ma.masked_where(out_image == ndv, out_image)

                out_meta.update(
                    {
                        "driver": "GTiff",
                        "height": out_image.shape[1],
                        "width": out_image.shape[2],
                        "transform": out_transform,
                        "tiled": True,
                        "blockxsize": 128,
                        "blockysize": 128,
                    }
                )

                with rasterio.open(outfile, "w", **out_meta) as dest:
                    dest.write(out_image.data)

            # remove intermediate file
            tempfile.unlink()

        # check
        return_code = h.check_out_tiff(outfile)
        if return_code != 0:
            if outfile.exists():
                outfile.unlink()
        else:
            check_file = outfile.parent / f".{outfile.name[:-4]}.processed"
            with open(str(check_file), "w") as file:
                file.write("passed all tests \n")


def gd_mosaic(list_of_args):

    filelist, outfile, config_file = list_of_args
    mosaic(filelist, outfile, config_file)


def _burst_list(track, date, product, subswath, config_dict):

    from shapely.wkt import loads
    import geopandas as gpd

    aoi = loads(config_dict["aoi"])
    processing_dir = Path(config_dict["processing_dir"])

    # adjust search pattern in case of coherence
    search_last = f"*.{product}.tif" if "coh" in product else f"{product}.tif"

    # search for all bursts within subswath(s) in time-series
    list_of_files = list(
        processing_dir.glob(
            f"[A,D]{track}_{subswath}*/Timeseries/" f"*.{date}.{search_last}"
        )
    )

    # search for timescans (in case timeseries not found)
    if not list_of_files:

        list_of_files = list(
            processing_dir.glob(
                f"[A,D]{track}_{subswath}*/Timescan/" f"*.{product}.{date}.tif"
            )
        )

    if not list_of_files:
        return None

    # get a list of all extent files to check for real AOI overlap
    if config_dict["processing"]["time-series_ARD"]["apply_ls_mask"]:
        list_of_extents = processing_dir.glob(
            f"*{track}_{subswath}*/*{track}*.valid.json"
        )
    else:
        list_of_extents = processing_dir.glob(
            f"*{track}_{subswath}*/*{track}*.min_bounds.json"
        )

    list_of_actual_extents = []
    for burst_extent in list_of_extents:

        burst = gpd.read_file(burst_extent)
        if any(burst.intersects(aoi)):

            burst_name = Path(str(burst_extent).split(".")[-3]).name
            list_of_actual_extents.append(burst_name)

    # filter the bursts for real AOI overlap
    list_of_files = [
        file
        for file in list_of_files
        for pattern in list_of_actual_extents
        if pattern in str(file)
    ]

    # and join them into a otb readable list
    list_of_files = " ".join([str(file) for file in list_of_files])
    return list_of_files


def mosaic_slc_acquisition(track, date, product, outfile, config_file):

    # -------------------------------------
    # 1 load project config
    with open(config_file, "r") as ard_file:
        config_dict = json.load(ard_file)
        temp_dir = Path(config_dict["temp_dir"])

    # create a list of bursts that actually overlap theAOI
    list_of_iw12 = _burst_list(track, date, product, "IW[1,2]", config_dict)
    list_of_iw3 = _burst_list(track, date, product, "IW3", config_dict)

    if list_of_iw12:
        logger.info(
            f"Pre-mosaicking {product} acquisition's IW1 and IW2 subswaths "
            f"from {track} taken at {date}."
        )
        temp_iw12 = temp_dir / f"{date}_{track}_{product}_IW1_2.tif"
        mosaic(list_of_iw12, temp_iw12, config_file, harm=False)

    if list_of_iw3:
        logger.info(
            f"Pre-mosaicking {product} acquisition's IW3 subswath "
            f"from {track} taken at {date}."
        )
        temp_iw3 = temp_dir / f"{date}_{track}_{product}_IW3.tif"
        mosaic(list_of_iw3, temp_iw3, config_file, harm=False)

    if list_of_iw12 and list_of_iw3:
        mosaic(
            " ".join([str(temp_iw12), str(temp_iw3)]),
            outfile,
            config_file,
            False,
            harm=True,
        )

        temp_iw12.unlink()
        temp_iw3.unlink()
    elif list_of_iw12 and not list_of_iw3:
        shutil.move(temp_iw12, outfile)
    elif not list_of_iw12 and list_of_iw3:
        shutil.move(temp_iw3, outfile)
    else:
        return


def gd_mosaic_slc_acquisition(list_of_args):

    track, date, product, outfile, config_file = list_of_args
    mosaic_slc_acquisition(track, date, product, outfile, config_file)
