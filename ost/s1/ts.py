# import stdlib modules
import os
from os.path import join as opj
import glob
import logging
from datetime import datetime

import rasterio
import numpy as np


from ost.helpers import raster as ras


logger = logging.getLogger(__name__)


def create_datelist(path_to_timeseries):
    """Create a text file of acquisition dates within your time-series

    Args:
        path_to_timeseries (str): path to an OST time-series directory
    """

    files = glob.glob("{}/*VV*tif".format(path_to_timeseries))
    dates = sorted([os.path.basename(file).split(".")[1] for file in files])

    with open("{}/datelist.txt".format(path_to_timeseries), "w") as file:
        for date in dates:
            file.write(
                str(datetime.strftime(datetime.strptime(date, "%y%m%d"), "%Y-%m-%d"))
                + " \n"
            )


def create_ts_animation(ts_dir, temp_dir, outfile, shrink_factor):

    for file in sorted(glob.glob(opj(ts_dir, "*VV.tif"))):

        file_index = os.path.basename(file).split(".")[0]
        date = os.path.basename(file).split(".")[1]
        file_vv = file
        file_vh = glob.glob(opj(ts_dir, "{}.*VH.tif".format(file_index)))[0]

        out_temp = opj(temp_dir, "{}.jpg".format(date))

        with rasterio.open(file_vv) as vv_pol:

            # get metadata
            out_meta = vv_pol.meta.copy()

            # !!!assure that dimensions match ####
            new_height = int(vv_pol.height / shrink_factor)
            new_width = int(vv_pol.width / shrink_factor)
            out_shape = (vv_pol.count, new_height, new_width)

            out_meta.update(height=new_height, width=new_width)

            # create empty array
            arr = np.zeros((int(out_meta["height"]), int(out_meta["width"]), int(3)))
            # read vv array
            arr[:, :, 0] = vv_pol.read(out_shape=out_shape, resampling=5)

        with rasterio.open(file_vh) as vh_pol:
            # read vh array
            arr[:, :, 1] = vh_pol.read(out_shape=out_shape, resampling=5)

        # create ratio
        arr[:, :, 2] = np.subtract(arr[:, :, 0], arr[:, :, 1])

        # rescale_to_datatype to uint8
        arr[:, :, 0] = ras.scale_to_int(arr[:, :, 0], -20.0, 0.0, "uint8")
        arr[:, :, 1] = ras.scale_to_int(arr[:, :, 1], -25.0, -5.0, "uint8")
        arr[:, :, 2] = ras.scale_to_int(arr[:, :, 2], 1.0, 15.0, "uint8")

        # update outfile's metadata
        out_meta.update({"driver": "JPEG", "dtype": "uint8", "count": 3})

        # transpose array to gdal format
        arr = np.transpose(arr, [2, 0, 1])

        # write array to disk
        with rasterio.open(out_temp, "w", **out_meta) as out:
            out.write(arr.astype("uint8"))

        # add date
        label_height = np.floor(np.divide(int(out_meta["height"]), 15))
        cmd = "convert -background '#0008' -fill white -gravity center \
              -size {}x{} caption:\"{}\" {} +swap -gravity north \
              -composite {}".format(
            out_meta["width"], label_height, date, out_temp, out_temp
        )
        os.system(cmd)

    # create gif
    lst_of_files = " ".join(sorted(glob.glob(opj(temp_dir, "*jpg"))))
    cmd = "convert -delay 200 -loop 20 {} {}".format(lst_of_files, outfile)
    os.system(cmd)

    for file in glob.glob(opj(temp_dir, "*jpg")):
        os.remove(file)
