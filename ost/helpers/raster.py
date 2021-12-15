#! /usr/bin/env python

"""Helper functions for raster data

"""
import os
import numpy as np
import json
import itertools
import shutil
from datetime import datetime

from godale._concurrent import Executor
from osgeo import gdal

import fiona
import imageio
import pyproj
import rasterio as rio
import rasterio.mask
from rasterio.features import shapes
from scipy.interpolate import LinearNDInterpolator
from shapely.geometry import shape, MultiPolygon

from ost.helpers import helpers as h


def polygonize_ls(infile, outfile, driver="GeoJSON"):

    with rio.open(infile) as src:
        image = src.read(1)

    image[image > 0] = 1
    mask = image == 1

    results = (
        {"properties": {"raster_val": v}, "geometry": s}
        for i, (s, v) in enumerate(shapes(image, mask=mask, transform=src.transform))
    )

    with fiona.open(
        outfile,
        "w",
        driver=driver,
        crs=pyproj.Proj(src.crs).srs,
        schema={"properties": [("raster_val", "int")], "geometry": "Polygon"},
    ) as dst:
        dst.writerecords(results)


def _closure_procedure(geojson_shape, buffer):
    """Helper function to buffer geo-json like geometries and close islands

    :param geojson_shape:
    :param buffer:
    :return:
    """

    # close islands
    s = (
        shape(geojson_shape)
        .buffer(-buffer, 1, join_style=2)
        .buffer(buffer, 1, join_style=2)
    )

    # do negative buffering to reduce border issues due to resampling
    s = s.buffer(buffer, 1, join_style=2)

    # upcast to MultiPolygon
    if s.geom_type == "Polygon":
        s = MultiPolygon([s])

    return s.__geo_interface__


def polygonize_bounds(infile, outfile, mask_value=1, driver="GeoJSON"):
    """Polygonize a raster mask based on a mask value

    :param infile:
    :type infile:
    :param outfile:
    :type outfile:
    :param mask_value:
    :type mask_value: int/float, optional
    :param driver:
    :type driver: str, optional
    :return:
    """

    with rio.open(infile) as src:
        image = src.read(1)
        pixel_size_x, pixel_size_y = src.res
        neg_buffer = np.round(-5 * pixel_size_x, 5)

        if mask_value is not None:
            mask = image == mask_value
        else:
            mask = None

        results = (
            {
                "properties": {"raster_val": v},
                "geometry": _closure_procedure(s, neg_buffer),
            }
            for i, (s, v) in enumerate(
                shapes(image, mask=mask, transform=src.transform)
            )
        )

        with fiona.open(
            outfile,
            "w",
            driver=driver,
            crs=pyproj.Proj(src.crs).srs,
            schema={"properties": [("raster_val", "int")], "geometry": "MultiPolygon"},
        ) as dst:
            dst.writerecords(results)


def outline(infile, outfile, ndv=0, less_then=False, driver="GeoJSON"):
    """Generates a vector file with the valid areas of a raster file

    :param infile: input raster file
    :param outfile: output shapefile
    :param ndv: no-data-value
    :param less_then:
    :param driver:
    :return:
    """

    with rio.open(infile) as src:

        # get metadata
        meta = src.meta

        # update driver, datatype and reduced band count
        meta.update(driver="GTiff", dtype="uint8", count=1)

        # we update the meta for more efficient looping due to
        # hardcoded vrt blocksizes
        meta.update(blockxsize=src.shape[1], blockysize=1)

        # create outfiles
        with rio.open(outfile.with_suffix(".tif"), "w", **meta) as out_min:

            # loop through blocks
            for _, window in out_min.block_windows(1):

                # read array with all bands
                stack = src.read(range(1, src.count + 1), window=window)

                # get stats
                stack[stack == np.nan] = 0
                min_array = np.min(stack, axis=0)

                if less_then is True:
                    min_array[min_array <= ndv] = 0
                else:
                    min_array[min_array == ndv] = 0

                min_array[min_array != ndv] = 1

                # write to dest
                out_min.write(np.uint8(min_array), window=window, indexes=1)

    # now let's polygonize
    polygonize_bounds(outfile.with_suffix(".tif"), outfile, driver=driver)
    outfile.with_suffix(".tif").unlink()


def image_bounds(data_dir):
    """Function to create a polygon of image boundary

    This function for all files within a dimap data directory

    :param data_dir:
    :return:
    """
    filelist = []
    for file in data_dir.glob("*img"):
        filelist.append(str(file))

        temp_extent = data_dir / f"{data_dir.name}_bounds.vrt"
        # build vrt stack from all scenes
        gdal.BuildVRT(
            str(temp_extent),
            filelist,
            options=gdal.BuildVRTOptions(srcNodata=0, separate=True),
        )

        file_id = "_".join(data_dir.name.split("_")[:2])
        outline(temp_extent, data_dir / f"{file_id}_bounds.json")
        (data_dir / f"{data_dir.name}_bounds.vrt").unlink()


# convert power to dB
def convert_to_db(pow_array):
    """Convert array of SAR power to decibel

    :param pow_array:
    :return:
    """

    import warnings

    warnings.filterwarnings("ignore", "invalid value encountered", RuntimeWarning)

    # assure all values are positive (strangely that's not always the case)
    pow_array[pow_array < 0] = 0.0000001

    # convert to dB
    db_array = np.multiply(10, np.log10(pow_array.clip(min=0.0000000000001)))

    # return
    return db_array


# rescale sar dB dat ot integer format
def scale_to_int(float_array, min_value, max_value, data_type):
    """Convert a float array to integer by linear scaling between min and max

    :param float_array:
    :param min_value:
    :param max_value:
    :param data_type:
    :return:
    """

    import warnings

    warnings.filterwarnings("ignore", "invalid value encountered", RuntimeWarning)

    # set output min and max
    display_min = 1.0
    if data_type == "uint8":
        display_max = 255.0
    elif data_type == "uint16":
        display_max = 65535.0
    else:
        raise ValueError("Datatype should be either uint8 or uint16.")

    # calculate stretch parameters a and x
    a = min_value - ((max_value - min_value) / (display_max - display_min))
    x = (max_value - min_value) / (display_max - 1)

    # clip float array to min and max for stretching
    float_array[float_array > max_value] = max_value
    float_array[float_array < min_value] = min_value

    # stretch array
    stretched = np.divide(np.subtract(float_array, a), x)

    # round to integer, convert nans to 0 and set datatype
    return np.round(np.nan_to_num(stretched)).astype(data_type)


def rescale_to_float(int_array, data_type):
    """Re-convert a previously converted integer array back to float

    :param int_array:
    :param data_type:
    :return:
    """

    # convert to float and turn 0s to nan
    int_array = int_array.astype("float32")
    int_array[int_array == 0] = np.nan

    # calculate conversion parameters
    if data_type == "uint8":
        a = np.divide(35.0, 254.0)
        b = np.subtract(-30.0, a)
    elif data_type == "uint16":
        a = np.divide(35.0, 65535.0)
        b = np.subtract(-30.0, a)
    else:
        raise TypeError("Unknown datatype")

    # apply stretch
    return np.add(np.multiply(int_array, a), b)


def fill_internal_nans(array):
    """Function that fills no-data values with interpolation

    :param array:
    :return:
    """
    print("a")
    a = array[0].astype("float32")
    shape = a.shape
    a[a == 0] = np.nan
    x, y = np.indices(shape)
    print("b")
    interp = np.array(a)
    print("c")
    # interp[np.isnan(interp)] = griddata(
    #    (x[~np.isnan(a)], y[~np.isnan(a)]),  # points we know
    #    a[~np.isnan(a)],  # values we know
    #    (x[np.isnan(a)], y[np.isnan(a)]),
    #    method='cubic'
    # )

    interp[np.isnan(interp)] = LinearNDInterpolator(
        (a[~np.isnan(a)], 2), a[~np.isnan(a)], (x[np.isnan(a)], y[np.isnan(a)])
    )
    print("d")
    return interp[np.newaxis, :]


def mask_by_shape(
    infile,
    outfile,
    vector,
    to_db=False,
    datatype="float32",
    rescale=True,
    min_value=0.000001,
    max_value=1,
    ndv=None,
    description=True,
):
    """Mask a raster layer with a vector file (including data conversions)

    :param infile:
    :param outfile:
    :param vector:
    :param to_db:
    :param datatype:
    :param rescale:
    :param min_value:
    :param max_value:
    :param ndv:
    :param description:
    :return:
    """

    # import vector geometries
    with fiona.open(vector, "r") as file:
        features = [feature["geometry"] for feature in file if feature["geometry"]]

    # import raster file
    with rio.open(infile) as src:
        out_image, out_transform = rio.mask.mask(src, features, crop=True)
        out_meta = src.meta.copy()
        out_image = np.ma.masked_where(out_image == ndv, out_image)

    # unmask array
    out_image = out_image.data

    if out_image.dtype == "float32":
        out_image[out_image == 0] = np.nan

    # if to decibel should be applied
    if to_db is True:
        out_image = convert_to_db(out_image)

    # if rescaling to integer should be applied
    if rescale and datatype == "uint8":
        out_image = scale_to_int(out_image, min_value, max_value, "uint8")
    elif rescale and datatype == "uint16":
        out_image = scale_to_int(out_image, min_value, max_value, "uint16")

    # update metadata for outfile
    out_meta.update(
        {
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
            "nodata": ndv,
            "dtype": datatype,
            "tiled": True,
            "blockxsize": 128,
            "blockysize": 128,
        }
    )

    # check that block size is in range of image (for very small subsets)
    if out_meta["blockysize"] > out_image.shape[1]:
        del out_meta["blockysize"]

    if out_meta["blockxsize"] > out_image.shape[2]:
        del out_meta["blockxsize"]

    # write output
    with rio.open(outfile, "w", **out_meta) as dest:
        dest.write(np.nan_to_num(out_image))

        # add some metadata to tif-file
        if description:
            dest.update_tags(1, BAND_NAME=str(infile.name)[:-4])
            dest.set_band_description(1, str(infile.name)[:-4])


def create_tscan_vrt(timescan_dir, config_file):

    # load ard parameters
    if isinstance(config_file, dict):
        config_dict = config_file
    else:
        config_file = open(config_file, "r")
        config_dict = json.load(config_file)
        config_file.close()

    ard_tscan = config_dict["processing"]["time-scan_ARD"]

    # loop through all potential products
    # a products list
    product_list = [
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

    metrics = ard_tscan["metrics"]
    if "percentiles" in metrics:
        metrics.remove("percentiles")
        metrics.extend(["p95", "p5"])

    if "harmonics" in metrics:
        metrics.remove("harmonics")
        metrics.extend(["amplitude", "phase", "residuals"])

    i, outfiles = 0, []
    iteration = itertools.product(product_list, metrics)
    for product, metric in iteration:

        # get file and add number for outfile
        infile = timescan_dir / f"{product}.{metric}.tif"

        # if there is no file sto the iteration
        if not infile.exists():
            continue

        i += 1
        # create namespace for output file and add to list for vrt creation
        outfile = timescan_dir / f"{i:02d}.{product}.{metric}.tif"
        outfiles.append(str(outfile))

        # otherwise rename the file
        infile.replace(outfile)

    # build vrt
    gdal.BuildVRT(
        str(timescan_dir / "Timescan.vrt"),
        outfiles,
        options=gdal.BuildVRTOptions(srcNodata=0, separate=True),
    )


def norm(array, percentile=False):
    """Normalize array by its min/max or 2- and 98 percentile

    :param array:
    :param percentile:
    :return:
    """
    if percentile:
        array_min, array_max = (np.percentile(array, 2), np.percentile(array, 98))
    else:
        array_min, array_max = np.nanmin(array), np.nanmax(array)

    return (array - array_min) / (array_max - array_min)


def visualise_rgb(filepath, shrink_factor=25):
    """

    :param filepath:
    :param shrink_factor:
    :return:
    """

    import matplotlib.pyplot as plt

    with rio.open(filepath) as src:

        # read array and resample by shrink_factor
        array = src.read(
            out_shape=(
                src.count,
                int(src.height / shrink_factor),
                int(src.width / shrink_factor),
            ),
            resampling=5,  # 5 = average
        )

    # convert 0 to nans
    array[array == 0] = np.nan

    if src.count == 3:
        # normalise RGB bands
        red = norm(scale_to_int(array[0], -18, 0, "uint8"))
        green = norm(scale_to_int(array[1], -25, -5, "uint8"))
        blue = norm(scale_to_int(array[2], 1, 15, "uint8"))

    else:
        red = norm(scale_to_int(array[0], -18, 0, "uint8"))
        green = norm(scale_to_int(array[0], -18, 0, "uint8"))
        blue = norm(scale_to_int(array[0], -18, 0, "uint8"))

    # stack image
    img = np.dstack((red, green, blue))

    plt.imshow(img)


def get_min(file, dtype="float32"):

    mins = {
        "bs.VV": -20,
        "bs.VH": -25,
        "bs.HH": -20,
        "bs.HV": -25,
        "bs.ratio": 1,
        "coh.VV": 0.1,
        "coh.VH": 0.1,
        "pol.Alpha": 60,
        "pol.Entropy": 0.1,
        "pol.Anisotropy": 0.1,
        "coh_IW1_VV": 0.1,
        "coh_IW2_VV": 0.1,
        "coh_IW3_VV": 0.1,
        "coh_IW1_VH": 0.1,
        "coh_IW2_VH": 0.1,
        "coh_IW3_VH": 0.1,
    }

    if dtype == "uint16":
        for item, value in mins.items():
            mins[item] = scale_to_int(np.array(value), -30, 5, "uint16")
    elif dtype == "uint8":
        for item, value in mins.items():
            mins[item] = scale_to_int(np.array(value), -30, 5, "uint8")

    for key, items in mins.items():
        if key in file:
            return items


def get_max(file, dtype="float32"):

    maxs = {
        "bs.VV": 0,
        "bs.VH": -12,
        "bs.HH": 0,
        "bs.HV": -5,
        "bs.ratio": 15,
        "coh.VV": 0.8,
        "coh.VH": 0.75,
        "pol.Alpha": 80,
        "pol.Entropy": 0.8,
        "pol.Anisotropy": 0.8,
        "coh_IW1_VV": 0.8,
        "coh_IW2_VV": 0.8,
        "coh_IW3_VV": 0.8,
        "coh_IW1_VH": 0.75,
        "coh_IW2_VH": 0.75,
        "coh_IW3_VH": 0.75,
    }

    if dtype == "uint16":
        for item, value in maxs.items():
            maxs[item] = scale_to_int(np.array(value), -30, 5, "uint16")
    elif dtype == "uint8":
        for item, value in maxs.items():
            maxs[item] = scale_to_int(np.array(value), -30, 5, "uint8")

    for key, items in maxs.items():
        if key in file:
            return items


def calc_min(band, stretch="minmax"):

    if stretch == "percentile":
        band_min = np.percentile(band, 2)
    elif stretch == "minmax":
        band_min = np.nanmin(band)
    else:
        raise ValueError(
            "Please select one of percentile or minmax " "for the stretch parameter."
        )

    return band_min


def calc_max(band, stretch="minmax"):

    if stretch == "percentile":
        band_max = np.percentile(band, 98)
    elif stretch == "minmax":
        band_max = np.nanmax(band)
    else:
        raise ValueError(
            "Please select one of percentile or minmax " "for the stretch parameter."
        )
    return band_max


def stretch_to_8bit(file, layer, dtype, aut_stretch=False):

    if aut_stretch:
        min_val = calc_min(layer, "percentile")
        max_val = calc_max(layer, "percentile")
    else:
        min_val = get_min(file, dtype)
        max_val = get_max(file, dtype)

    # if dtype == 'float32':
    layer = layer.astype("float32")
    layer[layer == 0] = np.nan

    layer = scale_to_int(layer, min_val, max_val, "uint8")
    return np.nan_to_num(layer)


def combine_timeseries(processing_dir, config_dict, timescan=True):

    # namespaces for folder
    comb_dir = processing_dir / "combined"
    if comb_dir.exists():
        h.remove_folder_content(comb_dir)

    tseries_dir = comb_dir / "Timeseries"
    tseries_dir.mkdir(parents=True, exist_ok=True)

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

    out_files, iter_list = [], []
    for product_type in PRODUCT_LIST:

        filelist = list(processing_dir.glob(f"*/Timeseries/*{product_type}.tif"))

        if len(filelist) > 1:
            datelist = sorted([file.name.split(".")[1] for file in filelist])

            for i, date in enumerate(datelist):
                file = list(
                    processing_dir.glob(f"*/Timeseries/*{date}*{product_type}.tif")
                )
                outfile = tseries_dir / f"{i+1:02d}.{date}.{product_type}.tif"

                shutil.copy(file[0], str(outfile))
                out_files.append(str(outfile))

            vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
            out_vrt = str(tseries_dir / f"Timeseries.{product_type}.vrt")
            gdal.BuildVRT(str(out_vrt), out_files, options=vrt_options)

            if timescan:
                from ost.generic import timescan as ts

                ard = config_dict["processing"]["single_ARD"]
                ard_mt = config_dict["processing"]["time-series_ARD"]
                ard_tscan = config_dict["processing"]["time-scan_ARD"]

                # get the db scaling right
                to_db = ard["to_db"]
                if ard["to_db"] or ard_mt["to_db"]:
                    to_db = True

                dtype_conversion = (
                    True if ard_mt["dtype_output"] != "float32" else False
                )

                tscan_dir = comb_dir / "Timescan"
                tscan_dir.mkdir(parents=True, exist_ok=True)

                # get timeseries vrt
                time_series = tseries_dir / f"Timeseries.{product_type}.vrt"

                if not time_series.exists():
                    continue

                # create a datelist for harmonics
                scene_list = [
                    str(file) for file in list(tseries_dir.glob(f"*{product_type}.tif"))
                ]

                # create a datelist for harmonics calculation
                datelist = []
                for file in sorted(scene_list):
                    datelist.append(os.path.basename(file).split(".")[1])

                # define timescan prefix
                timescan_prefix = tscan_dir / f"{product_type}"

                iter_list.append(
                    [
                        time_series,
                        timescan_prefix,
                        ard_tscan["metrics"],
                        dtype_conversion,
                        to_db,
                        ard_tscan["remove_outliers"],
                        datelist,
                    ]
                )

    if timescan:
        # now we run with godale, which works also with 1 worker
        executor = Executor(
            executor=config_dict["executor_type"],
            max_workers=config_dict["max_workers"],
        )

        # run timescan creation
        out_dict = {"track": [], "prefix": [], "metrics": [], "error": []}
        for task in executor.as_completed(func=ts.gd_mt_metrics, iterable=iter_list):
            burst, prefix, metrics, error = task.result()
            out_dict["track"].append(burst)
            out_dict["prefix"].append(prefix)
            out_dict["metrics"].append(metrics)
            out_dict["error"].append(error)

        create_tscan_vrt(tscan_dir, config_dict)


def create_rgb_jpeg(
    filelist,
    outfile=None,
    shrink_factor=1,
    resampling_factor=5,
    plot=False,
    date=None,
    filetype=None,
):
    """

    :param filelist:
    :param outfile:
    :param shrink_factor:
    :param resampling_factor: 5 is average
    :param plot:
    :param date:
    :param filetype:
    :return:
    """

    import matplotlib.pyplot as plt

    # convert file sto string
    filelist = [str(file) for file in filelist]

    with rio.open(filelist[0]) as src:

        # get metadata
        out_meta = src.meta.copy()
        dtype = src.meta["dtype"]

        # !!!assure that dimensions match ####
        new_height = int(src.height / shrink_factor)
        new_width = int(src.width / shrink_factor)
        out_meta.update(height=new_height, width=new_width)
        count = 1

        layer1 = src.read(
            out_shape=(src.count, new_height, new_width), resampling=resampling_factor
        )[0]

    if len(filelist) > 1:
        with rio.open(filelist[1]) as src:
            layer2 = src.read(
                out_shape=(src.count, new_height, new_width),
                resampling=resampling_factor,
            )[0]
            count = 3

    if len(filelist) == 2:  # that should be the BS ratio case

        if dtype == "float32":
            layer3 = scale_to_int(np.subtract(layer1, layer2), 1, 15, "uint8")
        else:
            layer3 = scale_to_int(
                np.subtract(
                    rescale_to_float(layer1, dtype), rescale_to_float(layer2, dtype)
                ),
                1,
                15,
                "uint8",
            )

    elif len(filelist) == 3:
        # that's the full 3layer case
        with rio.open(filelist[2]) as src:
            layer3 = src.read(
                out_shape=(src.count, new_height, new_width),
                resampling=resampling_factor,
            )[0]

            layer3 = stretch_to_8bit(filelist[2], layer3, dtype)

    elif len(filelist) > 3:
        return RuntimeError("Not more than 3 bands allowed for creation of RGB file")

    # create empty array
    arr = np.zeros((int(out_meta["height"]), int(out_meta["width"]), int(count)))

    # fill array with layers
    arr[:, :, 0] = stretch_to_8bit(filelist[0], layer1, dtype)
    if len(filelist) > 1:
        arr[:, :, 1] = stretch_to_8bit(filelist[1], layer2, dtype)
        arr[:, :, 2] = layer3

    # transpose array to gdal format
    arr = np.transpose(arr, [2, 0, 1])

    # update outfile's metadata
    filetype = filetype if filetype else "JPEG"
    out_meta.update({"driver": filetype, "dtype": "uint8", "count": count})

    if outfile:  # write array to disk
        with rio.open(outfile, "w", **out_meta) as out:
            out.write(arr.astype("uint8"))

        if date:

            # convert date to human readable
            # for mosaic or coherence case
            if len(date) > 6:
                try:
                    start, end = date.split("-")
                    string = "Mosaic"
                    try:
                        start = start.split("_")[0]
                        end = end.split("_")[1]
                        string = "Coh. Mosaic"
                    except Exception:
                        pass
                except ValueError:
                    start, end = date.split("_")
                    string = "Intf. Coherence"

                start = datetime.strptime(start, "%y%m%d")
                start = datetime.strftime(start, "%d.%m.%Y")
                end = datetime.strptime(end, "%y%m%d")
                end = datetime.strftime(end, "%d.%m.%Y")
                date = f"{string}: {start}-{end}"
            # for single date
            else:
                date = datetime.strptime(date, "%y%m%d")
                date = f'Image from: {datetime.strftime(date, "%d.%m.%Y")}'

            # calculate label height on the basis of the image width
            label_height = np.floor(np.divide(int(out_meta["height"]), 15))

            # create imagemagick command
            cmd = (
                f"convert -background '#0008' -fill white -gravity center "
                f'-size {out_meta["width"]}x{label_height} '
                f'caption:"{date}" {outfile} +swap -gravity north '
                f"-composite {outfile}"
            )
            # and execute
            h.run_command(cmd, f"{outfile}.log", elapsed=False)

    if plot:
        plt.imshow(arr)


def create_timeseries_animation(
    timeseries_folder,
    product_list,
    out_folder,
    shrink_factor=1,
    resampling_factor=5,
    duration=1,
    add_dates=False,
    prefix=False,
):

    # get number of products
    nr_of_products = len(list(timeseries_folder.glob(f"*{product_list[0]}.tif")))

    # for coherence it must be one less
    # if 'coh.VV' in product_list or 'coh.VH' in product_list:
    #    nr_of_products = nr_of_products - 1

    outfiles = []
    for i in range(nr_of_products):

        filelist = [
            list(timeseries_folder.glob(f"{i+1:02d}.*.{product}.tif"))[0]
            for product in product_list
        ]

        dates = filelist[0].name.split(".")[1]

        if add_dates:
            date = dates
        else:
            date = None

        create_rgb_jpeg(
            filelist,
            out_folder / f"{i+1:02d}.{dates}.jpeg",
            shrink_factor,
            resampling_factor,
            date=date,
        )

        outfiles.append(out_folder / f"{i+1:02d}.{dates}.jpeg")

    # create gif
    if prefix:
        gif_name = f"{prefix}_{product_list[0]}_ts_animation.gif"
    else:
        gif_name = f"{product_list[0]}_ts_animation.gif"
    with imageio.get_writer(
        out_folder / gif_name, mode="I", duration=duration
    ) as writer:

        for file in outfiles:
            image = imageio.imread(file)
            writer.append_data(image)
            file.unlink()
            if file.with_suffix(".jpeg.aux.xml").exists():
                file.with_suffix(".jpeg.aux.xml").unlink()
