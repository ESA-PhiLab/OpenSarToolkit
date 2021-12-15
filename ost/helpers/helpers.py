#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""This script provides core helper functions for the OST package.
"""

import os
import math
import glob
import time
import shlex
import shutil
import subprocess
import zipfile
import logging
from pathlib import Path
from datetime import timedelta
from osgeo import gdal

logger = logging.getLogger(__name__)


def timer(start):
    """A helper function to print a time elapsed statement

    :param start:
    :type start:
    :return:
    :rtype: str
    """

    elapsed = time.time() - start
    logger.debug(f"Time elapsed: {timedelta(seconds=elapsed)}")


def remove_folder_content(folder):
    """A helper function that cleans the content of a folder

    :param folder:
    """

    for root, dirs, files in os.walk(folder):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


def run_command(command, logfile=None, elapsed=True):
    """

    :param command:
    :param logfile:
    :param elapsed:
    :return:
    """

    currtime = time.time()

    if os.name == "nt":
        process = subprocess.run(command, stderr=subprocess.PIPE)
    else:
        process = subprocess.run(shlex.split(command), stderr=subprocess.PIPE)

    return_code = process.returncode

    if return_code != 0 and logfile is not None:
        with open(str(logfile), "w") as file:
            for line in process.stderr.decode().splitlines():
                file.write(f"{line}\n")

    if elapsed:
        timer(currtime)

    return process.returncode


def delete_dimap(dimap_prefix):
    """Removes both dim and data from a Snap dimap file"""

    if dimap_prefix.with_suffix(".data").exists():
        shutil.rmtree(dimap_prefix.with_suffix(".data"))

    if dimap_prefix.with_suffix(".dim").exists():
        dimap_prefix.with_suffix(".dim").unlink()


def delete_shapefile(shapefile):
    """Removes the shapefile and all its associated files"""

    extensions = (".shp", ".prj", ".shx", ".dbf", ".cpg", "shb")

    for file in glob.glob(f"{os.path.abspath(shapefile[:-4])}*"):

        if len(os.path.abspath(shapefile)) == len(file) and file.endswith(extensions):

            os.remove(file)


def move_dimap(infile_prefix, outfile_prefix, to_tif):
    """Function to move dimap's data and dim another locations"""

    if to_tif:

        gdal.Warp(outfile_prefix.with_suffix(".tif"), infile_prefix.with_suffix(".dim"))

    else:

        # delete outfile if exists
        if outfile_prefix.with_suffix(".data").exists():
            delete_dimap(outfile_prefix)

        # move them
        try:
            infile_prefix.with_suffix(".data").rename(
                outfile_prefix.with_suffix(".data")
            )
        except OSError:

            shutil.copytree(
                infile_prefix.with_suffix(".data"), outfile_prefix.with_suffix(".data")
            )
            shutil.rmtree(infile_prefix.with_suffix(".data"))

        try:
            infile_prefix.with_suffix(".dim").rename(outfile_prefix.with_suffix(".dim"))
        except OSError:
            shutil.move(
                infile_prefix.with_suffix(".dim"), outfile_prefix.with_suffix(".dim")
            )


def check_out_dimap(dimap_prefix, test_stats=True):

    # check if both dim and data exist, else return
    if not dimap_prefix.with_suffix(".dim").exists():
        return f"Output file {dimap_prefix}.dim has not been generated."

    if not dimap_prefix.with_suffix(".data").exists():
        return f"Output file {dimap_prefix}.data has not been generated."

    # check for file size of the dim file
    dim_size = dimap_prefix.with_suffix(".dim").stat().st_size

    if dim_size < 8:
        return f"File {dimap_prefix}.dim seems to small."

    for file in dimap_prefix.with_suffix(".data").glob("*.img"):

        # check size
        data_size = file.stat().st_size

        if data_size < 8:
            return f"Data file {file} in {dimap_prefix}.data seem to small."

        # test on statistics
        if test_stats:

            # open the file
            ds = gdal.Open(str(file))
            stats = ds.GetRasterBand(1).GetStatistics(0, 1)

            # if difference of min and max is 0 and mean are all 0
            if stats[1] - stats[0] == 0 and stats[2] == 0:
                return (
                    f"Data file {file.name} in {dimap_prefix}.data only "
                    f"contains no data values."
                )

    return 0


def check_out_tiff(file, test_stats=True):

    return_code = 0
    if isinstance(file, str):
        file = Path(file)

    # check if both dim and data exist, else return
    if not file.exists():
        return f"Output file {file.name} has not been generated."

    # check for file size of the dim file
    tiff_size = file.stat().st_size

    if tiff_size < 8:
        return f"File {file.name} seems to small."

    if test_stats:
        # open the file
        ds = gdal.Open(str(file))
        stats = ds.GetRasterBand(1).GetStatistics(0, 1)

        # if difference of min and max is 0 and mean are all 0
        if stats[1] - stats[0] == 0 and stats[2] == 0:
            return f"Data file {file.name} only contains no data values."

    return return_code


def check_zipfile(filename):

    try:
        zip_archive = zipfile.ZipFile(filename)
    except zipfile.BadZipFile as er:
        print("Error: {}".format(er))
        return 1

    try:
        zip_test = zip_archive.testzip()
    except Exception:
        print("Error")
        return 1
    else:
        return zip_test


def resolution_in_degree(latitude, meters):
    """Convert resolution in meters to degree based on Latitude

    :param latitude:
    :param meters:
    :return:
    """

    earth_radius = 6378137
    degrees_to_radians = math.pi / 180.0
    radians_to_degrees = 180.0 / math.pi
    "Given a latitude and a distance west, return the change in longitude."
    # Find the radius of a circle around the earth at given latitude.
    r = earth_radius * math.cos(latitude * degrees_to_radians)
    return (meters / r) * radians_to_degrees
