#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""This script provides core helper functions for the OST package.
"""

import os
import math
import glob
import shlex
import shutil
import subprocess
import time
from datetime import timedelta
from pathlib import Path
import zipfile
import logging

import gdal
import geopandas as gpd
from shapely.wkt import loads


logger = logging.getLogger(__name__)


def aoi_to_wkt(aoi):
    """Helper function to transform various AOI formats into WKT

    This function is used to import an AOI definition into an OST project.
    The AOIs definition can be from difffrent sources, i.e. an ISO3 country
    code (that calls GeoPandas low-resolution country boundaries),
    a WKT string,

    :param aoi: AOI , which can be an ISO3 country code, a WKT String or
                a path to a shapefile, a GeoPackage or a GeoJSON file
    :type aoi: str/Path
    :return: AOI as WKT string
    :rtype: WKT string
    """

    # load geopandas low res data and check if AOI is ISO3 country code
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    if aoi in world.iso_a3.tolist():

        # get lowres data from geopandas
        country = world.name[world.iso_a3 == aoi].values[0]
        logger.info(
            f'Getting the country boundaries from Geopandas low '
            f'resolution data for {country}'
        )

        # convert to WKT string
        aoi_wkt = world['geometry'][world['iso_a3'] == aoi].values[0].to_wkt()

    # if it is a file
    elif Path(aoi).exists():

        gdf = gpd.GeoDataFrame.from_file(aoi)

        logger.info(f'Using {aoi} as Area of Interest definition.')
    else:
        try:
            # let's check if it is a shapely readable WKT
            loads(str(aoi))
        except:
            raise ValueError('No valid OST AOI definition.')
        else:
            aoi_wkt = aoi

    return aoi_wkt


def timer(start):
    """A helper function to print a time elapsed statement

    :param start:
    :type start:
    :return:
    :rtype: str
    """

    elapsed = time.time() - start
    logger.info(f'Time elapsed: {timedelta(seconds=elapsed)}')


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
    """ A helper function to execute a command line command

    Args:
        command (str): the command to execute
        logfile (str): path to the logfile in case of errors

    """

    # currtime = time.time()

    if os.name == 'nt':
        process = subprocess.run(command, stderr=subprocess.PIPE)
    else:
        process = subprocess.run(shlex.split(command), stderr=subprocess.PIPE)

    return_code = process.returncode

    if return_code != 0 and logfile is not None:
        with open(str(logfile), 'w') as file:
            for line in process.stderr.decode().splitlines():
                file.write(f'{line}\n')

    # if elapsed:
    #    timer(currtime)

    return process.returncode


def delete_dimap(dimap_prefix):
    """Removes both dim and data from a Snap dimap file

    """

    if dimap_prefix.with_suffix('.data').exists():
        shutil.rmtree(dimap_prefix.with_suffix('.data'))

    if dimap_prefix.with_suffix('.dim').exists():
        dimap_prefix.with_suffix('.dim').unlink()


def delete_shapefile(shapefile):
    """Removes the shapefile and all its associated files

    """

    extensions = ('.shp', '.prj', '.shx', '.dbf', '.cpg', 'shb')

    for file in glob.glob('{}*'.format(os.path.abspath(shapefile[:-4]))):

        if len(os.path.abspath(shapefile)) == len(file):

            if file.endswith(extensions):
                os.remove(file)


def move_dimap(infile_prefix, outfile_prefix, to_tif):
    """Function to move dimap's data and dim another locations
    """

    # get any pre-sufffix (e.g. .LS or .bs)
    out_suffix = outfile_prefix.suffixes[0]

    if to_tif:

        suffix_tif = f'{out_suffix}.tif'
        gdal.Warp(
            outfile_prefix.with_suffix(suffix_tif),
            infile_prefix.with_suffix('.dim')
        )

    else:

        # construct final suffix
        suffix_dim = f'{out_suffix}.dim'
        suffix_data = f'{out_suffix}.data'

        # delete outfile if exists
        if outfile_prefix.with_suffix(suffix_data).exists():
            delete_dimap(outfile_prefix)

        # move them
        infile_prefix.with_suffix('.data').rename(
            outfile_prefix.with_suffix(suffix_data)
        )
        infile_prefix.with_suffix('.dim').rename(
            outfile_prefix.with_suffix(suffix_dim)
        )


def check_out_dimap(dimap_prefix, test_stats=True):

    # check if both dim and data exist, else return
    if not dimap_prefix.with_suffix('.dim').exists():
        return f'Output file {dimap_prefix}.dim has not been generated'

    if not dimap_prefix.with_suffix('.data').exists():
        return f'Output file {dimap_prefix}.data has not been generated'

    # check for file size of the dim file
    dim_size_in_mb = dimap_prefix.with_suffix('.dim').stat().st_size / 1048576

    if dim_size_in_mb < 0.1:
        return f'File {dimap_prefix}.dim seems to small.'

    for file in dimap_prefix.with_suffix('.data').glob('*.img'):

        # check size
        data_size_in_mb = file.stat().st_size / 1048576

        if data_size_in_mb < 1:
            return f'Data file {file} in {dimap_prefix}.data seem to small.'

        # test on statistics
        if test_stats:

            # open the file
            ds = gdal.Open(str(file))
            stats = ds.GetRasterBand(1).GetStatistics(0, 1)

            # if difference of min and max is 0 and mean are all 0
            if stats[1] - stats[0] == 0 and stats[2] == 0:
                return (
                    f'Data file {file.name} in {dimap_prefix}.data only '
                    f'contains no data values.'
                )

    return 0


def check_out_tiff(file, test_stats=True):
    return_code = 0
    file = str(file)
    # check if both dim and data exist, else return
    if not os.path.isfile(file):
        return 666

    # check for file size of the dim file
    tiff_size_in_mb = os.path.getsize(file) / 1048576

    if tiff_size_in_mb < 0.3:
        return 666

    if test_stats:
        # open the file
        ds = gdal.Open(file)
        stats = ds.GetRasterBand(1).GetStatistics(0, 1)

        # check for mean value of layer
        if stats[2] == 0:
            return 666

        # check for stddev value of layer
        if stats[3] == 0:
            return 666

        # if difference ofmin and max is 0
        if stats[1] - stats[0] == 0:
            return 666

    return return_code


def check_zipfile(filename):

    try:
        zip_archive = zipfile.ZipFile(filename)
    except zipfile.BadZipFile as er:
        print('Error: {}'.format(er))
        return 1

    try:
        zip_test = zip_archive.testzip()
    except:
        print('Error')
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
