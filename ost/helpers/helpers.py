#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""This script provides core helper functions for the OST package.
"""

import os
from os.path import join as opj
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
import fiona
import geopandas as gpd
from shapely.wkt import loads
from shapely.geometry import shape, LineString

from ost.helpers import vector as vec

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

        with fiona.open(aoi) as src:
            collection = [shape(item['geometry']).to_wkt() for item in src]
            rings = [LineString(pol.exterior.coords).wkt for pol in collection]
            aoi_wkt = rings[0]
            #aoi_wkt = str(vec.shp_to_wkt(aoi))


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

    Args:
        folder: the folder, where everything should be deleted
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

    currtime = time.time()

    if os.name == 'nt':
        process = subprocess.run(command, stderr=subprocess.PIPE)
    else:
        process = subprocess.run(shlex.split(command), stderr=subprocess.PIPE)

    return_code = process.returncode

    if return_code != 0 and logfile is not None:
        with open(str(logfile), 'w') as file:
            for line in process.stderr.decode().splitlines():
                file.write('{}\n'.format(line))

    if elapsed:
        timer(currtime)

    return process.returncode


def delete_dimap(dimap_prefix):
    """Removes both dim and data from a Snap dimap file

    """

    if os.path.isdir('{}.data'.format(dimap_prefix)):
        shutil.rmtree('{}.data'.format(dimap_prefix))

    if os.path.isfile('{}.dim'.format(dimap_prefix)):
        os.remove('{}.dim'.format(dimap_prefix))


def delete_shapefile(shapefile):
    """Removes the shapefile and all its associated files

    """

    extensions = ('.shp', '.prj', '.shx', '.dbf', '.cpg', 'shb')

    for file in glob.glob('{}*'.format(os.path.abspath(shapefile[:-4]))):

        if len(os.path.abspath(shapefile)) == len(file):

            if file.endswith(extensions):
                os.remove(file)


def move_dimap(infile_prefix, outfile_prefix):
    """This function moves a dimap file to another locations


    """

    # delete outfile if exists
    if os.path.isdir('{}.data'.format(outfile_prefix)):
        delete_dimap(outfile_prefix)

    # move them to the outfolder
    shutil.move('{}.data'.format(infile_prefix),
                '{}.data'.format(outfile_prefix))
    shutil.move('{}.dim'.format(infile_prefix),
                '{}.dim'.format(outfile_prefix))


def check_out_dimap(dimap_prefix, test_stats=True):

    # check if both dim and data exist, else return
    if not os.path.isfile('{}.dim'.format(dimap_prefix)):
        raise FileNotFoundError(' Output file {}.dim has not been generated'
                                .format(dimap_prefix))

    if not os.path.isdir('{}.data'.format(dimap_prefix)):
        raise NotADirectoryError(' Output directory {}.dim has not been '
                                 'generated'.format(dimap_prefix)
                                 )

    # check for file size of the dim file
    dim_size_in_mb = os.path.getsize('{}.dim'.format(dimap_prefix)) / 1048576

    if dim_size_in_mb < 0.1:
        raise ValueError(' File {}.dim seems to small.'.format(dimap_prefix))

    for file in glob.glob(opj('{}.data'.format(dimap_prefix), '*.img')):

        # check size
        data_size_in_mb = os.path.getsize(file) / 1048576

        if data_size_in_mb < 1:
            raise ValueError(' Data file {} in {}.data seem to small.'
                             .format(file, dimap_prefix)
                             )

        if test_stats:
            # open the file
            ds = gdal.Open(file)
            stats = ds.GetRasterBand(1).GetStatistics(0, 1)

            # check for mean value of layer
            if stats[2] == 0:
                raise ValueError(' Data file {} in {}.data contains only'
                                 ' no data values.'.format(file, dimap_prefix)
                                 )

            # check for stddev value of layer
            if stats[3] == 0:
                raise ValueError(' Data file {} in {}.data contains only'
                                 ' no data values.'.format(file, dimap_prefix)
                                 )

            # if difference of min and max is 0
            if stats[1] - stats[0] == 0:
                raise ValueError(' Data file {} in {}.data contains only'
                                 ' no data values.'.format(file, dimap_prefix)
                                 )


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
        print(zip_test)
        return zip_test


def resolution_in_degree(latitude, meters):
    '''Convert resolution in meters to degree based on Latitude

    '''

    earth_radius = 6378137
    degrees_to_radians = math.pi / 180.0
    radians_to_degrees = 180.0 / math.pi
    "Given a latitude and a distance west, return the change in longitude."
    # Find the radius of a circle around the earth at given latitude.
    r = earth_radius * math.cos(latitude * degrees_to_radians)
    return (meters / r) * radians_to_degrees
