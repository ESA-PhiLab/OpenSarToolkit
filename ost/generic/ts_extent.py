# -*- coding: utf-8 -*-

# import stdlib modules
import os
from os.path import join as opj
import time
import gdal

from ost.helpers import helpers as h, raster as ras, vector as vec


def mt_extent(list_of_scenes, out_file, temp_dir, buffer=None):
    if type(list_of_scenes) == str:
        list_of_scenes = list_of_scenes.replace("'", '').strip('][').split(', ')
    out_dir = os.path.dirname(out_file)
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)

    # build vrt stack from all scenes
    gdal.BuildVRT(opj(out_dir, 'extent.vrt'),
                  list_of_scenes,
                  options=vrt_options)
    start = time.time()

    outline_file = opj(temp_dir, os.path.basename(out_file))
    ras.outline(opj(out_dir, 'extent.vrt'), outline_file, 0, False)

    vec.exterior(outline_file, out_file, buffer)
    h.delete_shapefile(outline_file)

    os.remove(opj(out_dir, 'extent.vrt'))
    h.timer(start)
