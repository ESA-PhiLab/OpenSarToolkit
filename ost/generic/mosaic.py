# -*- coding: utf-8 -*-
import os
from os.path import join as opj
import gdal
import json
import numpy as np
import rasterio
from pathlib import Path
from tempfile import TemporaryDirectory

from ost.helpers import vector as vec
from ost.helpers import helpers as h


def create_timeseries_mosaic_vrt(list_of_args):
    ts_dir, product, outfiles = list_of_args

    gdal.BuildVRT(
        str(ts_dir.joinpath(f'{product}.Timeseries.vrt')),
        [str(outfile) for outfile in outfiles],
        options=gdal.BuildVRTOptions(srcNodata=0, separate=True)
    )


def mosaic(list_of_args):
    # unpack list of args
    filelist, outfile, project_file = list_of_args

    with open(project_file, 'r') as ard_file:
        project_params = json.load(ard_file)
        temp_dir = project_params['project']['temp_dir']
        aoi = project_params['project']['aoi']
        cut_to_aoi = project_params['processing_parameters']['mosaic']['cut_to_aoi']

    logfile = outfile.parent.joinpath(f'{str((outfile))[:-4]}.errLog')

    with TemporaryDirectory(prefix=f'{temp_dir}/') as temp:

        temp = Path(temp)

        # get datatype from first image in our mosaic filelist
        with rasterio.open(filelist.split(' ')[0]) as src:
            dtype = src.meta['dtype']
            dtype = 'float' if dtype == 'float32' else dtype

        if cut_to_aoi:
            tempfile = temp.joinpath(outfile.name)
        else:
            tempfile = outfile

        cmd = (
            f"otbcli_Mosaic -ram 4096  -progress 1 "
            f"-comp.feather large -harmo.method band "
            f"-harmo.cost rmse "
            f"-tmpdir {str(temp)} "
            f" -il {filelist} "
            f" -out {tempfile} {dtype}"
        )

        return_code = h.run_command(cmd, logfile)
        if return_code != 0:
            if os.path.isfile(tempfile):
                os.remove(tempfile)

            return

        if cut_to_aoi:
            # get aoi ina way rasterio wants it
            features = vec.gdf_to_json_geometry(vec.wkt_to_gdf(aoi))

            # import raster and mask
            with rasterio.open(tempfile) as src:
                out_image, out_transform = rasterio.mask.mask(src, features,
                                                              crop=True)
                out_meta = src.meta.copy()
                ndv = src.nodata
                out_image = np.ma.masked_where(out_image == ndv, out_image)

            out_meta.update({'driver': 'GTiff', 'height': out_image.shape[1],
                             'width': out_image.shape[2],
                             'transform': out_transform,
                             'tiled': True, 'blockxsize': 128,
                             'blockysize': 128})

            with rasterio.open(outfile, 'w', **out_meta) as dest:
                dest.write(out_image.data)

            # remove intermediate file
            os.remove(tempfile)

        # check
        return_code = h.check_out_tiff(outfile)
        if return_code != 0:
            if os.path.isfile(outfile):
                os.remove(outfile)

        # write file, so we know this ts has been succesfully processed
        #if return_code == 0:
        #    with open(str(check_file), 'w') as file:
        #        file.write('passed all tests \n')
