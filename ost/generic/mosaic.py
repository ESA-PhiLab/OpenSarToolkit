# -*- coding: utf-8 -*-
import os
from os.path import join as opj
import gdal
import numpy as np
import rasterio
from ost.helpers import vector as vec
from ost.helpers import helpers as h


def mosaic_to_vrt(ts_dir, product, outfiles):
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
    if type(outfiles) == str:
        outfiles = outfiles.replace("'", '').strip('][').split(', ')

    gdal.BuildVRT(opj(ts_dir, '{}.Timeseries.vrt'.format(product)),
                  outfiles,
                  options=vrt_options)


def mosaic(filelist, outfile, temp_dir, cut_to_aoi=False, ncores=os.cpu_count()):
    if type(cut_to_aoi)==str:
        if cut_to_aoi == 'False':
            cut_to_aoi=False

    if type(filelist) == list:
        filelist=' '.join([str(elem) for elem in filelist])
    check_file = opj(
        os.path.dirname(outfile), '.{}.processed'.format(os.path.basename(outfile)[:-4])
    )
    
    logfile = opj(
        os.path.dirname(outfile), '{}.errLog'.format(os.path.basename(outfile)[:-4])
    )
        
    with rasterio.open(filelist.replace("'", '').replace(",", '').strip('][').split(' ')[0]) as src:
        dtype = src.meta['dtype']
        dtype = 'float' if dtype == 'float32' else dtype
        
    if cut_to_aoi:
        tempfile = opj(temp_dir, os.path.basename(outfile))
    else: 
        tempfile = outfile
    cmd = ('otbcli_Mosaic -ram 4096'
                    ' -progress 1'
                    ' -comp.feather large'
                    ' -harmo.method band'
                    ' -harmo.cost rmse'
                    ' -tmpdir {}'
                    ' -il {}'
                    ' -out {} {}'.format(temp_dir, filelist.replace("'", '').replace(",", '').strip(']['),
                                         tempfile, dtype)
    )

    return_code = h.run_command(cmd, logfile)
    if return_code != 0:
        if os.path.isfile(tempfile):
            os.remove(tempfile)

        return

    if cut_to_aoi:
        
        # get aoi ina way rasterio wants it
        features = vec.gdf_to_json_geometry(vec.wkt_to_gdf(cut_to_aoi))
        
         # import raster and mask
        with rasterio.open(tempfile) as src:
            out_image, out_transform = rasterio.mask.mask(src, features, crop=True)
            out_meta = src.meta.copy()
            ndv = src.nodata
            out_image = np.ma.masked_where(out_image == ndv, out_image)
        
        out_meta.update({'driver': 'GTiff', 'height': out_image.shape[1],
                         'width': out_image.shape[2], 'transform': out_transform,
                         'tiled': True, 'blockxsize': 128, 'blockysize': 128})
        
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
    if return_code == 0:
        with open(str(check_file), 'w') as file:
            file.write('passed all tests \n')  