#! /usr/bin/env python
"""
This script provides wrapper functions for processing Sentinel-1 GRD products.
"""

# import stdlib modules
import os
from os.path import join as opj
import numpy as np
import json
import glob
import itertools
from pathlib import Path

# geo libs
import gdal
import fiona
import imageio
import rasterio
import rasterio.mask
from rasterio.features import shapes

from ost.helpers import helpers as h

# script infos
__author__ = 'Andreas Vollrath'
__copyright__ = 'phi-lab, European Space Agency'

__license__ = 'GPL'
__version__ = '1.0'
__maintainer__ = 'Andreas Vollrath'
__email__ = ''
__status__ = 'Production'



def replace_value(rasterfn, value_to_replace, new_value):

    # open raster file
    raster = gdal.Open(rasterfn, gdal.GA_Update)

    # Get blocksizes for iterating over tiles (chuuks)
    my_block_size = raster.GetRasterBand(1).GetBlockSize()
    x_block_size = my_block_size[0]
    y_block_size = my_block_size[1]

    # Get image sizes
    cols = raster.RasterXSize
    rows = raster.RasterYSize

    # loop through y direction
    for y in range(0, rows, y_block_size):
        if y + y_block_size < rows:
            ysize = y_block_size
        else:
            ysize = rows - y

        # loop throug x direction
        for x in range(0, cols, x_block_size):
            if x + x_block_size < cols:
                xsize = x_block_size
            else:
                xsize = cols - x

            raster_array = np.array(raster.GetRasterBand(1).ReadAsArray(
                x, y, xsize, ysize))
            raster_array[raster_array <= np.float32(value_to_replace)] = np.float32(
                new_value)

            raster.GetRasterBand(1).WriteArray(raster_array, x, y)


def polygonize_raster(infile, outfile, mask_value=1, driver='GPKG'):

    with rasterio.open(infile) as src:

        image = src.read(1)

        if mask_value is not None:
            mask = image == mask_value
        else:
            mask = None

        results = (
            {'properties': {'raster_val': v}, 'geometry': s}
            for i, (s, v)
            in enumerate(
                shapes(image, mask=mask, transform=src.transform)))

        with fiona.open(
            outfile, 'w',
            driver=driver,
            crs=src.crs,
            schema={'properties': [('raster_val', 'int')],
                    'geometry': 'Polygon'}
        ) as dst:
            dst.writerecords(results)


def outline(infile, outfile, ndv=0, less_then=False):
    '''
    This function returns the valid areas (i.e. non no-data areas) of a
    raster file as a shapefile.

    :param infile: input raster file
    :param outfile: output shapefile
    :param ndv: no data value of the input raster
    :return:
    '''

    with rasterio.open(infile) as src:

        # get metadata
        meta = src.meta

        # update driver, datatype and reduced band count
        meta.update(driver='GTiff', dtype='uint8', count=1)
        # we update the meta for more efficient looping due to
        # hardcoded vrt blocksizes
        meta.update(blockxsize=src.shape[1], blockysize=1)

        # create outfiles
        with rasterio.open(f'{outfile.stem}.tif', 'w', **meta) as out_min:

            # loop through blocks
            for _, window in out_min.block_windows(1):

                # read array with all bands
                stack = src.read(range(1, src.count + 1), window=window)

                # get stats
                min_array = np.nanmin(stack, axis=0)

                if less_then is True:
                    min_array[min_array <= ndv] = 0
                else:
                    min_array[min_array == ndv] = 0

                min_array[min_array != ndv] = 1

                # write to dest
                out_min.write(np.uint8(min_array), window=window, indexes=1)

    # now let's polygonize
    polygonize_raster(f'{outfile.stem}.tif', outfile)
    Path(f'{outfile.stem}.tif').unlink()


# convert power to dB
def convert_to_db(pow_array):

    # assure all values are positive (strangely that's not always the case)
    pow_array[pow_array < 0] = 0.0000001

    # convert to dB
    db_array = np.multiply(10, np.log10(pow_array.clip(min=0.0000000000001)))

    # return
    return db_array


# rescale sar dB dat ot integer format
def scale_to_int(float_array, min_value, max_value, datatype):

    # set output min and max
    display_min = 1.
    if datatype == 'uint8':
        display_max = 255.
    elif datatype == 'uint16':
        display_max = 65535.

    a = min_value - ((max_value - min_value) / (display_max - display_min))
    x = (max_value - min_value) / (display_max - 1)

    # float_array[float_array == 0.0] = np.nan
    float_array[float_array > max_value] = max_value
    float_array[float_array < min_value] = min_value

    stretched = np.divide(np.subtract(float_array, a), x)
    int_array = np.round(np.nan_to_num(stretched)).astype(datatype)

    return int_array


# rescale integer scaled sar data back to dB
def rescale_to_float(int_array, data_type_name):
    int_array = int_array.astype('float32')
    int_array[int_array == 0] = np.nan

    if data_type_name == 'uint8':
        a = np.divide(35., 254.)
        b = np.subtract(-30., a)
    elif data_type_name == 'uint16':
        a = np.divide(35., 65535.)
        b = np.subtract(-30., a)
    else:
        raise TypeError('Unknown datatype')

    return np.add(np.multiply(int_array, a), b)


def mask_by_shape(infile, outfile, shapefile, to_db=False, datatype='float32',
                  rescale=True, min_value=0.000001, max_value=1, ndv=None,
                  description=True):

    # import shapefile geometries
    with fiona.open(shapefile, 'r') as file:
        features = [feature['geometry'] for feature in file
                    if feature['geometry']]

    # import raster
    with rasterio.open(infile) as src:
        out_image, out_transform = rasterio.mask.mask(src, features, crop=True)
        out_meta = src.meta.copy()
        out_image = np.ma.masked_where(out_image == ndv, out_image)

    # unmask array
    out_image = out_image.data
    out_image[out_image == 0] = np.nan
    # if to decibel should be applied
    if to_db is True:
        out_image = convert_to_db(out_image)

    if rescale:

        if datatype == 'uint8':
            out_image = scale_to_int(out_image, min_value, max_value, 'uint8')
        elif datatype == 'uint16':
            out_image = scale_to_int(out_image, min_value, max_value, 'uint16')

    out_meta.update({'driver': 'GTiff', 'height': out_image.shape[1],
                     'width': out_image.shape[2], 'transform': out_transform,
                     'nodata': ndv, 'dtype': datatype, 'tiled': True,
                     'blockxsize': 128, 'blockysize': 128})

    with rasterio.open(outfile, 'w', **out_meta) as dest:
        dest.write(np.nan_to_num(out_image))

        if description:
            dest.update_tags(1,
                    BAND_NAME='{}'.format(os.path.basename(infile)[:-4]))
            dest.set_band_description(1,
                    '{}'.format(os.path.basename(infile)[:-4]))


def create_tscan_vrt(list_of_args):

    timescan_dir, project_file = list_of_args

    # load ard parameters
    with open(project_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing']
        ard_tscan = ard_params['time-scan_ARD']

    # loop through all potential products
    # a products list
    product_list = ['bs.HH', 'bs.VV', 'bs.HV', 'bs.VH',
                    'coh.VV', 'coh.VH', 'coh.HH', 'coh.HV', 
                    'pol.Entropy', 'pol.Anisotropy', 'pol.Alpha']
    
    i, outfiles = 0, []
    iteration = itertools.product(product_list, ard_tscan['metrics'])
    for product, metric in iteration:

        # get file and add number for outfile
        infile = timescan_dir.joinpath(f'{product}.{metric}.tif')

        # if there is no file sto the iteration
        if not infile.exists():
            continue

        i += 1
        # create namespace for output file and add to list for vrt creation
        outfile = timescan_dir.joinpath(f'{i:02d}.{product}.{metric}.tif')
        outfiles.append(str(outfile))

        # otherwise rename the file
        infile.replace(outfile)

    # build vrt
    gdal.BuildVRT(
        str(timescan_dir.joinpath('Timescan.vrt')),
        outfiles,
        options=gdal.BuildVRTOptions(srcNodata=0, separate=True)
     )
        

def norm(band, percentile=False):

    if percentile:
        band_min, band_max = np.percentile(band, 2), np.percentile(band, 98)
    else:
        band_min, band_max = np.nanmin(band), np.nanmax(band)
        
    return (band - band_min)/(band_max - band_min)


def visualise_rgb(filepath, shrink_factor=25):

    import matplotlib.pyplot as plt

    with rasterio.open(filepath) as src:
        array = src.read(
                out_shape=(src.count, int(src.height / shrink_factor),
                           int(src.width / shrink_factor)),
                resampling=5    # 5 = average
                )

    array[array == 0] = np.nan
    red = norm(scale_to_int(array[0], -18, 0, 'uint8'))
    green = norm(scale_to_int(array[1], -25, -5, 'uint8'))
    blue = norm(scale_to_int(array[2], 1, 15, 'uint8'))
    img = np.dstack((red, green, blue))
    img[img == 0] = np.nan
    plt.imshow(img)


def get_min(file):

    mins = {'bs.VV': -20, 'bs.VH': -25, 'bs.HH': -20, 'bs.HV': -25,
            'coh.VV': 0.1, 'coh.VH': 0.1,
            'pol.Alpha': 60, 'pol.Entropy': 0.1, 'pol.Anisotropy': 0.1,
            'coh_IW1_VV': 0.1, 'coh_IW2_VV': 0.1, 'coh_IW3_VV': 0.1,
            'coh_IW1_VH': 0.1, 'coh_IW2_VH': 0.1, 'coh_IW3_VH': 0.1}

    for key, items in mins.items():
        if key in file:
            return items

def get_max(file):

    maxs = {'bs.VV': 0, 'bs.VH': -12, 'bs.HH': 0, 'bs.HV': -5,
            'coh.VV': 0.8, 'coh.VH': 0.75,
            'pol.Alpha': 80, 'pol.Entropy': 0.8, 'pol.Anisotropy': 0.8,
            'coh_IW1_VV': 0.8, 'coh_IW2_VV': 0.8, 'coh_IW3_VV': 0.8,
            'coh_IW1_VH': 0.75, 'coh_IW2_VH': 0.75, 'coh_IW3_VH': 0.75}

    for key, items in maxs.items():
        if key in file:
            return items


def calc_min(band, stretch='minmax'):

    if stretch == 'percentile':
        band_min = np.percentile(band, 2)
    elif stretch == 'minmax':
        band_min = np.nanmin(band)
    else:
        print("Please select one of percentile or minmax for the stretch parameter")

    return band_min


def calc_max(band, stretch='minmax'):
    if stretch == 'percentile':
        band_max = np.percentile(band, 98)
    elif stretch == 'minmax':
        band_max = np.nanmax(band)
    else:
        print("Please select one of percentile or minmax for the stretch parameter")
    return band_max


def create_rgb_jpeg(filelist, outfile=None, shrink_factor=1, resampling_factor=5, plot=False,
                   minimum_list=None, maximum_list=None, date=None, filetype=None, stretch=False):

    import matplotlib.pyplot as plt

    minimum_list = []
    maximum_list = []

    with rasterio.open(filelist[0]) as src:
        
        # get metadata
        out_meta = src.meta.copy()

        # !!!assure that dimensions match ####
        new_height = int(src.height/shrink_factor)
        new_width = int(src.width/shrink_factor)
        out_meta.update(height=new_height, width=new_width)
        count=1
        
        layer1 = src.read(
                out_shape=(src.count, new_height, new_width),
                resampling=resampling_factor    # 5 = average
                )[0]
        if stretch:
            minimum_list.append(calc_min(layer1, stretch))
            maximum_list.append(calc_max(layer1, stretch))
        else:
            minimum_list.append(get_min(filelist[0]))
            maximum_list.append(get_max(filelist[0]))
        layer1[layer1 == 0] = np.nan
        
    if len(filelist) > 1:
        with rasterio.open(filelist[1]) as src:
            layer2 = src.read(
                    out_shape=(src.count, new_height, new_width),
                    resampling=resampling_factor    # 5 = average
                    )[0]
            if stretch:
                minimum_list.append(calc_min(layer2, stretch))
                maximum_list.append(calc_max(layer2, stretch))
            else:
                minimum_list.append(get_min(filelist[1]))
                maximum_list.append(get_max(filelist[1]))
            layer2[layer2 == 0] = np.nan
            count=3
            
    if len(filelist) == 2:    # that should be the BS ratio case
        layer3 = np.subtract(layer1, layer2)
        minimum_list.append(1)
        maximum_list.append(15)
        
    elif len(filelist) >= 3:
        # that's the full 3layer case
        with rasterio.open(filelist[2]) as src:
            layer3 = src.read(
                    out_shape=(src.count, new_height, new_width),
                    resampling=resampling_factor    # 5 = average
                    )[0]
        if stretch:
            minimum_list.append(calc_min(layer3, stretch))
            maximum_list.append(calc_max(layer3, stretch))
        else:
            minimum_list.append(get_min(filelist[2]))
            maximum_list.append(get_max(filelist[2]))
        layer3[layer3 == 0] = np.nan
    # create empty array
    arr = np.zeros((int(out_meta['height']),
                    int(out_meta['width']),
                    int(count)))
    
    arr[:, :, 0] = scale_to_int(layer1, minimum_list[0],
                                maximum_list[0], 'uint8')
    if len(filelist) > 1:
        arr[:, :, 1] = scale_to_int(layer2, minimum_list[1],
                                    maximum_list[1], 'uint8')
        arr[:, :, 2] = scale_to_int(layer3, minimum_list[2],
                                    maximum_list[2], 'uint8')
    # transpose array to gdal format
    arr = np.transpose(arr, [2, 0, 1])

    # update outfile's metadata
    if filetype:
        out_meta.update({'driver': filetype,
                         'dtype': 'uint8',
                         'count': count})
    else:
        out_meta.update({'driver': 'JPEG',
                     'dtype': 'uint8',
                     'count': count})

    
    if outfile:    # write array to disk
        with rasterio.open(outfile, 'w', **out_meta) as out:
            out.write(arr.astype('uint8'))
            
        if date:
            label_height = np.floor(np.divide(int(out_meta['height']), 15))
            cmd = 'convert -background \'#0008\' -fill white -gravity center \
                  -size {}x{} caption:\"{}\" {} +swap -gravity north \
                  -composite {}'.format(out_meta['width'], label_height,
                                        date, outfile, outfile)
            h.run_command(cmd, '{}.log'.format(outfile), elapsed=False)
            
    if plot:
        plt.imshow(arr)

    
def create_timeseries_animation(timeseries_folder, product_list, out_folder,
                                shrink_factor=1, resampling_factor=5, duration=1, add_dates=False, prefix=False):

    
    nr_of_products = len(glob.glob(
        opj(timeseries_folder, '*{}.tif'.format(product_list[0]))))
    outfiles = []
    # for coherence it must be one less
    if 'coh.VV' in product_list or 'coh.VH' in product_list:
        nr_of_products == nr_of_products - 1
        
    for i in range(nr_of_products):

        filelist = [glob.glob(opj(timeseries_folder, '{}.*{}*tif'.format(i + 1, product)))[0] for product in product_list]
        dates = os.path.basename(filelist[0]).split('.')[1]    
        
        if add_dates:
            date = dates
        else:
            date = None
        
        create_rgb_jpeg(filelist, 
                        opj(out_folder, '{}.{}.jpeg'.format(i+1, dates)),
                        shrink_factor,
                        resampling_factor,
                        date=date)

        outfiles.append(opj(out_folder, '{}.{}.jpeg'.format(i+1, dates)))

    # create gif
    if prefix:
        gif_name = '{}_{}_ts_animation.gif'.format(prefix,product_list[0])
    else:
        gif_name = '{}_ts_animation.gif'.format(product_list[0])
    with imageio.get_writer(opj(out_folder, gif_name), mode='I',
        duration=duration) as writer:

        for file in outfiles:
            image = imageio.imread(file)
            writer.append_data(image)
            os.remove(file)
            if os.path.isfile(file + '.aux.xml'):
                os.remove(file + '.aux.xml')
