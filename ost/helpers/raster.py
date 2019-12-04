import os
from os.path import join as opj
import numpy as np
import glob
import logging

import gdal
import osr
import ogr
import fiona
import imageio
import rasterio
import rasterio.mask
from rasterio.features import shapes

from ost.helpers import helpers as h

logger = logging.getLogger(__name__)


def read_file(rasterfn):

    # open raster file
    raster = gdal.Open(rasterfn)

    # Get blocksizes for iterating over tiles (chuuks)
    my_block_size = raster.GetRasterBand(1).GetBlockSize()
    x_block_size = my_block_size[0]
    y_block_size = my_block_size[1]

    # Get image sizes
    cols = raster.RasterXSize
    rows = raster.RasterYSize
    bands = raster.RasterCount

    # get datatype and transform to numpy readable
    data_type = raster.GetRasterBand(1).DataType
    data_type_name = gdal.GetDataTypeName(data_type)

    if data_type_name == "Byte":
        data_type_name = "uint8"

    logger.debug('INFO: Importing {} bands from {}'.format(raster.RasterCount,
                                                     rasterfn))

    geotransform = raster.GetGeoTransform()
    origin_x = geotransform[0]
    origin_y = geotransform[3]
    pixel_width = geotransform[1]
    pixel_height = geotransform[5]
    driver = gdal.GetDriverByName('GTiff')  # critical!!!!!!!!!!!!!!!!!!!!!!!
    ndv = raster.GetRasterBand(1).GetNoDataValue()

    # we need this for file creation
    outraster_srs = osr.SpatialReference()
    outraster_srs.ImportFromWkt(raster.GetProjectionRef())

    # we return a dict of all relevant values
    return {'xB': x_block_size, 'yB': y_block_size, 'cols': cols, 'rows': rows,
            'bands': bands, 'dType': data_type, 'dTypeName': data_type_name,
            'ndv': ndv, 'gtr': geotransform, 'oX': origin_x, 'oY': origin_y,
            'pW': pixel_width, 'pH': pixel_height, 'driver': driver,
            'outR': outraster_srs}


def create_file(newraster, geodict, bands, compression='None'):

    blocksize_x = 'BLOCKXSIZE={}'.format(geodict['xB'])
    blocksize_y = 'BLOCKYSIZE={}'.format(geodict['yB'])
    comp = 'COMPRESS={}'.format(compression)
    if blocksize_y == blocksize_x:
        tiled = 'YES'
    else:
        tiled = 'NO'

    if compression is None:
        opts = ['TILED={}'.format(tiled), 'BIGTIFF=IF_SAFER',
                blocksize_x, blocksize_y]
    else:
        opts = ['TILED={}'.format(tiled), 'BIGTIFF=IF_SAFER',
                blocksize_x, blocksize_y, comp]

    outraster = geodict['driver'].Create(newraster, geodict['cols'],
                                         geodict['rows'], bands,
                                         geodict['dType'], options=opts)

    outraster.SetGeoTransform((geodict['oX'],
                               geodict['pW'],
                               0,
                               geodict['oY'],
                               0,
                               geodict['pH']))

    outraster.SetProjection(geodict['outR'].ExportToWkt())

    if geodict['ndv'] is not None:
        outraster.GetRasterBand(1).SetNoDataValue(geodict['ndv'])

    return outraster


# write chunks of arrays to an already existent raster
def chunk_to_raster(outraster, array_chunk, ndv, x_pos, y_pos, z_pos):

    outraster = gdal.Open(outraster, gdal.GA_Update)
    outband = outraster.GetRasterBand(z_pos)

    # write to array
    outband.WriteArray(array_chunk, x_pos, y_pos, z_pos)


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


def polygonize_raster(infile, outfile, mask_value=1, driver='ESRI Shapefile'):

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
                    'geometry': 'Polygon'}) as dst:

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
        with rasterio.open(
                '{}.tif'.format(outfile[:-4]), 'w', **meta) as out_min:

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
    polygonize_raster('{}.tif'.format(outfile[:-4]), outfile)
    os.remove('{}.tif'.format(outfile[:-4]))


def polygonize_to_shape(inraster, out_shape, out_epsg=4326, mask=None):
    """
    This function takes an input raster and polygonizes it.

    :param raster: input raster file
    :param mask: mask file
    :param out_shape: output shapefile
    :param srs: spatial reference system in EPSG code
    :return:
    """

    raster = gdal.Open(inraster)
    src_band = raster.GetRasterBand(1)

    if mask is not None:
        mask_file = gdal.Open(mask)
        mask_band = mask_file.GetRasterBand(1)
    else:
        mask_band = src_band

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(out_epsg)

    dst_layername = out_shape
    drv = ogr.GetDriverByName("ESRI Shapefile")
    dstfile = drv.CreateDataSource(dst_layername)
    dst_layer = dstfile.CreateLayer(dst_layername, srs=srs)
    gdal.Polygonize(src_band, mask_band, dst_layer, -1, [], callback=None)


# convert dB to power
def convert_to_power(db_array):

    pow_array = 10 ** (db_array / 10)
    return pow_array


# convert power to dB
def convert_to_db(pow_array):

    pow_array[pow_array < 0] = 0.0000001
    db_array = 10 * np.log10(pow_array.clip(min=0.0000000000001))
    return db_array


# rescale sar dB dat ot integer format
def scale_to_int(float_array, min_value, max_value, datatype):

    # set output min and max
    display_min = 1.
    if datatype == 'uint8':
        display_max = 255.
    elif datatype == 'uint16':
        display_max = 65535.

    a = min_value - ((max_value - min_value)/(display_max - display_min))
    x = (max_value - min_value)/(display_max - 1)

    float_array[float_array > max_value] = max_value
    float_array[float_array < min_value] = min_value

    int_array = np.round((float_array - a) / x).astype(datatype)

    return int_array


# rescale integer scaled sar data back to dB
def rescale_to_float(int_array, data_type_name):

    if data_type_name == 'uint8':
        float_array = (int_array.astype(float)
                       * (35. / 254.) + (-30. - (35. / 254.)))
    elif data_type_name == 'uint16':
        float_array = (int_array.astype(float)
                       * (35. / 65535.) + (-30. - (35. / 65535.)))
    else:
        logger.debug('ERROR: Unknown datatype')

    return float_array


def mask_by_shape(infile, outfile, shapefile, to_db=False, datatype='float32',
                  rescale=True, min_value=0.000001, max_value=1, ndv=None):

    # import shapefile geometries
    with fiona.open(shapefile, 'r') as file:
        features = [feature['geometry'] for feature in file
                    if feature['geometry']]

    # import raster
    with rasterio.open(infile) as src:
        out_image, out_transform = rasterio.mask.mask(src, features, crop=True)
        out_meta = src.meta.copy()
        out_image = np.ma.masked_where(out_image == ndv, out_image)

    # if to decibel should be applied
    if to_db is True:
        out_image = convert_to_db(out_image)

    if rescale:
        # if we scale to another d
        if datatype != 'float32':

            if datatype == 'uint8':
                out_image = scale_to_int(out_image, min_value, max_value, 'uint8')
            elif datatype == 'uint16':
                out_image = scale_to_int(out_image, min_value, max_value, 'uint16')

    out_meta.update({'driver': 'GTiff', 'height': out_image.shape[1],
                     'width': out_image.shape[2], 'transform': out_transform,
                     'nodata': ndv, 'dtype': datatype, 'tiled': True,
                     'blockxsize': 128, 'blockysize': 128})

    with rasterio.open(outfile, 'w', **out_meta) as dest:
        dest.write(out_image.filled(ndv))


# the outlier removal, needs revision (e.g. use something profound)
def outlier_removal(arrayin, stddev=3):

    # calculate percentiles
    perc95 = np.percentile(arrayin, 95, axis=0)
    perc5 = np.percentile(arrayin, 5, axis=0)

    # we mask out the percetile outliers for std dev calculation
    masked_array = np.ma.MaskedArray(
        arrayin,
        mask=np.logical_or(
            arrayin > perc95,
            arrayin < perc5
            )
        )

    # we calculate new std and mean
    masked_std = np.std(masked_array, axis=0)
    masked_mean = np.mean(masked_array, axis=0)

    # we mask based on mean +- 3 * stddev
    array_out = np.ma.MaskedArray(
        arrayin,
        mask=np.logical_or(
            arrayin > masked_mean + masked_std * stddev,
            arrayin < masked_mean - masked_std * stddev,
            )
        )

    return array_out


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

    mins = {'BS.VV': -20, 'BS.VH': -25, 'BS.HH': -20, 'BS.HV': -25,
            'coh.VV': 0.1, 'coh.VH': 0.1,
            'Alpha': 60, 'Entropy': 0.1, 'Anisotropy': 0.1}

    for key, items in mins.items():
        if key in file:
            return items


def get_max(file):

    maxs = {'BS.VV': 0, 'BS.VH': -12, 'BS.HH': 0, 'BS.HV': -5,
            'coh.VV': 0.8, 'coh.VH': 0.75,
            'Alpha': 80, 'Entropy': 0.8, 'Anisotropy': 0.8}

    for key, items in maxs.items():
        if key in file:
            return items


def create_rgb_jpeg(filelist, outfile=None, shrink_factor=1, plot=False,
                   minimum_list=None, maximum_list=None, date=None):

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
                resampling=5    # 5 = average
                )[0]
        minimum_list.append(get_min(filelist[0]))
        maximum_list.append(get_max(filelist[0]))
        layer1[layer1 == 0] = np.nan
        
    if len(filelist) > 1:
        with rasterio.open(filelist[1]) as src:
            layer2 = src.read(
                    out_shape=(src.count, new_height, new_width),
                    resampling=5    # 5 = average
                    )[0]
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
                    resampling=5    # 5 = average
                    )[0]
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
    out_meta.update({'driver': 'JPEG',
                     'dtype': 'uint8',
                     'count': count})

    
    if outfile:    # write array to disk
        with rasterio.open(outfile, 'w', **out_meta) as out:
            out.write(arr.astype('uint8'))
            
        if date:
            label_height = np.floor(np.divide(int(out_meta['height']), 15))
            cmd = 'convert -background \'#0008\'-fill white -gravity center \
                  -size {}x{} caption:\"{}\"{} +swap -gravity north \
                  -composite {}'.format(out_meta['width'], label_height,
                                        date, outfile, outfile)
            h.run_command(cmd, '{}.log'.format(outfile), elapsed=False)
            
    if plot:
        plt.imshow(arr)

    
def create_timeseries_animation(timeseries_folder, product_list, out_folder,
                                shrink_factor=1, duration=1, add_dates=False):

    
    nr_of_products = len(glob.glob(
        opj(timeseries_folder, '*{}.tif'.format(product_list[0]))))
    outfiles = []
    # for coherence it must be one less
    if 'coh.VV'in product_list or 'coh.VH'in product_list:
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
                        date=date)

        outfiles.append(opj(out_folder, '{}.{}.jpeg'.format(i+1, dates)))

    # create gif
    with imageio.get_writer(opj(out_folder, 'ts_animation.gif'), mode='I',
        duration=duration) as writer:

        for file in outfiles:
            image = imageio.imread(file)
            writer.append_data(image)
            os.remove(file)
            if os.path.isfile(file + '.aux.xml'):
                os.remove(file + '.aux.xml')
