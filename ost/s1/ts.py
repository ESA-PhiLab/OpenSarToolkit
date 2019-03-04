
# import stdlib modules
import os
import sys
import glob
import gdal
import pkg_resources
import rasterio
import numpy as np
from ost.helpers import raster as ras
from datetime import datetime



from ost.helpers import helpers

# get the SNAP CL executable
global gpt_file
gpt_file = helpers.getGPT()
# define the resource package for getting the xml workflow files
global package
package = 'ost'


def createStackPol(fileList, polarisation, outStack, logFile, wkt=None):
    '''

    :param fileList: list of single Files (space separated)
    :param outFile: the stack that is generated
    :return:
    '''

    if wkt is None:
        graph = ('/'.join(('graphs', 'S1_TS', '1_BS_Stacking.xml')))
        graph = pkg_resources.resource_filename(package, graph)

        print(" INFO: Creating multi-temporal stack of images")
        stackCmd = '{} {} -x -q {} -Pfilelist={} -Ppol={} \
               -Poutput={}'.format(gpt_file, graph, os.cpu_count(),
                                   fileList, polarisation, outStack)
    else:
        # does not work with gpt at the moment
        graph = ('/'.join(('graphs', 'S1_TS', '1_BS_Stacking_Subset.xml')))
        graph = pkg_resources.resource_filename(package, graph)

        print(" INFO: Creating multi-temporal stack of images")
        stackCmd = '{} {} -x -q {} -Pfilelist={} -Ppol={} \
               -Pwkt=\'{}\' -Poutput={}'.format(gpt_file, graph,
                                                os.cpu_count(), fileList,
                                                polarisation, wkt, outStack)
                                            
    #print(stackCmd)
    rc = helpers.runCmd(stackCmd, logFile)

    if rc == 0:
        print(' INFO: Succesfully created multi-temporal stack')
    else:
        print(' ERROR: Stack creation exited with an error.'
              ' See {} for Snap Error output'.format(logFile))
        sys.exit(201)


def mtSpeckle(inStack, outStack, logFile):
    """

    :param inStack:
    :param outStack:
    :return:
    """

    graph = ('/'.join(('graphs', 'S1_TS', '2_MT_Speckle.xml')))
    graph = pkg_resources.resource_filename(package, graph)

    print(" INFO: Applying the multi-temporal speckle-filtering")
    mtSpkFltCmd = '{} {} -x -q {} -Pinput={} \
                   -Poutput={}'.format(gpt_file, graph, os.cpu_count(),
                                       inStack, outStack)

    rc = helpers.runCmd(mtSpkFltCmd, logFile)

    if rc == 0:
        print(' INFO: Succesfully applied multi-temporal speckle filtering')
    else:
        print(' ERROR: Multi-temporal speckle filtering exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(202)


# calculate multi-temporal metrics by looping throuch chunks defined by blocksize
def mtMetrics(rasterfn, newRasterfn, metrics, geoDict, toPower=True, rescale=True, outlier=True):

    raster3d = gdal.Open(rasterfn)

    # loop through y direction
    for y in range(0, geoDict['rows'], geoDict['yB']):
        if y + geoDict['yB'] < geoDict['rows']:
            ysize = geoDict['yB']
        else:
            ysize = geoDict['rows'] - y

        # loop throug x direction
        for x in range(0, geoDict['cols'], geoDict['xB']):
            if x + geoDict['xB'] < geoDict['cols']:
                xsize = geoDict['xB']
            else:
                xsize = geoDict['cols'] - x

            # create the blocksized array
            stacked_array=np.empty((raster3d.RasterCount, ysize, xsize), dtype=geoDict['dTypeName'])

            # loop through the timeseries and fill the stacked array part
            for i in range(raster3d.RasterCount):
                i += 0
                stacked_array[i,:,:] = np.array(raster3d.GetRasterBand(i+1).ReadAsArray(x,y,xsize,ysize))

            # take care of nans
            where_are_NaNs = np.isnan(stacked_array)
            stacked_array[where_are_NaNs] = geoDict['ndv']

            # original nd_mask
            nd_mask = stacked_array[1,:,:] == geoDict['ndv']

            # rescale to db if data comes in compressed integer format
            if rescale is True and geoDict['dTypeName'] != 'Float32':
                stacked_array = ras.rescale2Float(stacked_array, geoDict['dTypeName'])

            # convert from dB to power
            if toPower is True:
                stacked_array = ras.convert2Pow(stacked_array)

            # remove outliers
            if outlier is True and raster3d.RasterCount >= 5:
                stacked_array = ras.outlierRemoval(stacked_array)

            if 'avg' in metrics:
                # calulate the mean
                metric = np.mean(stacked_array, axis=0)

                # rescale to db
                if toPower is True:
                    metric = ras.convert2DB(metric)

                # rescale to actual data type
                if rescale is True and geoDict['dTypeName'] != 'Float32':
                    metric = ras.scale2Int(metric, -30., 5., geoDict['dTypeName'])
                    metric[nd_mask == True] = geoDict['ndv']

                # write out to raster
                ras.chunk2Raster(newRasterfn + ".avg.tif", metric, geoDict['ndv'], x, y, 1)

            if 'max' in metrics:
                # calulate the max
                metric = np.max(stacked_array, axis=0)

                # rescale to db
                if toPower is True:
                    metric = ras.convert2DB(metric)

                # rescale to actual data type
                if rescale is True and geoDict['dTypeName'] != 'Float32':
                    metric = ras.scale2Int(metric,-30. ,5. , geoDict['dTypeName'])
                    metric[nd_mask == True] = geoDict['ndv']

                # write out to raster
                ras.chunk2Raster(newRasterfn + ".max.tif", metric, geoDict['ndv'], x, y, 1)

            if 'min' in metrics:
                # calulate the max
                metric = np.min(stacked_array, axis=0)

                # rescale to db
                if toPower is True:
                    metric = ras.convert2DB(metric)

                # rescale to actual data type
                if rescale is True and geoDict['dTypeName'] != 'Float32':
                    metric = ras.scale2Int(metric,-30. ,5. , geoDict['dTypeName'])
                    metric[nd_mask == True] = geoDict['ndv']

                # write out to raster
                ras.chunk2Raster(newRasterfn + ".min.tif", metric, geoDict['ndv'], x, y, 1)

            if 'std' in metrics:
                # calulate the max
                metric = np.std(stacked_array, axis=0)

                # we do not rescale to dB for the standard deviation

                # rescale to actual data type
                if rescale is True and geoDict['dTypeName'] != 'Float32':
                    metric = ras.scale2Int(metric, 0.000001, 0.5, geoDict['dTypeName']) + 1 # we add 1 to avoid no false no data values
                    metric[nd_mask == True] = geoDict['ndv']

                # write out to raster
                ras.chunk2Raster(newRasterfn + ".std.tif", metric, geoDict['ndv'], x, y, 1)

            # Coefficient of Variation (aka amplitude dispersion)
            if 'cov' in metrics:
                # calulate the max
                
                #metric = scipy.stats.variation(stacked_array, axis=0)
                cv =  lambda x: np.std(x) / np.mean(x)
                metric = np.apply_along_axis(cv, axis=0, arr=stacked_array)
                # we do not rescale to dB for the CoV

                # rescale to actual data type
                if rescale is True and geoDict['dTypeName'] != 'Float32':
                    metric = ras.scale2Int(metric, 0.001, 1. , geoDict['dTypeName']) + 1 # we add 1 to avoid no false no data values
                    metric[nd_mask == True] = geoDict['ndv']

                # write out to raster
                ras.chunk2Raster(newRasterfn + ".cov.tif", metric, geoDict['ndv'], x, y, 1)

            # 90th percentile
            if 'p90' in metrics:
                # calulate the max
                metric = np.percentile(stacked_array, 90, axis=0)

                # rescale to db
                if toPower is True:
                    metric = ras.convert2DB(metric)

                # rescale to actual data type
                if rescale is True and geoDict['dTypeName'] != 'Float32':
                    metric = ras.scale2Int(metric,-30. ,5. , geoDict['dTypeName'])
                    metric[nd_mask == True] = geoDict['ndv']

                # write out to raster
                ras.chunk2Raster(newRasterfn + ".p90.tif", metric, geoDict['ndv'], x, y, 1)

            # 10th perentile
            if 'p10' in metrics:
                # calulate the max
                metric = np.percentile(stacked_array, 10, axis=0)

                # rescale to db
                if toPower is True:
                    metric = ras.convert2DB(metric)

                # rescale to actual data type
                if rescale is True and geoDict['dTypeName'] != 'Float32':
                    metric = ras.scale2Int(metric,-30. ,5. , geoDict['dTypeName'])
                    metric[nd_mask == True] = geoDict['ndv']

                # write out to raster
                ras.chunk2Raster(newRasterfn + ".p10.tif", metric, geoDict['ndv'], x, y, 1)

            # Difference between 90th and 10th percentile
            if 'pDiff' in metrics:
                # calulate the max
                metric = np.subtract(np.percentile(stacked_array, 90, axis=0), np.percentile(stacked_array, 10, axis=0))

                # rescale to actual data type
                if rescale is True and geoDict['dTypeName'] != 'Float32':
                    metric = ras.scale2Int(metric, 0.001, 1. , geoDict['dTypeName']) + 1 # we add 1 to avoid no false no data values
                    metric[nd_mask == True] = geoDict['ndv']

                # write out to raster
                ras.chunk2Raster(newRasterfn + ".pDiff.tif", metric, geoDict['ndv'], x, y, 1)

            # # Difference between 90th and 10th percentile
            if 'sum' in metrics:
                # calulate the max
                metric = np.sum(np.percentile(stacked_array, 90, axis=0), np.percentile(stacked_array, 10, axis=0))

                if rescale is True and geoDict['dTypeName'] != 'Float32':
                    # rescale to actual data type
                    metric = ras.scale2Int(metric,-0.0001 ,1. , geoDict['dTypeName'])
                    metric[nd_mask == True] = geoDict['ndv']

                # write out to raster
                ras.chunk2Raster(newRasterfn + ".sum.tif", metric, geoDict['ndv'], x, y, 1)


def mtMetricsMain(rasterfn, newRasterfn, metrics, toPower, rescale, outlier):

    # read the input raster and get the geo information
    geoDict = ras.readFile(rasterfn)

    # we create our empty output files
    print(" INFO: Creating output files.")
    for metric in metrics:
        
        ras.createFile('{}.{}.tif'.format(newRasterfn, metric), geoDict, 1, 'None')
        
    print(" INFO: Calculating the multi-temporal metrics and write them to the respective output files.")
    # calculate the multi temporal metrics by looping over blocksize
    mtMetrics(rasterfn, newRasterfn, metrics, geoDict, toPower, rescale, outlier)
    

def createDateList(tsPath):

    files = glob.glob('{}/*VV*tif'.format(tsPath))
    dates = sorted([os.path.basename(file).split('.')[1] for file in files])
    #outDates = [datetime.strftime(datetime.strptime(date,  '%y%m%d'), '%Y-%m-%d') ]
    f = open('{}/datelist.txt'.format(tsPath), 'w')
    for date in dates:
        f.write(str(datetime.strftime(datetime.strptime(date,  '%y%m%d'), '%Y-%m-%d')) + ' \n')        
    f.close()
    
    
def createTsAnimation(tsFolder, tmpDir, outFile, percSize):

    for file in sorted(glob.glob('{}/*VV.tif'.format(tsFolder))):

        nr = os.path.basename(file).split('.')[0]
        date = os.path.basename(file).split('.')[1]
        fileVV = file
        fileVH = glob.glob('{}/{}.*VH.tif'.format(tsFolder, nr))[0]

        outTmp = '{}/{}.jpg'.format(tmpDir, date)

        with rasterio.open(fileVV) as vv:
            outMeta = vv.meta.copy()
            # create empty array
            arr = np.zeros((int(outMeta['height']), int(outMeta['width']), int(3)))
            # read vv array
            arr[:,:,0] = vv.read()

        with rasterio.open(fileVH) as vh:
            # read vh array
            arr[:,:,1] = vh.read()

        arr[:,:,2] = np.subtract(arr[:,:,0], arr[:,:,1])

        # rescale to uint8
        arr[:,:,0] = ras.scale2Int(arr[:,:,0], -20., 0., 'uint8')
        arr[:,:,1] = ras.scale2Int(arr[:,:,1], -25., -5., 'uint8')
        arr[:,:,2] = ras.scale2Int(arr[:,:,2], 1., 15., 'uint8')

        # update outfile's metadata
        outMeta.update({'driver': 'JPEG', 'height': int(outMeta['height']),
                        'width': int(outMeta['width']), 'transform': outMeta['transform'],
                        'nodata': outMeta['nodata'], 'dtype': 'uint8', 'count': 3})

        # transpose array to gdal format
        arr = np.transpose(arr, [2, 0, 1])

        # write array to disk
        with rasterio.open(outTmp, 'w', **outMeta) as out:
            out.write(arr.astype('uint8'))

        # add date
        heightLabel=np.floor(np.divide(int(outMeta['height']), 15))
        cmd = 'convert -background \'#0008\' -fill white -gravity center -size {}x{} caption:\"{}\" \
        {} +swap -gravity north -composite {}'.format(outMeta['width'], heightLabel, date, outTmp, outTmp)
        os.system(cmd)

    # create gif
    listOfFiles = ' '.join(sorted(glob.glob('{}/*jpg'.format(tmpDir))))
    cmd = 'convert -delay 200 -loop 20 {} {}'.format(listOfFiles, outFile)
    os.system(cmd)

    for file in glob.glob('{}/*jpg'.format(tmpDir)):
        os.remove(file)
        
    
    