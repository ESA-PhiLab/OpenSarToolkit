#! /usr/bin/env python
"""
This script provides wrapper functions of the S1 Toolbox 
for processing Sentinel-1 products.
"""

# import stdlib modules
import os
import sys
import glob
import shutil
import time
import pkg_resources
import numpy as np
import gdal

from ost.helpers import helpers, raster as ras

# script infos
__author__ = 'Andreas Vollrath'
__copyright__ = 'phi-lab, European Space Agency'
__license__ = 'GPL'
__version__ = '1.0'
__maintainer__ = 'Andreas Vollrath'
__email__ = ''
__status__ = 'Production'

# get the SNAP CL executable
global gpt_file
gpt_file = helpers.getGPT()

# define the resource package for getting the xml workflow files
global package
package = 'ost' 


def sliceAssembly(fileList, outFile, logFile, polar='VV,VH,HH,HV'):
    """
    This function assembles consecutive frames acquired at the same date. 
    Can be either GRD or SLC products

    :param fileList: a list of the frames to be assembled
    :param outFile: the assembled file
    :return:
    """

    print(" INFO: Assembling consecutive frames:")
    #print([file for file in os.path.basename(fileList)])
    sliceAssemblyCmd = '{} SliceAssembly -x -q {} -PselectedPolarisations={} \
                       -t {} {}'.format(gpt_file, os.cpu_count(), polar,
                                        outFile, fileList)

    rc = helpers.runCmd(sliceAssemblyCmd, logFile)

    if rc == 0:
        print(' INFO: Succesfully assembled products')
    else:
        print(' ERROR: Slice Assembly exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(111)


def slcBurstImport(inFile, outPrefix, logFile,
                   swath, burst, polar='VV,VH,HH,HV'):
    """
    This function imports a raw SLC file,
    applies the precise orbit file (if available)
    and splits the subswaths into separate files for further processing.
    
    :param
    :param
    :return
    """

    graph = ('/'.join(('graphs', 'S1_SLCARD', 'S1_SLC_BurstSplit_AO.xml')))
    graph = pkg_resources.resource_filename(package, graph)

    print(' INFO: Importing Burst {} from Swath {}'
          ' from scene {}'.format(burst, swath, os.path.basename(inFile)))
    
    burstImportCmd = '{} {} -x -q {} -Pinput={} -Ppolar={} -Pswath={}\
           -Pburst={} -Poutput={}'.format(gpt_file, graph, os.cpu_count(),
                                          inFile, polar, swath, burst, 
                                          outPrefix)
    
    rc = helpers.runCmd(burstImportCmd, logFile)

    if rc == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(119)
        

def slcFrameImport(inFile, outPrefix, logFile, polar='VV,VH,HH,HV'):
    """
    This function imports a raw SLC file,
    applies the precise orbit file (if available)
    and splits the subswaths into separate files for further processing.
    """

    graph = ('/'.join(('graphs', 'S1_SLC2ARD', 'S1_SLC_AO_Split.xml')))
    graph = pkg_resources.resource_filename(package, graph)

    print(' INFO: Importing {} by applying the precise orbit file and'
          ' split into the subswaths'.format(os.path.basename(inFile)))
    frameImportCmd = '{} {} -x -q {} -Pinput={} -Ppolar={} -Piw1={}_iw1 \
                      -Piw2={}_iw2 -Piw3={}_iw3'.format(
                                 gpt_file, graph, os.cpu_count(), 
                                 inFile, polar, outPrefix, outPrefix, outPrefix
                                )
         
    rc = helpers.runCmd(frameImportCmd, logFile)

    if rc == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(112)


def slcFrameImportDeb(inFile, outPrefix, logFile, polar='VV,VH,HH,HV'):
    """
    This function imports a raw SLC file,
    applies the precise orbit file (if available)
    and splits the subswaths into separate files for further processing.
    """

    graph = ('/'.join(('graphs', 'S1_SLC2ARD', 'S1_SLC_AO_Split_Deb.xml')))
    graph = pkg_resources.resource_filename(package, graph)

    print(' INFO: Importing {} by applying the precise orbit file and'
          ' split into the subswaths'.format(os.path.basename(inFile)))
    frameImportCmd = '{} {} -x -q {} -Pinput={} -Ppolar={} -Piw1={}_iw1 \
                      -Piw2={}_iw2 -Piw3={}_iw3'.format(
                                 gpt_file, graph, os.cpu_count(), 
                                 inFile, polar, outPrefix, outPrefix, outPrefix
                                )
         
    rc = helpers.runCmd(frameImportCmd, logFile)

    if rc == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(112)
        
def slcCoreg(fileList, outFile, logFile):
    """
    This function co-registers a set of Sentinel-1 images (or subswaths)
    based on the backgeocoding and the Enhanced-Spectral-Diversity (ESD).
    """

    SLC_coreg_xml = ('/'.join(('graphs', 'S1_SLC', 'S1_SLC_BGD.xml')))
    SLC_coreg_xml = pkg_resources.resource_filename(package, SLC_coreg_xml)

    print(' INFO: Co-registering {}'.format(fileList[0]))
    currtime = time.time()
    cmd = '{} {} -x -q {} -Pfilelist={} -Poutput={}'.format(gpt_file, SLC_coreg_xml, os.cpu_count(), fileList, outFile)
    os.system(cmd)
    helpers.timer(currtime)


def slcCoregESD(fileList, outFile, logFile):
    """
    This function co-registers a set of Sentinel-1 images (or subswaths)
    based on the backgeocoding and the Enhanced-Spectral-Diversity (ESD).
    """

    SLC_coreg_xml = ('/'.join(('graphs', 'S1_SLC', 'S1_SLC_BGD_ESD.xml')))
    SLC_coreg_xml = pkg_resources.resource_filename(package, SLC_coreg_xml)

    print(" INFO: Co-registering with Enhanced Spectral Diversity")
    currtime = time.time()
    cmd = '{} {} -x -q {} -Pfilelist={} -Poutput={}'.format(gpt_file, SLC_coreg_xml, os.cpu_count(), fileList, outFile)
    os.system(cmd)
    helpers.timer(currtime)


def slcBackscatter(inFile, outFile, tmpdir, logFile, prType='GTCgamma', reGrid=False):
    """
    This function is a wrapper function of the SNAP toolbox for the creation of
    radiometrically terrain corrected product.
    """

    # define path to xml file based on product type
    if prType == 'RTC':
        SLC_calibrate_xml = '/'.join(('graphs', 'S1_SLC', 'S1_SLC_TNR_Calbeta_Deb.xml'))
        SLC_calibrate_xml = pkg_resources.resource_filename(package, SLC_calibrate_xml)
        outTmpFile = '{}/beta'.format(tmpdir)
    elif prType == 'GTCgamma':
        SLC_calibrate_xml = '/'.join(('graphs', 'S1_SLC', 'S1_SLC_TNR_CalGamma_Deb.xml'))
        SLC_calibrate_xml = pkg_resources.resource_filename(package, SLC_calibrate_xml)
        outTmpFile = outFile
    elif prType == 'GTCsigma':
        SLC_calibrate_xml = '/'.join(('graphs', 'S1_SLC', 'S1_SLC_TNR_CalSigma_Deb.xml'))
        SLC_calibrate_xml = pkg_resources.resource_filename(package, SLC_calibrate_xml)
        outTmpFile = outFile

    print(" INFO: Removing thermal noise, calibrating and debursting")
    currtime = time.time()
    cmd = '{} {} -x -q {} -Pinput={} -Poutput={}'.format(gpt_file, SLC_calibrate_xml, os.cpu_count(), inFile, outTmpFile)
    os.system(cmd)
    helpers.timer(currtime)

    if prType == 'RTC':
        print(' INFO: Correcting for the illumination along slopes (Terrain Flattening).')
        currtime = time.time()
        cmd = '{} Terrain-Flattening -x -q {} -PreGridMethod={} \
              -t {} {}'.format(gpt_file, os.cpu_count(), reGrid, outFile, '{}.dim'.format(outTmpFile))
        os.system(cmd)
        helpers.timer(currtime)
        print(" Removing Tmp files")

        os.remove('{}.dim'.format(outTmpFile))
        shutil.rmtree('{}.data'.format(outTmpFile))


def slcLSMap(inFile, outFile, logFile, resol=20):
    """
    This function is a wrapper function of the SNAP toolbox for the creation of
    a layover/shadow mask.
    """

    # define path to xml file
    SLC_ls_xml = ('/'.join(('graphs', 'S1_SLC', 'S1_SLC_LS_TC.xml')))
    SLC_ls_xml = pkg_resources.resource_filename(package, SLC_ls_xml)

    print(" INFO: Compute Layover/Shadow mask")
    currtime = time.time()
    cmd = '{} {} -x -q {} -Pinput={} -Presol={} -Poutput={}'.format(gpt_file, SLC_ls_xml, os.cpu_count(),
                                                                    inFile, resol, outFile)
    os.system(cmd)
    helpers.timer(currtime)


def slcCoherence(inFile, outFile, logFile):
    """
    This is a wrapper of S1TBX for the creation of interferometric coherence
    It includes Coherence (10x3 window) and deburst of the input product.
    """

    # define path to xml file
    SLC_coh_xml = ('/'.join(('graphs', 'S1_SLC', 'S1_SLC_Coh_Deb.xml')))
    SLC_coh_xml = pkg_resources.resource_filename(package, SLC_coh_xml)

    print(' INFO: Coherence estimation')
    currtime = time.time()
    cmd = '{} {} -x -q {} -Pinput={} -Poutput={}'.format(gpt_file, SLC_coh_xml, os.cpu_count(), inFile, outFile)
    os.system(cmd)
    helpers.timer(currtime)


def slcHalpha(inFile, outFile, logFile):
    """
    This is a wrapper of S1TBX for the creation of the H-alpha dual-pol decomposition.
    Input should be a frame imported product. Processing includes the deburst
    and Polarimetric Speckle Filtering (Improved Lee Sigma) and the H-alpha dual-pol decomposition.
    """

    #SLC_pol_xml = ('/'.join(('graphs', 'S1_SLC', 'S1_SLC_Deb_Spk_Halpha.xml')))
    SLC_pol_xml = ('/'.join(('graphs', 'S1_SLC', 'S1_SLC_Deb_Halpha.xml')))
    SLC_pol_xml = pkg_resources.resource_filename(package, SLC_pol_xml)

    print(" INFO: Calculating the H-alpha dual polarisation")
    currtime = time.time()
    cmd = '{} {} -x -q {} -Pinput={} -Poutput={}'.format(gpt_file, SLC_pol_xml, os.cpu_count(), inFile, outFile)
    os.system(cmd)
    helpers.timer(currtime)


def slcTC(inFile, outFile, resolution=20):
    """
    This is a wrapper of S1TBX for the creation of a multi-looked, terrain-corrected product.
    """

    SLC_tc_xml = ('/'.join(('graphs', 'S1_SLC', 'S1_SLC_ML_TC.xml')))
    SLC_tc_xml = pkg_resources.resource_filename(package, SLC_tc_xml)

    print(" INFO: Multilook and terrain-correcting input scene")
    currtime = time.time()
    cmd = '{} {} -x -q {} -Pinput={} -Presol={} -Poutput={}'.format(gpt_file, SLC_tc_xml, os.cpu_count(),
                                                                    inFile, resolution, outFile)
    os.system(cmd)
    helpers.timer(currtime)


def texture(inFile, outFile):
    """
    This is a wrapper of S1TBX for the creation of the GLCM texture layers.
    Input should be an GTC or RTC product,
    """

    SLC_tex_xml = ('/'.join(('graphs', 'S1_SLC', 'S1_SLC_Tex.xml')))
    SLC_tex_xml = pkg_resources.resource_filename(package, SLC_tex_xml)

    print(" INFO: Calculating texture measures")
    currtime = time.time()
    cmd = '{} {} -x -q {} -Pinput={} -Poutput={}'.format(gpt_file, SLC_tex_xml, os.cpu_count(), inFile, outFile)
    os.system(cmd)
    helpers.timer(currtime)



def slcImp2Coh(masterDate, slaveDate, outDir, tmpDir, subswath, outResolution=20):

    # define coherence output file
    outCoh = '{}/{}_{}_coh'.format(outDir, masterDate, slaveDate)
    mstImport = '{}/{}_imported'.format(tmpDir, masterDate)
    slvImport = '{}/{}_imported'.format(tmpDir, slaveDate)

    # co-registration
    fileList = ['{}_{}.dim'.format(mstImport, subswath), '{}_{}.dim'.format(slvImport, subswath)]
    fileList = '\'{}\''.format(','.join(fileList))
    slcCoreg(fileList, '{}/{}_{}_coreg_{}'.format(tmpDir, masterDate, slaveDate, subswath))

    #  remove subswathmports
    os.remove('{}_{}.dim'.format(mstImport, subswath))
    shutil.rmtree('{}_{}.data'.format(mstImport, subswath))
    os.remove('{}_{}.dim'.format(slvImport, subswath))
    shutil.rmtree('{}_{}.data'.format(slvImport, subswath))

    # calculate coherence and deburst
    slcCoherence('{}/{}_{}_coreg_{}.dim'.format(tmpDir, masterDate, slaveDate, subswath),
                       '{}/{}_{}_coh_{}'.format(tmpDir, masterDate, slaveDate, subswath))
    #
    # # remove coreg tmp files
    os.remove('{}/{}_{}_coreg_{}.dim'.format(tmpDir, masterDate, slaveDate, subswath))
    shutil.rmtree('{}/{}_{}_coreg_{}.data'.format(tmpDir, masterDate, slaveDate, subswath))

    # geocode
    slcTC('{}/{}_{}_coh_{}.dim'.format(tmpDir, masterDate, slaveDate, subswath),
                '{}/{}_{}_coh_{}'.format(outDir, masterDate, slaveDate, subswath), outResolution)

    # remove tmp files
    os.remove('{}/{}_{}_coh_{}.dim'.format(tmpDir, masterDate, slaveDate, subswath))
    shutil.rmtree('{}/{}_{}_coh_{}.data'.format(tmpDir, masterDate, slaveDate, subswath))

# =============================================================================
# def slc2CohRGB(inDir, mstFileID, slvFileID, subswath, outDir, tmpDir):
# 
#     mstInt = (glob.glob('{}/{}*{}*/*VV*img'.format(inDir, mstFileID, subswath)))
#     slvInt = (glob.glob('{}/{}*{}*/*VV*img'.format(inDir, slvFileID, subswath)))
#     coh = (glob.glob('{}/{}_{}*{}*/*VV*img'.format(inDir, mstFileID, slvFileID, subswath)))
# 
# 
#     cmd = 'gdalbuildvrt -separate -srcnodata 0 {}/stack.vrt {} {} {}'.format(tmpDir, mstInt, slvInt, coh)
#     os.system(cmd)
#     ras.outline('{}/stack.vrt'.format(tmpDir), '{}/min.tif'.format(tmpDir), 0, True)
#     ras.polygonize2Shp('{}/min.tif'.format(tmpDir), '{}/min.shp'.format(tmpDir))
#     
#     newRasterAvg = '{}/{}.{}.avg.tif'.format(outDir, mstFileID, slvFileID)
#     newRasterDiff = '{}/{}.{}.diff.tif'.format(outDir, mstFileID, slvFileID)
#     newRasterCoh = '{}/{}.{}.coh.tif'.format(outDir, mstFileID, slvFileID)
# =============================================================================





def slc2CohRGB(inDir, mstFileID, slvFileID, subswath, outDir):

    mstInt = (glob.glob('{}/{}*{}*/*VV*img'.format(inDir, mstFileID, subswath)))
    slvInt = (glob.glob('{}/{}*{}*/*VV*img'.format(inDir, slvFileID, subswath)))
    coh = (glob.glob('{}/{}_{}*{}*/*VV*img'.format(inDir, mstFileID, slvFileID, subswath)))

    newRasterAvg = '{}/{}.{}.prod.tif'.format(outDir, mstFileID, slvFileID)
    newRasterDiff = '{}/{}.{}.diff.tif'.format(outDir, mstFileID, slvFileID)
    newRasterCoh = '{}/{}.{}.coh.tif'.format(outDir, mstFileID, slvFileID)

    geo_list = ras.readFile(mstInt[0])

    ras.createFile(newRasterAvg, geo_list['cols'], geo_list['rows'], 1, geo_list['dt'], geo_list['oX'], geo_list['oY'],
                   geo_list['pW'], geo_list['pH'], geo_list['outR'], geo_list['driver'], 0)
    ras.createFile(newRasterDiff, geo_list['cols'], geo_list['rows'], 1, geo_list['dt'], geo_list['oX'], geo_list['oY'],
                   geo_list['pW'], geo_list['pH'], geo_list['outR'], geo_list['driver'], 0)
    ras.createFile(newRasterCoh, geo_list['cols'], geo_list['rows'], 1, geo_list['dt'], geo_list['oX'], geo_list['oY'],
                   geo_list['pW'], geo_list['pH'], geo_list['outR'], geo_list['driver'], 0)

    Master = gdal.Open(mstInt[0])
    Slave = gdal.Open(slvInt[0])
    Coh = gdal.Open(coh[0])

    x_block_size = 128
    y_block_size = 128

    # Get image sizes
    cols = Master.RasterXSize
    rows = Slave.RasterYSize

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

            ArrayAvg = np.empty((Master.RasterCount, ysize, xsize), dtype=np.float32)
            ArrayDiff = np.empty((Master.RasterCount, ysize, xsize), dtype=np.float32)

            ArrayMst = np.array(Master.GetRasterBand(1).ReadAsArray(x, y, xsize, ysize))
            ArraySlv = np.array(Slave.GetRasterBand(1).ReadAsArray(x, y, xsize, ysize))

            ArrayAvg = ras.convert2DB(np.divide(np.multiply(ArrayMst, ArraySlv), 2))
            ArrayDiff = ArrayMst / ArraySlv
            #ArrayProd[dBArrayVV == -130] = 0


            #arrayCoh = np.empty((Coh.RasterCount, ysize, xsize), dtype=np.float32)
            ArrayCoh = np.array(Coh.GetRasterBand(1).ReadAsArray(x, y, xsize, ysize))

            ras.chunk2raster(newRasterAvg, ArrayAvg, 0, x, y, 1)
            ras.chunk2raster(newRasterDiff, ArrayDiff, 0, x, y, 1)
            ras.chunk2raster(newRasterCoh, ArrayCoh, 0, x, y, 1)

    cmd = 'gdalbuildvrt -separate -srcnodata 0 {}/{}_{}_{}.vrt {} {} {}'.format(outDir, mstFileID, slvFileID, subswath,
                                                                                newRasterCoh, newRasterAvg, newRasterDiff)
    os.system(cmd)


#-------------------------------------------------
# Stacking operations
#-------------------------------------------------





#----------------------------------------------------------
# workflows
#----------------------------------------------------------
# def single_slc2all(fileList, outFile, tmpdir):
#
#     # construct the imported filename
#     slcImport = '{}/imported'.format(tmpdir)
#
#     # do the slice assembly if there is more than one product
#     if len(fileList.split()) > 1:
#         slcFile = '{}/assembled'.format(tmpdir)
#         sliceAssembly(fileList, slcFile)
#         frameImport(slcFile, slcImport)
#         # remove assembled files from prior processing step
#         os.remove('{}.dim'.format(slcFile))
#         shutil.rmtree('{}.data'.format(slcFile))
#     else:
#         slcFile = fileList
#         frameImport(slcFile, slcImport)
#
#     # calculate all measures
#     for i in ['iw1', 'iw2', 'iw3']:
#         backscatter('{}_{}.dim'.format(slcImport, i), '{}_gtc_{}'.format(slcImport, i), tmpdir)
#         Halpha('{}_{}.dim'.format(slcImport, i), '{}_pol_{}'.format(slcImport, i))
#         prd2mltc('{}_gtc_{}.dim'.format(slcImport, i), '{}_rtc_mltc_{}'.format(slcImport, i) )
#         prd2mltc('{}_pol_{}.dim'.format(slcImport, i), '{}_pol_mltc_{}'.format(slcImport, i) )
#         os.remove('{}_{}.dim'.format(slcImport, i))
#         shutil.rmtree('{}_{}.data'.format(slcImport, i))
        #texture('{}_rtc_mltc_{}.dim'.format(slcImport, i) , '{}_tex_mltc_{}'.format(slcImport, i) )

    # TopSAR merge of products

#def singleCoh(fileList, outFile, tmpDir):




# # preliminary main function
# if __name__ == "__main__":
#
#     datadir = '/home/avollrath/data/Ecuador/'
#     tmpdir = '/home/avollrath/tmp'
#     #
#     # for acqdate in glob.glob('{}/2*'.format(datadir)):
#     #     print(' INFO: Processing acquisition from: {}'.format(acqdate))
#     #     fileList=[]
#     #     for files in glob.glob('{}/S1*.zip'.format(acqdate)):
#     #         fileList.append(files)
#
#     fileList = "/eodata/Sentinel-1/SAR/SLC/2018/01/01/S1A_IW_SLC__1SDV_20180101T041818_20180101T041847_019956_021FB8_CF0B.SAFE/manifest.safe"
#     #print(' '.join(fileList))
#     # run the single scene slc processor (provide formatted filelist with join command)
#     #single_slc2all(' '.join(fileList), '/home/avollrath/data/Ecuador/{}/{}', tmpdir)
#     single_slc2all(fileList, '/home/avollrath/data/Ecuador/{}/{}', tmpdir)
