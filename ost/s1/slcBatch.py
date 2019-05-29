#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import glob
import datetime
import gdal

from os.path import join as opj

from ..helpers import raster as ras
from .. import ts

def createProcParamDict(
                    aoi, outResolution, lsMask, spkFlt, outPrdType,
                    mtSpkFlt, metrics, toDB, dType,
                    subset=None, polarisation='VV,VH,HH,HV',
                    toPower=True, rescale=True, outRem=True,
                        ):

    procParams = {
                  'aoi': aoi,
                  'resolution': outResolution,
                  'prdType': outPrdType,
                  'polarisation': polarisation,
                  'lsMask': lsMask,
                  'spkFlt': spkFlt,
                  'subset': subset,
                  'mtSpkFlt': mtSpkFlt,
                  'metrics': metrics,
                  'toDB': toDB,
                  'dType': dType,
                  'toPower': toPower,
                  'rescale': rescale,
                  'outlierRemoval': outRem,
                }

    return procParams


def ard2Ts(burstDf, prcDir, tmpDir, procDict):

    for burst in burstDf.bid.unique():
        
        productList = {'BS': 'Gamma0', 'coh': 'coh', 'HAalpha': 'Alpha'}
        prdList = []
        
        # we loop through each osible product
        for p, pName in productList.items():
            
           
             # we loop thorugh each polarisation
             for pol in ['VV', 'VH', 'HH', 'HV']:
                
                 # see if there is imagery
                 prdList = sorted(glob.glob(opj(prcDir, burst, '20*', '*data*', '{}*{}*img'.format(pName, pol))))
                 if len(prdList) > 1:
    
                     prdList = sorted(glob.glob(opj(prcDir, burst, '20*/', '*{}*dim'.format(p))))
                     prdList = '\'{}\''.format(','.join(prdList))
                    
                     outDir = '{}/{}/Timeseries'.format(prcDir, burst)
                     os.makedirs(outDir, exist_ok=True)
                    
                     tmpStack = '{}/{}_{}_{}_mt'.format(tmpDir, burst, p, pol)
                     outStack = '{}/{}_{}_{}_mt'.format(outDir, burst, p, pol)
                    
                     ts.createStackPol(prdList, pol, tmpStack, '/home/avollrath/log')
                     ts.mtSpeckle('{}.dim'.format(tmpStack), outStack, '/home/avollrath/log')
                    
                     # remove HAalpha tmp files
                     os.remove('{}.dim'.format(tmpStack))
                     shutil.rmtree('{}.data'.format(tmpStack))
                    
                     if p == 'BS':
                         # get the dates of the files
                         dates = [datetime.datetime.strptime(x.split('_')[-1][:-4], '%d%b%Y') for x in glob.glob(opj('{}.data'.format(outStack), '*img'))]
                         # sort them
                         dates.sort()
                         # write them back to string for following loop
                         sortedDates = [datetime.datetime.strftime(ts, "%d%b%Y") for ts in dates]                        
    
                         i, outFiles = 1, []
                         for date in sortedDates:
    
                             # restructure date to YYMMDD
                             inDate = datetime.datetime.strptime(date, '%d%b%Y')
                             outDate = datetime.datetime.strftime(inDate, '%y%m%d')
    
                             inFile = glob.glob(opj('{}.data'.format(outStack), '*{}*{}*img'.format(pol, date)))[0]
                             # create outFile
                             outFile = opj(outDir, '{}.{}.{}.{}.tif'.format(i, outDate, p, pol))
                             # mask by extent
                             ras.maskByShape(inFile, outFile, "/Phi_avollrath/Toulouse/AOI/bounds.shp",
                                             toDB=toDB, dType=dType, minVal=-30, maxVal=5, ndv=0)
                             # add ot a list for subsequent vrt creation
                             outFiles.append(outFile)
    
                             i += 1
    
                         # build vrt of timeseries
                         vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
                         gdal.BuildVRT(opj(outDir, 'Timeseries.{}.vrt'.format(p)), outFiles, options=vrt_options)
    
                     if p == 'coh':
                                         
                         # get slave and master Date
                         mstDates = [datetime.datetime.strptime(os.path.basename(x).split('_')[3].split('.')[0] , '%d%b%Y') for x in glob.glob(opj('{}.data'.format(outStack), '*img'))]
                         slvDates = [datetime.datetime.strptime(os.path.basename(x).split('_')[4].split('.')[0] , '%d%b%Y') for x in glob.glob(opj('{}.data'.format(outStack), '*img'))]
                         # sort them
                         mstDates.sort()
                         slvDates.sort()
                         # write them back to string for following loop
                         sortedMstDates = [datetime.datetime.strftime(ts, "%d%b%Y") for ts in mstDates]
                         sortedSlvDates = [datetime.datetime.strftime(ts, "%d%b%Y") for ts in slvDates]
                        
                         i, outFiles = 1, []
                         for mst, slv in zip(sortedMstDates, sortedSlvDates):
    
                             inMst = datetime.datetime.strptime(mst, '%d%b%Y')
                             inSlv = datetime.datetime.strptime(slv, '%d%b%Y')
                            
                             outMst = datetime.datetime.strftime(inMst, '%y%m%d')
                             outSlv = datetime.datetime.strftime(inSlv, '%y%m%d')
    
                             inFile = glob.glob(opj('{}.data'.format(outStack), '*{}*{}_{}*img'.format(pol, mst, slv)))[0]
                             outFile = opj(outDir, '{}.{}.{}.{}.{}.tif'.format(i, outMst, outSlv, p, pol))
                            
                             ras.maskByShape(inFile, outFile, "/Phi_avollrath/Toulouse/AOI/bounds.shp",
                                             toDB=False, dType=dType, minVal=0.000001, maxVal=1, ndv=0)
                            
                             # add ot a list for subsequent vrt creation
                             outFiles.append(outFile)
    
                             i += 1
    
                         # build vrt of timeseries
                         vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
                         gdal.BuildVRT(opj(outDir, 'Timeseries.{}.vrt'.format(p)), outFiles, options=vrt_options)
   