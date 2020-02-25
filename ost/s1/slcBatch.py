#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# standard libs
import os
import glob
import datetime
import gdal

# import for os independent path handling
from os.path import join as opj

# ost imports
from . import metadata
from . import burst2Ard
from ..helpers import raster as ras
from ..helpers import helpers as h


def createParamDict(
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
                  'coherence': coh,
                  'haalpha': haalpha
                }

    return procParams


def burst2ArdBatch(inputDf, dwnDir, prcDir, tmpDir, procParams):


    for burst in inputDf.bid.unique():

        # create a list of dates over which we loop
        dates = inputDf.Date[inputDf.bid == burst].sort_values().tolist()

        # loop through dates
        for idx, date in enumerate(dates):

            # get master date
            dateMst = dates[idx]
            end = False

            # try to get slave date
            try:
                dateSlv = dates[idx + 1] # here we will jave problems for the last one
            except:
                print(' Reached the end of the time-series')
                    

            # read master burst
            brstMst = inputDf[(inputDf.Date == dateMst) &
                              (inputDf.bid == burst)]

            # get path to file
            inFileMst = metadata.s1Metadata(brstMst.SceneID.values[0]).s1CreoPath() #(dwnDir)
            # get subswath
            subSwath = brstMst.SwathID.values[0]
            # get burst number in file
            burstMst = brstMst.BurstNr.values[0]
            # create a fileId
            fileIdMst = '{}_{}'.format(dateMst, brstMst.bid.values[0])
            # create logFile
            logFile = '{}.errLog'.format(fileIdMst)

            # create out folder
            out = '{}/{}/{}'.format(outDir, burst, date)
            os.makedirs(out, exist_ok=True)

            if end is True:
                # run the single burst routine (i.e. without coherence)
                burst2Ard.slcBurst2PolArd(inFileMst, logFile,
                                          subSwath, burstMst, out, fileIdMst,
                                          tmpDir, prdType, outResolution)

            else:
                # read slave burst
                brstSlv = inputDf[(inputDf.Date == dateSlv) &
                                  (inputDf.bid == burst)]

                # get path to slave file
                inFileSlv = metadata.s1Metadata(brstSlv.SceneID.values[0]).s1CreoPath() #dwnDir)
                # burst number in slave file (subSwath is same)
                burstSlv = brstSlv.BurstNr.values[0]
                # outFile name
                fileIdSlv = '{}_{}'.format(dateSlv, brstSlv.bid.values[0])

                # run routine
                burst2Ard.slcBurst2CohPolArd(inFileMst, inFileSlv, logFile,
                                             subSwath, burstMst, burstSlv,
                                             out, fileIdMst, fileIdSlv,
                                             tmpDir, prdType, outResolution)


def ard2Ts(burstDf, prcDir, tmpDir, procDict):

    for burst in burstDf.bid.unique():

        productList = {'BS': 'Gamma0', 'coh': 'coh', 'HAalpha': 'Alpha'}

        # we loop through each possible product
        for p, pName in productList.items():

            # we loop through each polarisation
            for pol in ['VV', 'VH', 'HH', 'HV']:

                # see if there is actually any imagery
                prdList = sorted(glob.glob(opj(prcDir, burst, '20*', '*data*', '{}*{}*img'.format(pName, pol))))

                if len(prdList) > 1:

                    # check for all datafiles of this product type
                    prdList = sorted(glob.glob(opj(prcDir, burst, '20*/', '*{}*dim'.format(p))))
                    prdList = '\'{}\''.format(','.join(prdList))

                    # define outDir for stacking routine
                    outDir = '{}/{}/Timeseries'.format(prcDir, burst)
                    os.makedirs(outDir, exist_ok=True)

                    # create namespaces
                    tmpStack = '{}/{}_{}_{}_mt'.format(tmpDir, burst, p, pol)
                    outStack = '{}/{}_{}_{}_mtF'.format(outDir, burst, p, pol)

                    # run stacking routines
                    ts.createStackPol(prdList, pol, tmpStack, '/home/avollrath/log')

                    # run mt speckle filter
                    if mtSpeckle is True:
                        ts.mtSpeckle('{}.dim'.format(tmpStack), outStack, '/home/avollrath/log')
                        # remove tmp files
                        h.delDimap(tmpStack)
                    else:
                        outStack = tmpStack

                    # convert to GeoTiffs
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
                        gdal.BuildVRT(opj(outDir, 'Timeseries.{}.{}.vrt'.format(p, pol)), outFiles, options=vrt_options)

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
                        gdal.BuildVRT(opj(outDir, 'Timeseries.{}.{}.vrt'.format(p, pol)), outFiles, options=vrt_options)

                    # remove tmp files
                    h.delDimap(outStack)


        for pol in ['Alpha', 'Entropy', 'Anisotropy']:

            prdList = sorted(glob.glob(opj(prcDir, burst, '20*', '*{}*'.format(p), '*{}.img'.format(pol))))

            if len(prdList) > 1:

                prdList = sorted(glob.glob(opj(prcDir, burst, '20*/', '*{}*dim'.format(p))))
                prdList = '\'{}\''.format(','.join(prdList))

                #print(prdList)
                outDir = '{}/{}/Timeseries'.format(prcDir, burst)
                os.makedirs(outDir, exist_ok=True)

                tmpStack = '{}/{}_{}_mt'.format(tmpDir, burst, pol)
                outStack = '{}/{}_{}_mt'.format(outDir, burst, pol)

                # processing routines
                ts.createStackPattern(prdList, pol, tmpStack, '/home/avollrath/log')
                if mtSpeckle is True:
                    ts.mtSpeckle('{}.dim'.format(tmpStack), outStack, '/home/avollrath/log')
                    # remove tmp files
                    h.delDimap(tmpStack)
                else:
                    outStack = tmpStack

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
                    maxV = 90 if pol is 'Alpha' else 1
                    ras.maskByShape(inFile, outFile, "/Phi_avollrath/Toulouse/AOI/bounds.shp",
                                    toDB=False, dType=dType, minVal=0.000001, maxVal=maxV, ndv=0)

                    # add ot a list for subsequent vrt creation
                    outFiles.append(outFile)
                    i += 1

                # build vrt of timeseries
                vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
                gdal.BuildVRT(opj(outDir, 'Timeseries.{}.vrt'.format(pol)), outFiles, options=vrt_options)

                # remove tmp files
                h.delDimap(outStack)
