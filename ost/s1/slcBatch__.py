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
from ost import S1Scene
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


def get_scene_path(scene_id):

    scene = S1Scene(scene_id)

    if scene.creodias_path():
        path = scene.creodias_path()
    elif scene.mundi_path():
        path = scene.mundi_path()
    elif scene.onda_path():
        path = scene.onda_path()
    elif scene.download_path():
        path = scene.download_path()

    return path


def burst_to_ard_batch(burst_inventory, download_dir, processing_dir,
                       temp_dir, ard_parameters):
    '''

    '''

    resolution = ard_parameters['resolution']
    border_noise = ard_parameters['border_noise']
    prdType = ard_parameters['product_type']
    spkFlt = ard_parameters['speckle_filter']
    ls_mask = ard_parameters['ls_mask']
    to_db = ard_parameters['to_db']
    dem = ard_parameters['dem']
    coherence = ard_parameters['coherence']
    polarimetry = ard_parameters['polarimetry']

    for burst in burst_inventory.bid.unique():

        # create a list of dates over which we loop
        dates = burst_inventory.Date[
                burst_inventory.bid == burst].sort_values().tolist()

        # loop through dates
        for idx, date in enumerate(dates):

            # get master date
            master_date = dates[idx]
            end = False

            # try to get slave date
            try:
                slave_date = dates[idx + 1] # here we will jave problems for the last one
            except:
                print(' Reached the end of the time-series')


            # read master burst
            master_burst = burst_inventory[
                (burst_inventory.Date == master_date) &
                (burst_inventory.bid == burst)]

            master_scene = S1Scene(master_burst.SceneID.values[0])

            # get path to file
            master_file = master_scene.get_scene_path()
            # get subswath
            subswath = master_burst.SwathID.values[0]
            # get burst number in file
            master_burst_nr = master_burst.BurstNr.values[0]
            # create a fileId
            master_id = '{}_{}'.format(master_date, master_burst.bid.values[0])
            # create logFile
            logFile = '{}.errLog'.format(master_id)

            # create out folder
            out = '{}/{}/{}'.format(processing_dir, burst, date)
            os.makedirs(out, exist_ok=True)

            if end is True:
                coherence=False
            else:
                # read slave burst
                slave_burst = burst_inventory[
                        (burst_inventory.Date == slave_date) &
                        (burst_inventory.bid == burst)]

                slave_scene = S1Scene(slave_burst.SceneID.values[0])

                # get path to slave file
                slave_file = slave_scene.get_scene_path()

                # burst number in slave file (subswath is same)
                slave_burst_nr = slave_burst.BurstNr.values[0]

                # outFile name
                slave_id = '{}_{}'.format(slave_date,
                    slave_burst.bid.values[0])

            # run routine
            burst2Ard.slcBurst2CohPolArd(
                    master_file, slave_file, logFile,
                    subswath, master_burst_nr, slave_burst_nr,
                    out, master_id, slave_id,
                    temp_dir, prdType, resolution)


def ard2Ts(burstDf, processing_dir, temp_dir, procDict):

    for burst in burstDf.bid.unique():

        productList = {'BS': 'Gamma0', 'coh': 'coh', 'HAalpha': 'Alpha'}

        # we loop through each possible product
        for p, pName in productList.items():

            # we loop through each polarisation
            for pol in ['VV', 'VH', 'HH', 'HV']:

                # see if there is actually any imagery
                prdList = sorted(glob.glob(opj(processing_dir, burst, '20*', '*data*', '{}*{}*img'.format(pName, pol))))

                if len(prdList) > 1:

                    # check for all datafiles of this product type
                    prdList = sorted(glob.glob(opj(processing_dir, burst, '20*/', '*{}*dim'.format(p))))
                    prdList = '\'{}\''.format(','.join(prdList))

                    # define outDir for stacking routine
                    outDir = '{}/{}/Timeseries'.format(processing_dir, burst)
                    os.makedirs(outDir, exist_ok=True)

                    # create namespaces
                    tmpStack = '{}/{}_{}_{}_mt'.format(temp_dir, burst, p, pol)
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

            prdList = sorted(glob.glob(opj(processing_dir, burst, '20*', '*{}*'.format(p), '*{}.img'.format(pol))))

            if len(prdList) > 1:

                prdList = sorted(glob.glob(opj(processing_dir, burst, '20*/', '*{}*dim'.format(p))))
                prdList = '\'{}\''.format(','.join(prdList))

                #print(prdList)
                outDir = '{}/{}/Timeseries'.format(processing_dir, burst)
                os.makedirs(outDir, exist_ok=True)

                tmpStack = '{}/{}_{}_mt'.format(temp_dir, burst, pol)
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
