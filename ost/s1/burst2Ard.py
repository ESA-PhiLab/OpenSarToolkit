# -*- coding: utf-8 -*-
import os
from os.path import join as opj
import shutil 

from ost.s1 import slc2Ard
from ost.helpers import helpers as h




def slcBurst2CohPolArd(mstFile, slvFile, logFile,
                       swath, burstMst, burstSlv, 
                       outDir, fileIdMst, fileIdSlv, 
                       tmpDir, prdType='GTCgamma', outResolution=20, 
                       removeSlvImport=False):

    # import master
    importMst = opj(tmpDir, '{}_import'.format(fileIdMst))
    
    if not os.path.exists('{}.dim'.format(importMst)):
        slc2Ard.slcBurstImport(mstFile, importMst, logFile, swath, burstMst)
    
    # create HAalpha file
    outH = opj(tmpDir, '{}_h'.format(fileIdMst))
    slc2Ard.slcHalpha('{}.dim'.format(importMst), outH, logFile)
    
    # geo code HAalpha
    outHTc = opj(tmpDir, '{}_HAalpha'.format(fileIdMst))
    slc2Ard.slcTC('{}.dim'.format(outH), outHTc, logFile, outResolution)
    
    # move them to the outDir
    shutil.move('{}.data'.format(outHTc), opj(outDir, '{}_HAalpha.data'.format(fileIdMst)))
    shutil.move('{}.dim'.format(outHTc), opj(outDir, '{}_HAalpha.dim'.format(fileIdMst)))
    
    # remove HAalpha tmp files
    h.delDimap(outH)
    
    # calibrate
    outCal = opj(tmpDir, '{}_cal'.format(fileIdMst))
    slc2Ard.slcBackscatter('{}.dim'.format(importMst), outCal, logFile, prdType)
    
    # do terrain flattening in case it is selected
    if prdType == 'RTC':
        # define outfile
        outRtc = opj(tmpDir, '{}_rtc'.format(fileIdMst))
        # do the TF
        slc2Ard.slcTerrainFlattening('{}.dim'.format(outCal), outRtc, logFile)
        # remove tmp files
        h.delDimap(outCal)
        # set outRtc to outCal for further processing
        outCal = outRtc
    
    # geo code backscatter products
    outTc = opj(tmpDir, '{}_BS'.format(fileIdMst))
    slc2Ard.slcTC('{}.dim'.format(outCal), outTc, logFile, outResolution)
    
    # move them to the outfolder
    shutil.move('{}.data'.format(outTc), opj(outDir, '{}_BS.data'.format(fileIdMst)))
    shutil.move('{}.dim'.format(outTc), opj(outDir, '{}_BS.dim'.format(fileIdMst)))
    
    # create LS map
    outLs = opj(tmpDir, '{}_LS'.format(fileIdMst))
    slc2Ard.slcLSMap('{}.dim'.format(outCal), outLs, logFile, outResolution)
    
    # move LS map to out folder
    shutil.move('{}.data'.format(outLs), opj(outDir, '{}_LS.data'.format(fileIdMst)))
    shutil.move('{}.dim'.format(outLs), opj(outDir, '{}_LS.dim'.format(fileIdMst)))
    
    # remove calibrated files
    h.delDimap(outCal)
    
    # import slave
    importSlv = opj(tmpDir, '{}_import'.format(fileIdSlv))
    slc2Ard.slcBurstImport(slvFile, importSlv, logFile, swath, burstSlv)
    
    # co-registration
    fileList = ['{}.dim'.format(importMst), '{}.dim'.format(importSlv)]
    fileList = '\'{}\''.format(','.join(fileList))
    outCoreg = opj(tmpDir, '{}_coreg'.format(fileIdMst))
    slc2Ard.slcCoreg(fileList, outCoreg, logFile)

    #  remove imports
    h.delDimap(importMst)
        
    if removeSlvImport is True:
        h.delDimap(importSlv)
        
    # calculate coherence and deburst
    outCoh = opj(tmpDir, '{}_c'.format(fileIdMst))
    slc2Ard.slcCoherence('{}.dim'.format(outCoreg), outCoh, logFile)
                       
    # remove coreg tmp files
    h.delDimap(outCoreg)
    
    # geocode
    outTc = opj(tmpDir, '{}_coh'.format(fileIdMst))
    slc2Ard.slcTC('{}.dim'.format(outCoh), outTc, logFile, outResolution)

    shutil.move('{}.data'.format(outTc), opj(outDir, '{}_coh.data'.format(fileIdMst)))
    shutil.move('{}.dim'.format(outTc), opj(outDir, '{}_coh.dim'.format(fileIdMst)))
    
    # remove tmp files
    h.delDimap(outCoh)
    
    
def slcBurst2PolArd(mstFile, logFile, 
                    swath, burstMst, 
                    outDir, fileIdMst, 
                    tmpDir, prdType='GTCgamma', outResolution=20):
    
       # import master
    importMst = opj(tmpDir, '{}_import'.format(fileIdMst))
    
    if not os.path.exists('{}.dim'.format(importMst)):
        slc2Ard.slcBurstImport(mstFile, importMst, logFile, swath, burstMst)
    
    # create HAalpha file
    outH = opj(tmpDir, '{}_h'.format(fileIdMst))
    slc2Ard.slcHalpha('{}.dim'.format(importMst), outH, logFile)
    
    # geo code HAalpha
    outHTc = opj(tmpDir, '{}_HAalpha'.format(fileIdMst))
    slc2Ard.slcTC('{}.dim'.format(outH), outHTc, logFile, outResolution)
    
    # move them to the outDir
    shutil.move('{}.data'.format(outHTc), opj(outDir, '{}_HAalpha.data'.format(fileIdMst)))
    shutil.move('{}.dim'.format(outHTc), opj(outDir, '{}_HAalpha.dim'.format(fileIdMst)))
    
    # remove HAalpha tmp files
    h.delDimap(outH)
    
    # calibrate
    outCal = opj(tmpDir, '{}_cal'.format(fileIdMst))
    slc2Ard.slcBackscatter('{}.dim'.format(importMst), outCal, logFile, prdType)
    
    #  remove import
    h.delDimap(importMst)
    
    # do terrain flattening in case it is selected
    if prdType == 'RTC':
        # define outfile
        outRtc = opj(tmpDir, '{}_rtc'.format(fileIdMst))
        # do the TF
        slc2Ard.slcTerrainFlattening('{}.dim'.format(outCal), outRtc, logFile)
        # remove tmp files
        h.delDimap(outCal)
        # set outRtc to outCal for further processing
        outCal = outRtc
    
    # geo code backscatter products
    outTc = opj(tmpDir, '{}_BS'.format(fileIdMst))
    slc2Ard.slcTC('{}.dim'.format(outCal), outTc, logFile, outResolution)
    
    # move them to the outfolder
    shutil.move('{}.data'.format(outTc), opj(outDir, '{}_BS.data'.format(fileIdMst)))
    shutil.move('{}.dim'.format(outTc), opj(outDir, '{}_BS.dim'.format(fileIdMst)))
    
    # create LS map
    outLs = opj(tmpDir, '{}_LS'.format(fileIdMst))
    slc2Ard.slcLSMap('{}.dim'.format(outCal), outLs, logFile, outResolution)
    
    # move LS map to out folder
    shutil.move('{}.data'.format(outLs), opj(outDir, '{}_LS.data'.format(fileIdMst)))
    shutil.move('{}.dim'.format(outLs), opj(outDir, '{}_LS.dim'.format(fileIdMst)))
    
    # remove calibrated files
    h.delDimap(outCal)