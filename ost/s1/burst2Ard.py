# -*- coding: utf-8 -*-
import os
import shutil 

from ost.s1 import slc2Ard


def slcBurst2CohPolArd(mstFile, slvFile, swath, burstMst, burstSlv, 
                       outDir, logFile, fileIdMst, fileIdSlv, 
                       tmpDir, prdType='GTCgamma', outResolution=20):

    # import master
    importMst = '{}/{}_import'.format(tmpDir, fileIdMst)
    
    if not os.path.exists('{}.dim'.format(importMst)):
        slc2Ard.slcBurstImport(mstFile, importMst, logFile, swath, burstMst)
    
    # create HAalpha file
    outH = '{}/{}_h'.format(tmpDir, fileIdMst)
    slc2Ard.slcHalpha('{}.dim'.format(importMst), outH, logFile)
    
    # geo code HAalpha
    outHTc = '{}/{}_hTc'.format(tmpDir, fileIdMst)
    slc2Ard.slcTC('{}.dim'.format(outH), outHTc, logFile)
    
    # move them to the outDir
    shutil.move('{}.data'.format(outHTc), '{}/{}_HAalpha.data'.format(outDir, fileIdMst))
    shutil.move('{}.dim'.format(outHTc), '{}/{}_HAalpha.dim'.format(outDir, fileIdMst))
    
    # remove HAalpha tmp files
    os.remove('{}.dim'.format(outH))
    shutil.rmtree('{}.data'.format(outH))
    
    # calibrate
    outCal = '{}/{}_cal'.format(tmpDir, fileIdMst)
    slc2Ard.slcBackscatter('{}.dim'.format(importMst), outCal, logFile, prdType)
    
    # do terrain flattening in case it is selected
    if prdType == 'RTC':
        # define outfile
        outRtc = '{}/{}_rtc'.format(tmpDir, fileIdMst)
        # do the TF
        slc2Ard.slcTerrainFlattening('{}.dim'.format(outCal), outRtc, logFile)
        # remove tmp files
        os.remove('{}.dim'.format(outCal))
        shutil.rmtree('{}.data'.format(outCal))
        # set outRtc to outCal for further processing
        outCal = outRtc
    
    # geo code backscatter products
    outTc = '{}/{}_tc'.format(tmpDir, fileIdMst)
    slc2Ard.slcTC('{}.dim'.format(outCal), outTc, logFile)
    
    # move them to the outfolder
    shutil.move('{}.data'.format(outTc), '{}/{}_BS.data'.format(outDir, fileIdMst))
    shutil.move('{}.dim'.format(outTc), '{}/{}_BS.dim'.format(outDir, fileIdMst))
    
    # create LS map
    outLs = '{}/{}_ls'.format(tmpDir, fileIdMst)
    slc2Ard.slcLSMap('{}.dim'.format(outCal), outLs, logFile, outResolution)
    
    # move LS map to out folder
    shutil.move('{}.data'.format(outLs), '{}/{}_LS.data'.format(outDir, fileIdMst))
    shutil.move('{}.dim'.format(outLs), '{}/{}_LS.dim'.format(outDir, fileIdMst))
    
    # remove calibrated files
    os.remove('{}.dim'.format(outCal))
    shutil.rmtree('{}.data'.format(outCal))
    
    # import slave
    importSlv = '{}/{}_import'.format(tmpDir, fileIdSlv)
    slc2Ard.slcBurstImport(slvFile, importSlv, logFile, swath, burstSlv)
    
    # co-registration
    fileList = ['{}.dim'.format(importMst), '{}.dim'.format(importSlv)]
    fileList = '\'{}\''.format(','.join(fileList))
    outCoreg = '{}/{}_coreg'.format(tmpDir, fileIdMst)
    slc2Ard.slcCoreg(fileList, outCoreg, logFile)

    #  remove imports
    os.remove('{}.dim'.format(importMst))
    shutil.rmtree('{}.data'.format(importMst))
    
    #os.remove('{}.dim'.format(importSlv))
    #shutil.rmtree('{}.data'.format(importSlv))

    # calculate coherence and deburst
    outCoh = '{}/{}_coh'.format(tmpDir, fileIdMst)
    slc2Ard.slcCoherence('{}.dim'.format(outCoreg), outCoh, logFile)
                       
    # remove coreg tmp files
    os.remove('{}.dim'.format(outCoreg))
    shutil.rmtree('{}.data'.format(outCoreg))

    # geocode
    outTc = '{}/{}_cohTc'.format(tmpDir, fileIdMst)
    slc2Ard.slcTC('{}.dim'.format(outCoh),outTc, logFile)

    shutil.move('{}.data'.format(outTc), '{}/{}_Coh.data'.format(outDir, fileIdMst))
    shutil.move('{}.dim'.format(outTc), '{}/{}_Coh.dim'.format(outDir, fileIdMst))
    
    # remove tmp files
    os.remove('{}.dim'.format(outCoh))
    shutil.rmtree('{}.data'.format(outCoh))
    
    
def slcBurst2PolArd(mstFile, swath, burstMst, 
                    outDir, logFile, fileIdMst, 
                    tmpDir, prdType='GTCgamma', outResolution=20):
    
    # import master
    importMst = '{}/{}_import'.format(tmpDir, fileIdMst)
    
    if not os.path.exists('{}.dim'.format(importMst)):
        slc2Ard.slcBurstImport(mstFile, importMst, logFile, swath, burstMst)
    
    # create HAalpha file
    outH = '{}/{}_h'.format(tmpDir, fileIdMst)
    slc2Ard.slcHalpha('{}.dim'.format(importMst), outH, logFile)
    
    # geo code HAalpha
    outHTc = '{}/{}_hTc'.format(tmpDir, fileIdMst)
    slc2Ard.slcTC('{}.dim'.format(outH), outHTc, logFile)
    
    # move them to the outDir
    shutil.move('{}.data'.format(outHTc), '{}/{}_HAalpha.data'.format(outDir, fileIdMst))
    shutil.move('{}.dim'.format(outHTc), '{}/{}_HAalpha.dim'.format(outDir, fileIdMst))
    
    # remove HAalpha tmp files
    os.remove('{}.dim'.format(outH))
    shutil.rmtree('{}.data'.format(outH))
    
    # calibrate
    outCal = '{}/{}_cal'.format(tmpDir, fileIdMst)
    slc2Ard.slcBackscatter('{}.dim'.format(importMst), outCal, logFile, prdType)
    
    # remove import
    os.remove('{}.dim'.format(importMst))
    shutil.rmtree('{}.data'.format(importMst))
        
    # do terrain flattening in case it is selected
    if prdType == 'RTC':
        # define outfile
        outRtc = '{}/{}_rtc'.format(tmpDir, fileIdMst)
        # do the TF
        slc2Ard.slcTerrainFlattening('{}.dim'.format(outCal), outRtc, logFile)
        # remove tmp files
        os.remove('{}.dim'.format(outCal))
        shutil.rmtree('{}.data'.format(outCal))
        # set outRtc to outCal for further processing
        outCal = outRtc
    
    # geo code backscatter products
    outTc = '{}/{}_tc'.format(tmpDir, fileIdMst)
    slc2Ard.slcTC('{}.dim'.format(outCal), outTc, logFile)
    
    # move them to the outfolder
    shutil.move('{}.data'.format(outTc), '{}/{}_BS.data'.format(outDir, fileIdMst))
    shutil.move('{}.dim'.format(outTc), '{}/{}_BS.dim'.format(outDir, fileIdMst))
    
    # create LS map
    outLs = '{}/{}_ls'.format(tmpDir, fileIdMst)
    slc2Ard.slcLSMap('{}.dim'.format(outCal), outLs, logFile, outResolution)
    
    # move LS map to out folder
    shutil.move('{}.data'.format(outLs), '{}/{}_LS.data'.format(outDir, fileIdMst))
    shutil.move('{}.dim'.format(outLs), '{}/{}_LS.dim'.format(outDir, fileIdMst))
    
    # remove calibrated files
    os.remove('{}.dim'.format(outCal))
    shutil.rmtree('{}.data'.format(outCal))