# -*- coding: utf-8 -*-
import os
from os.path import join as opj
import imp
import sys

from ost.helpers import helpers as h


def _import(infile, out_prefix, logfile, swath, burst, polar='VV,VH,HH,HV'):
    '''A wrapper of SNAP import of a single Sentinel-1 SLC burst

    This function takes an original Sentinel-1 scene (either zip or
    SAFE format), updates the orbit information (does not fail if not
    available), and extracts a single burst based on the
    given input parameters.

    Args:
        infile: string or os.path object for
                an original Sentinel-1 GRD product in zip or SAFE format
        out_prefix: string or os.path object for the output
                    file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        swath (str): the corresponding IW subswath of the burst
        burst (str): the burst number as in the Sentinel-1 annotation file
        polar (str): a string consisiting of the polarisation (comma separated)
                     e.g. 'VV,VH',
                     default value: 'VV,VH,HH,HV'

    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootPath = imp.find_module('ost')[1]
    graph = opj(rootPath, 'graphs', 'S1_SLC2ARD', 'S1_SLC_BurstSplit_AO.xml')

    print(' INFO: Importing Burst {} from Swath {}'
          ' from scene {}'.format(burst, swath, os.path.basename(infile)))

    command = '{} {} -x -q {} -Pinput={} -Ppolar={} -Pswath={}\
                      -Pburst={} -Poutput={}' \
        .format(gpt_file, graph, os.cpu_count(), infile, polar, swath,
                burst, out_prefix)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(119)


def _HAalpha(infile, outfile, logfile, pol_speckle_filter=False):
    '''A wrapper of SNAP H-A-alpha polarimetric decomposition

    This function takes an OST imported Sentinel-1 scene/burst
    and calulates the polarimetric decomposition parameters for
    the H-A-alpha decomposition.

    Args:
        infile: string or os.path object for
                an original Sentinel-1 GRD product in zip or SAFE format
        out_prefix: string or os.path object for the output
                    file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        pol_speckle_filter (bool): wether or not to apply the
                                   polarimetric speckle filter

    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootPath = imp.find_module('ost')[1]

    if pol_speckle_filter:
        graph = opj(rootPath, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_Deb_Spk_Halpha.xml')
    else:
        graph = opj(rootPath, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_Deb_Halpha.xml')

    print(" INFO: Calculating the H-alpha dual polarisation")
    command = '{} {} -x -q {} -Pinput={} -Poutput={}' \
        .format(gpt_file, graph, 2 * os.cpu_count(), infile, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully created H/Alpha product')
    else:
        print(' ERROR: H/Alpha exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(121)


def _calibration(infile, outfile, logfile, product_type='GTCgamma'):
    '''A wrapper around SNAP's radiometric calibration

    This function takes OST imported Sentinel-1 product and generates
    it to calibrated backscatter.

    3 different calibration modes are supported.
        - Radiometrically terrain corrected Gamma nought (RTC)
          NOTE: that the routine actually calibrates to bet0 and needs to
          be used together with _terrain_flattening routine
        - ellipsoid based Gamma nought (GTCgamma)
        - Sigma nought (GTCsigma).

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        resolution (int): the resolution of the output product in meters
        product_type (str): the product type of the output product
                            i.e. RTC, GTCgamma or GTCsigma

    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootPath = imp.find_module('ost')[1]

    if product_type == 'RTC':
        print(' INFO: Calibrating the product to a RTC product.')
        graph = opj(rootPath, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_Calbeta_Deb.xml')
    elif product_type == 'GTCgamma':
        print(' INFO: Calibrating the product to a GTC product (Gamma0).')
        graph = opj(rootPath, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_CalGamma_Deb.xml')
    elif product_type == 'GTCsigma':
        print(' INFO: Calibrating the product to a GTC product (Sigma0).')
        graph = opj(rootPath, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_CalSigma_Deb.xml')
    else:
        print(' ERROR: Wrong product type selected.')
        exit

    print(" INFO: Removing thermal noise, calibrating and debursting")
    command = '{} {} -x -q {} -Pinput={} -Poutput={}' \
        .format(gpt_file, graph, 2 * os.cpu_count(), infile, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(121)


def _terrain_flattening(infile, outfile, logfile, reGrid=True,
                        dem='SRTM 1sec HGT'):
    '''A wrapper around SNAP's terrain flattening

    This function takes OST calibrated Sentinel-1 SLC product and applies
    the terrain flattening to correct for radiometric distortions along slopes

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        reGrid (bool): boolean for the re-grid method should be used
                       (from SNAP 7 on the only one that works)

    '''

    # get gpt file
    gpt_file = h.gpt_path()

    print(' INFO: Correcting for the illumination along slopes'
          ' (Terrain Flattening).')

    command = '{} Terrain-Flattening -x -q {} -PreGridMethod={} \
             -PadditionalOverlap=0.2 -PoversamplingMultiple=1.5 \
             -PdemName=\'{}\' -t {} {}' \
        .format(gpt_file, 2 * os.cpu_count(), reGrid, dem, outfile, infile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully applied the terrain flattening')
    else:
        print(' ERROR: Terrain Flattening exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(121)


def _ls_mask(infile, outfile, logfile, resol=20, dem='SRTM 1sec HGT'):
    '''A wrapper around SNAP's Layover/Shadow mask routine

    This function takes OST imported Sentinel-1 product and calculates
    the Layover/Shadow mask.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        resolution (int): the resolution of the output product in meters
        dem (str): A Snap compliant string for the dem to use.
                   Possible choices are:
                       'SRTM 1sec HGT' (default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootPath = imp.find_module('ost')[1]
    graph = opj(rootPath, 'graphs', 'S1_SLC2ARD', 'S1_SLC_LS_TC.xml')

    print(" INFO: Compute Layover/Shadow mask")
    command = '{} {} -x -q {} -Pinput={} -Presol={} -Poutput={} -Pdem=\'{}\'' \
        .format(gpt_file, graph, 2 * os.cpu_count(), infile, resol,
                outfile, dem)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully created Layover/Shadow mask')
    else:
        print(' ERROR: Layover/Shadow mask creation exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(121)


def _coreg(filelist, outfile, logfile, dem='SRTM 1sec HGT'):
    '''A wrapper around SNAP's back-geocoding co-registration routine

    This function takes a list of 2 OST imported Sentinel-1 SLC products
    and co-registers them properly. This routine is sufficient for coherence
    estimation, but not for InSAR, since the ESD refinement is not applied.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        dem (str): A Snap compliant string for the dem to use.
                   Possible choices are:
                       'SRTM 1sec HGT' (default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootPath = imp.find_module('ost')[1]
    graph = opj(rootPath, 'graphs', 'S1_SLC2ARD', 'S1_SLC_BGD.xml')

    print(' INFO: Co-registering {}'.format(filelist[0]))
    command = '{} {} -x -q {} -Pfilelist={} -Poutput={} -Pdem=\'{}\''\
        .format(gpt_file, graph, 2 * os.cpu_count(), filelist, outfile, dem)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully coregistered product')
    else:
        print(' ERROR: Co-registration exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(112)


def _coherence(infile, outfile, logfile):
    '''A wrapper around SNAP's coherence routine

    This function takes a co-registered stack of 2 Sentinel-1 SLC products
    and calculates the coherence.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to

    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootPath = imp.find_module('ost')[1]
    graph = opj(rootPath, 'graphs', 'S1_SLC2ARD', 'S1_SLC_Coh_Deb.xml')

    print(' INFO: Coherence estimation')
    command = '{} {} -x -q {} -Pinput={} -Poutput={}' \
        .format(gpt_file, graph, 2 * os.cpu_count(), infile, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully created Coherence product')
    else:
        print(' ERROR: Coherence exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(121)


def _terrain_correction(infile, outfile, logfile, resolution=20,
                        dem='SRTM 1sec HGT'):
    '''A wrapper around SNAP's Terrain Correction routine

    This function takes an OST calibrated Sentinel-1 product and
    does the geocodification.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        resolution (int): the resolution of the output product in meters
        dem (str): A Snap compliant string for the dem to use.
                   Possible choices are:
                       'SRTM 1sec HGT' (default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get gpt file
    gpt_file = h.gpt_path()

    print(" INFO: Geocoding input scene")

    command = '{} Terrain-Correction -x -q {} \
              -PdemResamplingMethod=\'BILINEAR_INTERPOLATION\' \
              -PimgResamplingMethod=\'BILINEAR_INTERPOLATION\' \
              -PnodataValueAtSea=\'false\' \
              -PpixelSpacingInMeter=\'{}\' \
              -PdemName=\'{}\' \
              -t {} {}' \
              .format(gpt_file, 2 * os.cpu_count(), resolution, dem,
                      outfile, infile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Geocoding exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(122)


def burst_to_ard(master_file,
                 swath,
                 master_burst_nr,
                 master_burst_id,
                 out_dir,
                 temp_dir,
                 logfile,
                 slave_file=None,
                 slave_burst_nr=None,
                 slave_burst_id=None,
                 coherence=False,
                 polarimetry=False,
                 resolution=20,
                 product_type='GTCgamma',
                 speckle_filter=False,
                 to_db=False,
                 ls_mask=False,
                 dem='SRTM 1sec HGT',
                 remove_slave_import=False):
    '''The main routine to turn a burst into an ARD product

    Args:
        master_file (str): path to full master SLC scene
        swath (str): subswath
        master_burst_nr (): index number of the burst
        master_burst_id ():
        out_dir (str):
        temp_dir (str):
        logfile (str):
        slave_file (str):
        slave_burst_nr (str):
        slave_burst_id (str):
        coherence (bool):
        polarimetry (bool):
        resolution (int):
        product_type (str):
        speckle_filter (bool):
        to_db (bool):
        ls_mask (bool):
        dem (str):
        remove_slave_import (bool):

    '''

    # import master
    master_import = opj(temp_dir, '{}_import'.format(master_burst_id))

    if not os.path.exists('{}.dim'.format(master_import)):
        _import(master_file, master_import, logfile, swath, master_burst_nr)

    if polarimetry:
        # create HAalpha file
        outH = opj(temp_dir, '{}_h'.format(master_burst_id))
        _HAalpha('{}.dim'.format(master_import), outH, logfile)

        # geo code HAalpha
        outHTc = opj(temp_dir, '{}_HAalpha'.format(master_burst_id))
        _terrain_correction(
                '{}.dim'.format(outH), outHTc, logfile, resolution, dem)

        h.move_dimap(outHTc,
                     opj(out_dir, '{}_HAalpha'.format(master_burst_id)))

        # remove HAalpha tmp files
        h.delete_dimap(outH)

    if speckle_filter:
        print(' This is where the speckle filtering should go')

    # calibrate
    outCal = opj(temp_dir, '{}_cal'.format(master_burst_id))
    _calibration(
            '{}.dim'.format(master_import), outCal, logfile, product_type)

    if not coherence:
        #  remove imports
        h.delete_dimap(master_import)

    # do terrain flattening in case it is selected
    if product_type == 'RTC':
        # define outfile
        outRtc = opj(temp_dir, '{}_rtc'.format(master_burst_id))
        # do the TF
        _terrain_flattening('{}.dim'.format(outCal), outRtc, logfile)
        # remove tmp files
        h.delete_dimap(outCal)
        # set outRtc to outCal for further processing
        outCal = outRtc

    # geo code backscatter products
    outTc = opj(temp_dir, '{}_BS'.format(master_burst_id))
    _terrain_correction(
            '{}.dim'.format(outCal), outTc, logfile, resolution, dem)

    if to_db:
        print('This is where the the to db should go')

    h.move_dimap(outTc, opj(out_dir, '{}_BS'.format(master_burst_id)))

    if ls_mask:
        # create LS map
        outLs = opj(temp_dir, '{}_LS'.format(master_burst_id))
        _ls_mask('{}.dim'.format(outCal), outLs, logfile, resolution, dem)

        h.move_dimap(outLs, opj(out_dir, '{}_LS'.format(master_burst_id)))

    # remove calibrated files
    h.delete_dimap(outCal)

    if coherence:

        # import slave
        slave_import = opj(temp_dir, '{}_import'.format(slave_burst_id))
        _import(slave_file, slave_import, logfile, swath, slave_burst_nr)

        # co-registration
        filelist = ['{}.dim'.format(master_import),
                    '{}.dim'.format(slave_import)]
        filelist = '\'{}\''.format(','.join(filelist))
        outCoreg = opj(temp_dir, '{}_coreg'.format(master_burst_id))
        _coreg(filelist, outCoreg, logfile)

        #  remove imports
        h.delete_dimap(master_import)

        if remove_slave_import is True:
            h.delete_dimap(slave_import)

        # calculate coherence and deburst
        outCoh = opj(temp_dir, '{}_c'.format(master_burst_id))
        _coherence('{}.dim'.format(outCoreg), outCoh, logfile)

        # remove coreg tmp files
        h.delete_dimap(outCoreg)

        # geocode
        outTc = opj(temp_dir, '{}_coh'.format(master_burst_id))
        _terrain_correction(
                '{}.dim'.format(outCoh), outTc, logfile, resolution, dem)

        h.move_dimap(outTc, opj(out_dir, '{}_coh'.format(master_burst_id)))

        # remove tmp files
        h.delete_dimap(outCoh)
