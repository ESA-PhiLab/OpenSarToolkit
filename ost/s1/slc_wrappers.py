import os
from os.path import join as opj
import importlib
import json
import sys

from ost.helpers import helpers as h
from ost.snap_common import common


def _import(infile, out_prefix, logfile, swath, burst, polar='VV,VH,HH,HV',
            ncores=os.cpu_count()):
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
        ncores(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count
    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
    graph = opj(rootpath, 'graphs', 'S1_SLC2ARD', 'S1_SLC_BurstSplit_AO.xml')

    print(' INFO: Importing Burst {} from Swath {}'
          ' from scene {}'.format(burst, swath, os.path.basename(infile)))

    command = '{} {} -x -q {} -Pinput={} -Ppolar={} -Pswath={}\
                      -Pburst={} -Poutput={}' \
        .format(gpt_file, graph, ncores, infile, polar, swath,
                burst, out_prefix)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(119)

    return return_code


def _ha_alpha(infile, outfile, logfile, pol_speckle_filter=False,
              pol_speckle_dict=None, ncores=os.cpu_count()):
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
        ncores(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count


    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]

    if pol_speckle_filter:
        graph = opj(rootpath, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_Deb_Spk_Halpha.xml')
        print(' INFO: Applying the polarimetric speckle filter and'
              ' calculating the H-alpha dual-pol decomposition')
        command = ('{} {} -x -q {} -Pinput={} -Poutput={}'
                   ' -Pfilter=\'{}\''
                   ' -Pfilter_size=\'{}\''
                   ' -Pnr_looks={}'
                   ' -Pwindow_size={}'
                   ' -Ptarget_window_size={}'
                   ' -Ppan_size={}'
                   ' -Psigma={}'.format(
            gpt_file, graph, ncores,
            infile, outfile,
            pol_speckle_dict['filter'],
            pol_speckle_dict['filter size'],
            pol_speckle_dict['num of looks'],
            pol_speckle_dict['window size'],
            pol_speckle_dict['target window size'],
            pol_speckle_dict['pan size'],
            pol_speckle_dict['sigma']
        )
        )
    else:
        graph = opj(rootpath, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_Deb_Halpha.xml')

        print(" INFO: Calculating the H-alpha dual polarisation")
        command = '{} {} -x -q {} -Pinput={} -Poutput={}' \
            .format(gpt_file, graph, ncores, infile, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully created H/A/Alpha product')
    else:
        print(' ERROR: H/Alpha exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(121)

    return return_code


def _calibration(infile, outfile, logfile, product_type='GTCgamma',
                 ncores=os.cpu_count()):
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
        ncores(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count


    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]

    if product_type == 'RTC-gamma0':
        print(' INFO: Calibrating the product to beta0.')
        graph = opj(rootpath, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_Calbeta_Deb.xml')
    elif product_type == 'GTC-gamma0':
        print(' INFO: Calibrating the product to gamma0.')
        graph = opj(rootpath, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_CalGamma_Deb.xml')
    elif product_type == 'GTC-sigma0':
        print(' INFO: Calibrating the product to sigma0.')
        graph = opj(rootpath, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_CalSigma_Deb.xml')
    elif product_type == 'Coherence_only':
        print('INFO: No need to calibrate just for coherence')
        return_code = 0
        return return_code
    else:
        print(' ERROR: Wrong product type selected.')
        sys.exit(121)

    print(" INFO: Removing thermal noise, calibrating and debursting")
    command = '{} {} -x -q {} -Pinput={} -Poutput={}' \
        .format(gpt_file, graph, ncores, infile, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully calibrated product')
    else:
        print(' ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(121)

    return return_code


def _terrain_flattening(infile, outfile, logfile, dem_dict,
                        ncores=os.cpu_count()):
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
        ncores(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count


    '''

    # get gpt file
    gpt_file = h.gpt_path()

    print(' INFO: Correcting for the illumination along slopes'
          ' (Terrain Flattening).'
          )
    # make dem file snap readable in case of no external dem
    if not dem_dict['dem file']:
        dem_dict['dem file'] = " "

    command = ('{} Terrain-Flattening -x -q {} '
               ' -PadditionalOverlap=0.15'
               ' -PoversamplingMultiple=1.5'
               ' -PdemName=\'{}\''
               ' -PexternalDEMFile=\'{}\''
               ' -PexternalDEMNoDataValue=\'{}\''
               ' -PdemResamplingMethod=\'{}\''
               ' -t {} {}'.format(
        gpt_file, ncores,
        dem_dict['dem name'], dem_dict['dem file'],
        dem_dict['dem nodata'], dem_dict['dem resampling'],
        outfile, infile)
    )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully applied the terrain flattening.')
    else:
        print(' ERROR: Terrain Flattening exited with an error.'
              ' See {} for Snap Error output'.format(logfile)
              )

    return return_code


def _speckle_filter(infile, outfile, logfile, ncores=os.cpu_count()):
    '''A wrapper around SNAP's Lee-Sigma Speckle Filter

    This function takes OST imported Sentinel-1 product and applies
    a standardised version of the Lee-Sigma Speckle Filter with
    SNAP's defaut values.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        ncores(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count

    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    print(' INFO: Applying the Lee-Sigma Speckle Filter')
    # contrcut command string
    command = '{} Speckle-Filter -x -q {} -PestimateENL=true \
              -t \'{}\' \'{}\''.format(gpt_file, ncores,
                                       outfile, infile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # hadle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully speckle-filtered product')
    else:
        print(' ERROR: Speckle Filtering exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(111)

    return return_code


def _linear_to_db(infile, outfile, logfile, ncores=os.cpu_count()):
    '''A wrapper around SNAP's linear to db routine

    This function takes an OST calibrated Sentinel-1 product
    and converts it to dB.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        ncores(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count

    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    print(' INFO: Converting the image to dB-scale.')
    # construct command string
    command = '{} LinearToFromdB -x -q {} -t \'{}\' {}'.format(
        gpt_file, ncores, outfile, infile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully converted product to dB-scale.')
    else:
        print(' ERROR: Linear to dB conversion exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(113)
    return return_code


def _ls_mask(infile, outfile, logfile, resolution, dem_dict,
             ncores=os.cpu_count()):
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
        ncores(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count


    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
    graph = opj(rootpath, 'graphs', 'S1_SLC2ARD', 'S1_SLC_LS_TC.xml')

    # make dem file snap readable in case of no external dem
    if not dem_dict['dem file']:
        dem_dict['dem file'] = " "

    print(" INFO: Compute Layover/Shadow mask")
    command = ('{} {} -x -q {}'
               ' -Pinput={}'
               ' -Presol={}'
               ' -Pdem=\'{}\''
               ' -Pdem_file=\'{}\''
               ' -Pdem_nodata=\'{}\''
               ' -Pdem_resampling=\'{}\''
               ' -Pimage_resampling=\'{}\''
               ' -Poutput={}'.format(
        gpt_file, graph, ncores, infile, resolution,
        dem_dict['dem name'], dem_dict['dem file'],
        dem_dict['dem nodata'], dem_dict['dem resampling'],
        dem_dict['image resampling'],
        outfile)
    )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully created Layover/Shadow mask')
    else:
        print(' ERROR: Layover/Shadow mask creation exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(121)

    return return_code


# def _coreg(filelist, outfile, logfile, dem_dict, ncores=os.cpu_count()):
#    '''A wrapper around SNAP's back-geocoding co-registration routine
#
#    This function takes a list of 2 OST imported Sentinel-1 SLC products
#    and co-registers them properly. This routine is sufficient for coherence
#    estimation, but not for InSAR, since the ESD refinement is not applied.
#
#    Args:
#        infile: string or os.path object for
#                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
#        outfile: string or os.path object for the output
#                 file written in BEAM-Dimap format
#        logfile: string or os.path object for the file
#                 where SNAP'S STDOUT/STDERR is written to
#        dem (str): A Snap compliant string for the dem to use.
#                   Possible choices are:
#                       'SRTM 1sec HGT' (default)
#                       'SRTM 3sec'
#                       'ASTER 1sec GDEM'
#                       'ACE30'
#        ncores(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count

#
#    '''
#
#    # get gpt file
#    gpt_file = h.gpt_path()
#
#    # get path to graph
#    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
#    graph = opj(rootpath, 'graphs', 'S1_SLC2ARD', 'S1_SLC_BGD.xml')
#
#    print(' INFO: Co-registering {}'.format(filelist[0]))
#    command = '{} {} -x -q {} -Pfilelist={} -Poutput={} -Pdem=\'{}\''\
#        .format(gpt_file, graph, ncores, filelist, outfile, dem)
#
#    return_code = h.run_command(command, logfile)
#
#    if return_code == 0:
#        print(' INFO: Succesfully coregistered product.')
#    else:
#        print(' ERROR: Co-registration exited with an error. \
#                See {} for Snap Error output'.format(logfile))
#        # sys.exit(112)
#
#    return return_code


def _coreg2(master, slave, outfile, logfile, dem_dict, ncores=os.cpu_count()):
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
        ncores(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count


    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
    graph = opj(rootpath, 'graphs', 'S1_SLC2ARD', 'S1_SLC_Coreg.xml')

    # make dem file snap readable in case of no external dem
    if not dem_dict['dem file']:
        dem_dict['dem file'] = " "

    print(' INFO: Co-registering {} and {}'.format(master, slave))
    command = ('{} {} -x -q {} '
               ' -Pmaster={}'
               ' -Pslave={}'
               ' -Pdem=\'{}\''
               ' -Pdem_file=\'{}\''
               ' -Pdem_nodata=\'{}\''
               ' -Pdem_resampling=\'{}\''
               ' -Poutput={} '.format(
        gpt_file, graph, ncores,
        master, slave,
        dem_dict['dem name'], dem_dict['dem file'],
        dem_dict['dem nodata'], dem_dict['dem resampling'],
        outfile)
    )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully coregistered product.')
    else:
        print(' ERROR: Co-registration exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code


def _coherence(infile, outfile, logfile, polar='VV,VH,HH,HV',
               ncores=os.cpu_count()):
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
        ncores(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count


    '''

    # get gpt file
    gpt_file = h.gpt_path()

    # get path to graph
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
    graph = opj(rootpath, 'graphs', 'S1_SLC2ARD', 'S1_SLC_Coh_Deb.xml')

    print(' INFO: Coherence estimation')
    command = '{} {} -x -q {} -Pinput={} -Ppolar=\'{}\' -Poutput={}' \
        .format(gpt_file, graph, ncores, infile, polar, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully created coherence product.')
    else:
        print(' ERROR: Coherence exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code


def _terrain_correction(infile, outfile, logfile, resolution, dem_dict,
                        ncores=os.cpu_count()):
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
        ncores(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count


    '''

    # get gpt file
    gpt_file = h.gpt_path()

    print(" INFO: Geocoding input scene")
    # make dem file snap readable in case of no external dem
    if not dem_dict['dem file']:
        dem_dict['dem file'] = " "

    command = ('{} Terrain-Correction -x -q {}'
               ' -PdemName=\'{}\''
               ' -PexternalDEMFile=\'{}\''
               ' -PexternalDEMNoDataValue=\'{}\''
               ' -PdemResamplingMethod=\'{}\''
               ' -PimgResamplingMethod=\'{}\''
               ' -PnodataValueAtSea=\'false\''
               ' -PpixelSpacingInMeter=\'{}\''
               ' -t {} {}'.format(
        gpt_file, ncores,
        dem_dict['dem name'], dem_dict['dem file'],
        dem_dict['dem nodata'], dem_dict['dem resampling'],
        dem_dict['image resampling'],
        resolution, outfile, infile)
    )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully orthorectified product.')
    else:
        print(' ERROR: Geocoding exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code

# def _terrain_correction_deg(infile, outfile, logfile, resolution=0.001,
#                            dem='SRTM 1sec HGT', ncores=os.cpu_count()):
#    '''A wrapper around SNAP's Terrain Correction routine
#
#    This function takes an OST calibrated Sentinel-1 product and
#    does the geocodification.
#
#    Args:
#        infile: string or os.path object for
#                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
#        outfile: string or os.path object for the output
#                 file written in BEAM-Dimap format
#        logfile: string or os.path object for the file
#                 where SNAP'S STDOUT/STDERR is written to
#        resolution (int): the resolution of the output product in degree
#        dem (str): A Snap compliant string for the dem to use.
#                   Possible choices are:
#                       'SRTM 1sec HGT' (default)
#                       'SRTM 3sec'
#                       'ASTER 1sec GDEM'
#                       'ACE30'
#        ncores(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count

#
#    '''
#
#    # get gpt file
#    gpt_file = h.gpt_path()
#
#    print(" INFO: Geocoding input scene")
#    command = '{} Terrain-Correction -x -q {} \
#              -PdemResamplingMethod=\'BILINEAR_INTERPOLATION\' \
#              -PimgResamplingMethod=\'BILINEAR_INTERPOLATION\' \
#              -PnodataValueAtSea=\'false\' \
#              -PpixelSpacingInDegree=\'{}\' \
#              -PdemName=\'{}\' \
#              -t {} {}' \
#              .format(gpt_file, ncores, resolution, dem,
#                      outfile, infile)
#
#    return_code = h.run_command(command, logfile)
#
#    if return_code == 0:
#        print(' INFO: Succesfully orthorectified product.')
#    else:
#        print(' ERROR: Geocoding exited with an error. \
#                See {} for Snap Error output'.format(logfile))
#
#    return return_code