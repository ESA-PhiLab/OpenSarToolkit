import os
from os.path import join as opj
import sys
import json
import logging

import numpy as np
from retrying import retry

from ost.settings import GPT_FILE, OST_ROOT
from ost.errors import GPTRuntimeError
from ost.helpers import helpers as h

logger = logging.getLogger(__name__)


@retry(stop_max_attempt_number=3, wait_fixed=1)
def burst_import(infile, outfile, logfile, swath, burst, polar='VV,VH,HH,HV',
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
                     default: 'VV,VH,HH,HV'
        ncores(int): the number of cpu cores to allocate to the gpt job,
                default: os.cpu_count()
    '''


    # get path to graph
    graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD', 'S1_SLC_BurstSplit_AO.xml')

    logger.info('Importing Burst {} from Swath {}'
          ' from scene {}'.format(burst, swath, os.path.basename(infile)))

    command = '{} {} -x -q {} -Pinput={} -Ppolar={} -Pswath={}\
                      -Pburst={} -Poutput={}' \
        .format(GPT_FILE, graph, ncores, infile, polar, swath,
                burst, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully imported product')
        return return_code
    else:
        raise GPTRuntimeError(
            'ERROR: Frame import exited with an error {}. See {} for '
            'Snap Error output'.format(return_code, logfile)
        )


@retry(stop_max_attempt_number=3, wait_fixed=1)
def ha_alpha(infile, outfile, logfile, pol_speckle_filter=False,
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

    if pol_speckle_filter:
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_Deb_Spk_Halpha.xml')
        logger.info('Applying the polarimetric speckle filter and'
              ' calculating the H-alpha dual-pol decomposition')
        command = ('{} {} -x -q {} -Pinput={} -Poutput={}'
                   ' -Pfilter=\'{}\''
                   ' -Pfilter_size=\'{}\''
                   ' -Pnr_looks={}'
                   ' -Pwindow_size={}'
                   ' -Ptarget_window_size={}'
                   ' -Ppan_size={}'
                   ' -Psigma={}'.format(
            GPT_FILE, graph, ncores,
            infile, outfile,
            pol_speckle_dict['filter'],
            pol_speckle_dict['filter_size'],
            pol_speckle_dict['num_of_looks'],
            pol_speckle_dict['window_size'],
            pol_speckle_dict['target_window_size'],
            pol_speckle_dict['pan_size'],
            pol_speckle_dict['sigma']
        )
        )
    else:
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_Deb_Halpha.xml')

        print(" INFO: Calculating the H-alpha dual polarisation")
        command = '{} {} -x -q {} -Pinput={} -Poutput={}' \
            .format(GPT_FILE, graph, ncores, infile, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully created H/A/Alpha product')
    else:
        raise GPTRuntimeError('ERROR: H/Alpha exited with an error {}. \
                See {} for Snap Error output'.format(return_code, logfile)
                              )

@retry(stop_max_attempt_number=3, wait_fixed=1)
def calibration(infile, outfile, logfile, proc_file,
                region='', ncores=os.cpu_count()):
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

    # load ards
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing_parameters']
        ard = ard_params['single_ARD']
        dem_dict = ard['dem']

    # calculate Multi-Look factors
    azimuth_looks = 1   #int(np.floor(ard['resolution'] / 10 ))
    range_looks = 5   #int(azimuth_looks * 5)

    # construct command dependent on selected product type
    if ard['product_type'] == 'RTC-gamma0':
        logger.debug('INFO: Calibrating the product to a RTC product.')

        # get graph for RTC generation
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_CalBeta_Deb_ML_TF_Sub.xml')

        # construct command
        command = '{} {} -x -q {} ' \
                  '-Prange_looks={} -Pazimuth_looks={} ' \
                  '-Pdem=\'{}\' -Pdem_file="{}" -Pdem_nodata={} ' \
                  '-Pdem_resampling={} -Pregion="{}" ' \
                  '-Pinput={} -Poutput={}'.format(
            GPT_FILE, graph, ncores,
            range_looks, azimuth_looks,
            dem_dict['dem_name'], dem_dict['dem_file'],
            dem_dict['dem_nodata'], dem_dict['dem_resampling'],
            region, infile, outfile)


    elif ard['product_type'] == 'GTC-gamma0':

        logger.info('Calibrating the product to a GTC product (Gamma0).')

        # get graph for GTC gammao0 generation
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_CalGamma_Deb_ML_Sub.xml')

        # construct command
        command = '{} {} -x -q {} ' \
                  '-Prange_looks={} -Pazimuth_looks={} ' \
                  '-Pregion="{}" -Pinput={} -Poutput={}' \
            .format(GPT_FILE, graph, ncores,
                    range_looks, azimuth_looks,
                    region, infile, outfile)

    elif ard['product_type'] == 'GTC-sigma0':
        logger.debug(
            'INFO: Calibrating the product to a GTC product (Sigma0).'
        )

        # get graph for GTC-gamma0 generation
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_CalSigma_Deb_ML_Sub.xml')

        # construct command
        command = '{} {} -x -q {} ' \
                  '-Prange_looks={} -Pazimuth_looks={} ' \
                  '-Pregion="{}" -Pinput={} -Poutput={}' \
            .format(GPT_FILE, graph, ncores,
                    range_looks, azimuth_looks,
                    region, infile, outfile)
    else:
        logger.debug('ERROR: Wrong product type selected.')
        sys.exit(121)

    logger.debug("INFO: Removing thermal noise, calibrating and debursting")
    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug('INFO: Succesfully calibrated product')
        return return_code
    else:
        raise GPTRuntimeError('ERROR: Calibration exited with an error {}. \
                        See {} for Snap Error output'.format(return_code,
                                                             logfile)
        )



# @retry(stop_max_attempt_number=3, wait_fixed=1)
# def _calibration(infile, outfile, logfile, product_type='GTC-gamma0',
#                  ncores=os.cpu_count()):
#     '''A wrapper around SNAP's radiometric calibration
#
#     This function takes OST imported Sentinel-1 product and generates
#     it to calibrated backscatter.
#
#     3 different calibration modes are supported.
#         - Radiometrically terrain corrected Gamma nought (RTC)
#           NOTE: that the routine actually calibrates to bet0 and needs to
#           be used together with _terrain_flattening routine
#         - ellipsoid based Gamma nought (GTCgamma)
#         - Sigma nought (GTCsigma).
#
#     Args:
#         infile: string or os.path object for
#                 an OST imported frame in BEAM-Dimap format (i.e. *.dim)
#         outfile: string or os.path object for the output
#                  file written in BEAM-Dimap format
#         logfile: string or os.path object for the file
#                  where SNAP'S STDOUT/STDERR is written to
#         resolution (int): the resolution of the output product in meters
#         product_type (str): the product type of the output product
#                             i.e. RTC, GTCgamma or GTCsigma
#         ncores(int): the number of cpu cores to allocate to the gpt job,
#                 default: os.cpu_count()
#
#
#     '''
#
#     # get gpt file
#     GPT_FILE = h.gpt_path()
#
#     # get path to graph
#     OST_ROOT = importlib.util.find_spec('ost').submodule_search_locations[0]
#
#     if product_type == 'RTC-gamma0':
#         logger.info('Calibrating the product to beta0.')
#         graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
#                     'S1_SLC_TNR_Calbeta_Deb.xml')
#     elif product_type == 'GTC-gamma0':
#         logger.info('Calibrating the product to gamma0.')
#         graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
#                     'S1_SLC_TNR_CalGamma_Deb.xml')
#     elif product_type == 'GTC-sigma0':
#         logger.info('Calibrating the product to sigma0.')
#         graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
#                     'S1_SLC_TNR_CalSigma_Deb.xml')
#     elif product_type == 'Coherence_only':
#         print('INFO: No need to calibrate just for coherence')
#         return_code = 0
#         return return_code
#     else:
#         print(' ERROR: Wrong product type selected.')
#         sys.exit(121)
#
#     print(" INFO: Removing thermal noise, calibrating and debursting")
#     command = '{} {} -x -q {} -Pinput={} -Poutput={}' \
#         .format(GPT_FILE, graph, ncores, infile, outfile)
#
#     return_code = h.run_command(command, logfile)
#
#     if return_code == 0:
#         logger.info('Succesfully calibrated product')
#         return return_code
#     else:
#         raise GPTRuntimeError('ERROR: Frame import exited with an error {}. \
#                 See {} for Snap Error output'.format(return_code, logfile)
#                               )


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
#        ncores(int): the number of cpu cores to allocate to the gpt job,
#               default: os.cpu_count()

#
#    '''
#
#    # get gpt file
#    GPT_FILE = h.gpt_path()
#
#    # get path to graph
#    OST_ROOT = importlib.util.find_spec('ost').submodule_search_locations[0]
#    graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD', 'S1_SLC_BGD.xml')
#
#    logger.info('Co-registering {}'.format(filelist[0]))
#    command = '{} {} -x -q {} -Pfilelist={} -Poutput={} -Pdem=\'{}\''\
#        .format(GPT_FILE, graph, ncores, filelist, outfile, dem)
#
#    return_code = h.run_command(command, logfile)
#
#    if return_code == 0:
#        logger.info('Succesfully coregistered product.')
#    else:
#        print(' ERROR: Co-registration exited with an error. \
#                See {} for Snap Error output'.format(logfile))
#        # sys.exit(112)
#
#    return return_code


@retry(stop_max_attempt_number=3, wait_fixed=1)
def coreg2(master, slave, outfile, logfile, dem_dict, ncores=os.cpu_count()):
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

    # get path to graph
    graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD', 'S1_SLC_Coreg.xml')

    # make dem file snap readable in case of no external dem
    #if not dem_dict['dem file']:
    #    dem_dict['dem file'] = " "

    logger.info('Co-registering {} and {}'.format(master, slave))
    command = ('{} {} -x -q {} '
               ' -Pmaster={}'
               ' -Pslave={}'
               ' -Pdem=\'{}\''
               ' -Pdem_file=\'{}\''
               ' -Pdem_nodata=\'{}\''
               ' -Pdem_resampling=\'{}\''
               ' -Poutput={} '.format(
        GPT_FILE, graph, ncores,
        master, slave,
        dem_dict['dem_name'], dem_dict['dem_file'],
        dem_dict['dem_nodata'], dem_dict['dem_resampling'],
        outfile)
    )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully coregistered product.')
    else:
        raise GPTRuntimeError('ERROR: Co-registration exited with '
                              'an error {}. See {} for Snap '
                              'Error output'.format(return_code, logfile)
                              )


@retry(stop_max_attempt_number=3, wait_fixed=1)
def coherence(infile, outfile, logfile, ard,
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
        ncores(int): the number of cpu cores to allocate to the gpt job,
                default: os.cpu_count()
    '''

    # get path to graph
    graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD', 'S1_SLC_Coh_Deb.xml')
    polar = ard['coherence_bands'].replace(' ', '')
    logger.info('Coherence estimation')
    command = '{} {} -x -q {} ' \
              '-Pazimuth_window={} -Prange_window={} ' \
              '-Ppolar=\'{}\' -Pinput={} -Poutput={}' \
        .format(GPT_FILE, graph, ncores,
                ard['coherence_azimuth'], ard['coherence_range'],
                polar, infile, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully created coherence product.')
        return return_code
    else:
        raise GPTRuntimeError('ERROR: Coherence exited with an error {}. \
                See {} for Snap Error output'.format(return_code, logfile))
