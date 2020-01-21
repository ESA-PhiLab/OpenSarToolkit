# -*- coding: utf-8 -*-
import os
from os.path import join as opj
import logging
import sys
import rasterio
import warnings

from rasterio.errors import NotGeoreferencedWarning

from ost.helpers import helpers as h
from ost.settings import SNAP_S1_RESAMPLING_METHODS, OST_ROOT

logger = logging.getLogger(__name__)


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

    graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD', 'S1_SLC_BurstSplit_AO.xml')

    logger.debug('INFO: Importing Burst {} from Swath {} '
                 'from scene {}'.format(burst, swath, os.path.basename(infile))
                 )
    command = '{} {} -x -q {} -Pinput={} -Ppolar={} -Pswath={}\
                      -Pburst={} -Poutput={}'\
        .format(gpt_file, graph, 2, infile, polar, swath,
                burst, out_prefix)
    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug('INFO: Succesfully imported product')
    else:
        logger.debug('ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(119)

    return return_code


def _ha_alpha(infile, outfile, logfile, pol_speckle_filter=False):
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


    if pol_speckle_filter:
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_Deb_Spk_Halpha.xml'
                    )
    else:
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_Deb_Halpha.xml'
                    )

    logger.debug("INFO: Calculating the H-alpha dual polarisation")
    command = '{} {} -x -q {} -Pinput={} -Poutput={}'\
        .format(gpt_file, graph, 2, infile, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug('INFO: Succesfully created H/Alpha product')
    else:
        logger.debug('ERROR: H/Alpha exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(121)

    return return_code


def _calibration(infile,
                 outfile,
                 logfile,
                 product_type='GTCgamma',
                 dem='SRTM 1sec HGT',
                 resampling=SNAP_S1_RESAMPLING_METHODS[2],
                 dem_file='',
                 dem_nodata=0.0,
                 region=''
                 ):
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
    if product_type == 'RTC':
        logger.debug('INFO: Calibrating the product to a RTC product.')
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_Calbeta_Deb_ML_TF_SUB.xml')
        command = '{} {} -x -q {} -Pdem=\'{}\' -Pdem_file="{}" ' \
                  '-Pdem_nodata={} -Presampling={} -Pregion="{}" -Pinput={} ' \
                  '-Poutput={}' \
            .format(gpt_file, graph, 2, dem, dem_file,
                    dem_nodata, resampling, region, infile, outfile)
    elif product_type == 'GTCgamma':
        logger.debug('INFO: Calibrating the product to a GTC product (Gamma0).')
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_CalGamma_Deb.xml')
        command = '{} {} -x -q {} -Pinput={} -Poutput={}' \
            .format(gpt_file, graph, 2, infile, outfile)
    elif product_type == 'GTCsigma':
        logger.debug('INFO: Calibrating the product to a GTC product (Sigma0).')
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_CalSigma_Deb.xml')
        command = '{} {} -x -q {} -Pinput={} -Poutput={}' \
            .format(gpt_file, graph, 2, infile, outfile)
    else:
        logger.debug('ERROR: Wrong product type selected.')
        sys.exit(121)

    logger.debug("INFO: Removing thermal noise, calibrating and debursting")
    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug('INFO: Succesfully calibrated product')
    else:
        logger.debug('ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(121)

    return return_code


def _speckle_filter(infile, outfile, logfile):
    '''A wrapper around SNAP's Refined Lee Speckle Filter

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
    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    logger.debug('INFO: Applying the Refined-Lee Speckle Filter')
    # contrcut command string
    command = '{} Speckle-Filter -x -q {} -PestimateENL=true -Pfilter=\'Refined Lee\' \
              -t \'{}\' \'{}\''.format(gpt_file, 2,
                                       outfile, infile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # hadle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully imported product')
    else:
        logger.debug('ERROR: Speckle Filtering exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(111)

    return return_code


def _linear_to_db(infile, outfile, logfile):
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
    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    logger.debug('INFO: Converting the image to dB-scale.')
    # construct command string
    command = '{} LinearToFromdB -x -q {} -t \'{}\' {}'.format(
        gpt_file, 2, outfile, infile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully converted product to dB-scale.')
    else:
        logger.debug('ERROR: Linear to dB conversion exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(113)
    return return_code


def _ls_mask(infile, outfile, logfile, resolution, dem='SRTM 1sec HGT'):
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
                       'SRTM 1sec HGT'(default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get gpt file
    gpt_file = h.gpt_path()

    graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD', 'S1_SLC_LS_TC.xml')

    logger.debug("INFO: Compute Layover/Shadow mask")
    command = '{} {} -x -q {} -Pinput={} -Presol={} -Poutput={} -Pdem=\'{}\''\
        .format(gpt_file, graph, 2, infile, resolution,
                outfile, dem)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug('INFO: Succesfully created Layover/Shadow mask')
    else:
        logger.debug('ERROR: Layover/Shadow mask creation exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(121)

    return return_code


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
                       'SRTM 1sec HGT'(default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''
    # get gpt file
    gpt_file = h.gpt_path()

    graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD', 'S1_SLC_BGD.xml')

    logger.debug('INFO: Co-registering {}'.format(filelist[0]))
    command = '{} {} -x -q {} -Pfilelist={} -Poutput={} -Pdem=\'{}\''\
        .format(gpt_file, graph, 2, filelist, outfile, dem)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug('INFO: Succesfully coregistered product.')
    else:
        logger.debug('ERROR: Co-registration exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(112)

    return return_code


def _coreg2(master, slave,  outfile, logfile, dem='SRTM 1sec HGT', master_burst_poly=''):
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
                       'SRTM 1sec HGT'(default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''
    # get gpt file
    gpt_file = h.gpt_path()

    graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD', 'S1_SLC_Coreg.xml')

    logger.debug('INFO: Co-registering {} and {}'.format(master, slave))
    command = '{} {} -x -q {} -Pmaster={} -Pslave={} -Poutput={} ' \
              '-Pdem=\'{}\' -Pregion="{}"' \
        .format(gpt_file, graph, 2, master, slave,
                outfile, dem, master_burst_poly
                )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug('INFO: Succesfully coregistered product.')
    else:
        logger.debug('ERROR: Co-registration exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(112)

    return return_code


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

    graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD', 'S1_SLC_Coh_Deb.xml')

    logger.debug('INFO: Coherence estimation')
    command = '{} {} -x -q {} -Pinput={} -Poutput={}' \
        .format(gpt_file, graph, 2, infile, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug('INFO: Succesfully created coherence product.')
    else:
        logger.debug('ERROR: Coherence exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(121)

    return return_code


def _terrain_correction(infile, outfile, logfile, resolution,
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
                       'SRTM 1sec HGT'(default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get gpt file
    gpt_file = h.gpt_path()

    logger.debug("INFO: Geocoding input scene")

    command = '{} Terrain-Correction -x -q {} \
              -PdemResamplingMethod=\'BILINEAR_INTERPOLATION\'\
              -PimgResamplingMethod=\'BILINEAR_INTERPOLATION\'\
              -PnodataValueAtSea=\'false\'\
              -PpixelSpacingInMeter=\'{}\'\
              -PdemName=\'{}\'\
              -t {} {}'\
              .format(gpt_file, 2, resolution, dem,
                      outfile, infile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug('INFO: Succesfully orthorectified product.')
    else:
        logger.debug('ERROR: Geocoding exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(122)

    return return_code


def _terrain_correction_deg(infile, outfile, logfile, resolution=0.001,
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
        resolution (int): the resolution of the output product in degree
        dem (str): A Snap compliant string for the dem to use.
                   Possible choices are:
                       'SRTM 1sec HGT'(default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get gpt file
    gpt_file = h.gpt_path()

    logger.debug("INFO: Geocoding input scene")
    command = '{} Terrain-Correction -x -q {} \
              -PdemResamplingMethod=\'BILINEAR_INTERPOLATION\'\
              -PimgResamplingMethod=\'BILINEAR_INTERPOLATION\'\
              -PnodataValueAtSea=\'false\'\
              -PpixelSpacingInDegree=\'{}\'\
              -PdemName=\'{}\'\
              -t {} {}'\
              .format(gpt_file, 2, resolution, dem,
                      outfile, infile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.debug('INFO: Succesfully orthorectified product.')
    else:
        logger.debug('ERROR: Geocoding exited with an error. \
                See {} for Snap Error output'.format(logfile))
        # sys.exit(122)

    return return_code


def burst_to_ard(
        master_file,
        swath,
        master_burst_nr,
        master_burst_id,
        master_burst_poly,
        out_dir,
        out_prefix,
        temp_dir,
        polarimetry=False,
        pol_speckle_filter=False,
        resolution=20,
        product_type='GTCgamma',
        speckle_filter=False,
        to_db=False,
        ls_mask_create=False,
        dem='SRTM 1sec HGT'
):
    '''The main routine to turn a burst into an ARD product

    Args:
        master_file (str): path to full master SLC scene
        swath (str): subswath
        master_burst_nr (): index number of the burst
        master_burst_id ():
        master_burst_poly (): burst WKT used for faster calibration
        out_dir (str):
        temp_dir (str):
        slave_file (str):
        slave_burst_nr (str):
        slave_burst_id (str):
        coherence (bool):
        polarimetry (bool):
        pol_speckle_filter (bool):
        resolution (int):
        product_type (str):
        speckle_filter (bool):
        to_db (bool):
        ls_mask (bool):
        dem (str):
        remove_slave_import (bool):

    '''

    # Check for empty spaces in prefix
    out_prefix = out_prefix.replace(' ', '_')

    # import master
    master_import = opj(temp_dir, '{}_import'.format(master_burst_id))

    out_ard_path = opj(out_dir, '{}_{}_BS'.format(out_prefix, master_burst_id))
    if os.path.isfile(out_ard_path+'.dim'):
        return_code = 0
        logger.debug('File for burst %s and its swath exists, skipping!',
                     master_burst_id
                     )
        return return_code

    if not os.path.exists('{}.dim'.format(master_import)):
        import_log = opj(out_dir, '{}_import.err_log'.format(master_burst_id))
        return_code = _import(master_file, master_import, import_log,
                              swath, master_burst_nr)
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

    if polarimetry:
        # create HAalpha file
        out_haa = opj(temp_dir, '{}_h'.format(master_burst_id))
        haa_log = opj(out_dir, '{}_haa.err_log'.format(
            master_burst_id))
        return_code = _ha_alpha('{}.dim'.format(master_import),
                                out_haa, haa_log, pol_speckle_filter)

        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

        # geo code HAalpha
        out_htc = opj(temp_dir, '{}_ha_alpha'.format(master_burst_id))
        haa_tc_log = opj(out_dir, '{}_haa_tc.err_log'.format(
            master_burst_id))
        _terrain_correction(
            '{}.dim'.format(out_haa), out_htc, haa_tc_log, resolution, dem)

        # last check on the output files
        return_code = h.check_out_dimap(out_htc)
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

        # move to final destination
        h.move_dimap(
            out_htc, opj(out_dir, '{}_ha_alpha'.format(master_burst_id)))

        # remove HAalpha tmp files
        h.delete_dimap(out_haa)

    # Calibrate
    out_cal = opj(temp_dir, '{}_cal'.format(master_burst_id))
    cal_log = opj(out_dir, '{}_cal.err_log'.format(master_burst_id))
    return_code = _calibration(
        '{}.dim'.format(master_import),
        out_cal,
        cal_log,
        product_type,
        region=master_burst_poly
    )
    if return_code != 0:
        h.remove_folder_content(temp_dir)
        return return_code

    #  remove imports
    h.delete_dimap(master_import)

    # speckle filtering
    if speckle_filter:
        speckle_import = opj(temp_dir, '{}_speckle_import'.format(
            master_burst_id))
        speckle_log = opj(out_dir, '{}_speckle.err_log'.format(
            master_burst_id))
        return_code = _speckle_filter('{}.dim'.format(out_cal),
                                      speckle_import, speckle_log)
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

        # remove temp file
        h.delete_dimap(out_cal)

        # reset master_import for next routine
        out_cal = speckle_import

    if to_db:
        out_db = opj(temp_dir, '{}_cal_db'.format(master_burst_id))
        db_log = opj(out_dir, '{}_cal_db.err_log'.format(master_burst_id))
        return_code = _linear_to_db('{}.dim'.format(out_cal), out_db, db_log)
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

        # remove tmp files
        h.delete_dimap(out_cal)
        # set out_cal to out_db for further processing
        out_cal = out_db

    # geo code backscatter products
    out_tc = opj(temp_dir, '{}_{}_BS'.format(out_prefix, master_burst_id))
    tc_log = opj(out_dir, '{}_BS_tc.err_log'.format(master_burst_id))
    _terrain_correction(
        '{}.dim'.format(out_cal), out_tc, tc_log, resolution, dem)

    # last check on backscatter data
    return_code = h.check_out_dimap(out_tc)
    if return_code != 0:
        h.remove_folder_content(temp_dir)
        return return_code
    # we move backscatter to final destination
    h.move_dimap(out_tc, opj(out_dir, '{}_{}_BS'.format(out_prefix, master_burst_id)))

    if ls_mask_create:
        # create LS map
        out_ls = opj(temp_dir, '{}_{}_LS'.format(out_prefix, master_burst_id))
        ls_log = opj(out_dir, '{}_LS.err_log'.format(master_burst_id))
        return_code = _ls_mask('{}.dim'.format(out_cal), out_ls, ls_log,
                               resolution, dem)
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

        # last check on ls data
        return_code = h.check_out_dimap(out_ls, test_stats=False)
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

        # move ls data to final destination
        h.move_dimap(out_ls, opj(
            out_dir,
            '{}_{}_LS'.format(out_prefix, master_burst_id))
                     )

    # remove calibrated files
    h.delete_dimap(out_cal)

    # write file, so we know this burst has been succesfully processed
    if return_code == 0:
        check_file = opj(out_dir, '.processed')
        with open(str(check_file), 'w') as file:
            file.write('passed all tests \n')
    else:
        try:
            h.remove_folder_content(temp_dir)
            h.remove_folder_content(out_dir)
        except Exception as e:
            logger.debug(e)
    return return_code


def _2products_coherence_tc(
        master_scene,
        master_file,
        master_burst_poly,
        slave_scene,
        slave_file,
        out_dir,
        temp_dir,
        swath,
        master_burst_id,
        master_burst_nr,
        slave_burst_id,
        slave_burst_nr,
        resolution=20,
        dem='SRTM 1Sec HGT',
        dem_file='',
        resampling='BILINEAR_INTERPOLATION',
        polar='VV,VH,HH,HV'
):
    warnings.filterwarnings("ignore", category=NotGeoreferencedWarning)
    return_code = None
    # import master
    master_import = opj(temp_dir, '{}_import'.format(master_burst_id))
    if not os.path.exists('{}.dim'.format(master_import)):
        import_log = opj(out_dir, '{}_import.err_log'.format(master_burst_id))
        return_code = _import(
            infile=master_file,
            out_prefix=master_import,
            logfile=import_log,
            swath=swath,
            burst=master_burst_nr,
            polar=polar
        )
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code
    # check if master has data or not
    data_path = opj(temp_dir, '{}_import.data'.format(master_burst_id))
    if not os.path.exists(data_path):
        return 333
    for f in os.listdir(data_path):
        if f.endswith('.img') and 'q' in f:
            f = opj(data_path, f)
            with rasterio.open(f, 'r') as in_img:
                if not in_img.read(1).any():
                    return_code = 333
                else:
                    return_code = 0
    if return_code != 0:
        #  remove imports
        h.delete_dimap(master_import)
        return return_code
    # import slave
    slave_import = opj(temp_dir, '{}_slave_import'.format(slave_burst_id))
    import_log = opj(out_dir, '{}_slave_import.err_log'.format(slave_burst_id))
    return_code = _import(
        infile=slave_file,
        out_prefix=slave_import,
        logfile=import_log,
        swath=swath,
        burst=slave_burst_nr,
        polar=polar
    )
    if return_code != 0:
        h.remove_folder_content(temp_dir)
        return return_code
    # check if slave has data or not
    data_path = opj(temp_dir, '{}_slave_import.data'.format(master_burst_id))
    if not os.path.exists(data_path):
        return 333
    for f in os.listdir(data_path):
        if f.endswith('.img') and 'q' in f:
            f = opj(data_path, f)
            with rasterio.open(f, 'r') as in_img:
                if not in_img.read(1).any():
                    return_code = 333
                else:
                    return_code = 0
    if return_code != 0:
        #  remove imports
        h.delete_dimap(slave_import)
        return return_code

    # co-registration
    out_coreg = opj(temp_dir, '{}_coreg'.format(master_burst_id))
    coreg_log = opj(out_dir, '{}_coreg.err_log'.format(master_burst_id))
    logger.debug('{}.dim'.format(master_import))
    logger.debug('{}.dim'.format(slave_import))
    return_code = _coreg2('{}.dim'.format(master_import),
                          '{}.dim'.format(slave_import),
                          out_coreg,
                          coreg_log,
                          dem,
                          master_burst_poly
                          )
    if return_code != 0:
        h.remove_folder_content(temp_dir)
        return return_code

    #  remove imports
    h.delete_dimap(master_import)
    h.delete_dimap(slave_import)

    # calculate coherence and deburst
    out_coh = opj(temp_dir, '{}_c'.format(master_burst_id))
    coh_log = opj(out_dir, '{}_coh.err_log'.format(master_burst_id))
    return_code = _coherence('{}.dim'.format(out_coreg),
                             out_coh, coh_log
                             )
    if return_code != 0:
        h.remove_folder_content(temp_dir)
        return return_code

    # remove coreg tmp files
    h.delete_dimap(out_coreg)

    # geocode
    out_tc = opj(temp_dir, '{}_{}_{}_coh'.format(master_scene.start_date,
                                                 slave_scene.start_date,
                                                 master_burst_id
                                                 )
                 )
    tc_log = opj(out_dir, '{}_coh_tc.err_log'.format(master_burst_id)
                 )
    _terrain_correction(
        '{}.dim'.format(out_coh),
        out_tc,
        tc_log,
        resolution,
        dem
    )
    # last check on coherence data
    return_code = h.check_out_dimap(out_tc)
    if return_code != 0:
        h.remove_folder_content(temp_dir)
        return return_code

    # move to final destination
    h.move_dimap(out_tc, opj(out_dir, '{}_{}_{}_coh'.format(master_scene.start_date,
                                                            slave_scene.start_date,
                                                            master_burst_id)
                             )
                 )
    # remove tmp files
    h.delete_dimap(out_coh)

    # write file, so we know this burst has been succesfully processed
    if return_code == 0:
        check_file = opj(out_dir, '.processed')
        with open(str(check_file), 'w') as file:
            file.write('passed all tests \n')
    else:
        h.remove_folder_content(temp_dir)
        h.remove_folder_content(out_dir)
    return return_code
