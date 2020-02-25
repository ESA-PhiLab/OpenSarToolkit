# -*- coding: utf-8 -*-
import os
from os.path import join as opj
import importlib
import json
import sys

from ost.helpers import helpers as h
from ost.snap_common import common

def _import(infile, out_prefix, logfile, swath, burst, polar='VV,VH,HH,HV',ncores=os.cpu_count()):
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
              pol_speckle_dict=None,ncores=os.cpu_count()):
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


def _calibration(infile, outfile, logfile, product_type='GTCgamma',ncores=os.cpu_count()):
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
        return_code=0
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


def _terrain_flattening(infile, outfile, logfile, dem_dict, ncores=os.cpu_count()):
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


def _ls_mask(infile, outfile, logfile, resolution, dem_dict, ncores=os.cpu_count()):
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


#def _coreg(filelist, outfile, logfile, dem_dict, ncores=os.cpu_count()):
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


def _coreg2(master, slave,  outfile, logfile, dem_dict, ncores=os.cpu_count()):
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


def _coherence(infile, outfile, logfile, polar='VV,VH,HH,HV', ncores=os.cpu_count()):
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


def _terrain_correction(infile, outfile, logfile, resolution, dem_dict, ncores=os.cpu_count()):
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


#def _terrain_correction_deg(infile, outfile, logfile, resolution=0.001,
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


def burst_to_ard(master_file,
                 swath,
                 master_burst_nr,
                 master_burst_id,
                 proc_file,
                 out_dir,
                 temp_dir,
                 slave_file=None,
                 slave_burst_nr=None,
                 slave_burst_id=None,
                 coherence=False,
                 remove_slave_import=False,
                 ncores=os.cpu_count()):
    '''The main routine to turn a burst into an ARD product

    Args:
        master_file (str): path to full master SLC scene
        swath (str): subswath
        master_burst_nr (): index number of the burst
        master_burst_id ():
        out_dir (str):
        temp_dir (str):
        slave_file (str):
        slave_burst_nr (str):
        slave_burst_id (str):
        proc_file (str):
        remove_slave_import (bool):
        ncores (int): number of cpus used - useful for parallel processing
    '''
    if type(remove_slave_import) == str:
        if remove_slave_import == 'True':
            remove_slave_import = True
        elif remove_slave_import == 'False':
            remove_slave_import = False
    if type(coherence) == str:
        if coherence == 'True':
            coherence = True
        elif coherence == 'False':
            coherence = False
    # load ards
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        ard = ard_params['single ARD']
     
    # ---------------------------------------------------------------------
    # 1 Import
    # import master
    master_import = opj(temp_dir, '{}_import'.format(master_burst_id))

    if not os.path.exists('{}.dim'.format(master_import)):
        import_log = opj(out_dir, '{}_import.err_log'.format(master_burst_id))
        polars = ard['polarisation'].replace(' ', '')
        return_code = _import(master_file, master_import, import_log,
                              swath, master_burst_nr, polars, ncores
        )
        if return_code != 0:
            h.delete_dimap(master_import)
            return return_code

    imported = '{}.dim'.format(master_import)
    # ---------------------------------------------------------------------
    # 2 H-A-Alpha
    if ard['H-A-Alpha']:
        # create HAalpha file
        out_haa = opj(temp_dir, '{}_h'.format(master_burst_id))
        haa_log = opj(out_dir, '{}_haa.err_log'.format(master_burst_id))
        return_code = _ha_alpha(imported,
                                out_haa, haa_log, 
                                ard['remove pol speckle'], 
                                ard['pol speckle filter'],
                                ncores
        )

        # delete files in case of error
        if return_code != 0:
            h.delete_dimap(out_haa)
            h.delete_dimap(master_import)
            return return_code

        # geo code HAalpha
        out_htc = opj(temp_dir, '{}_pol'.format(master_burst_id))
        haa_tc_log = opj(out_dir, '{}_haa_tc.err_log'.format(
            master_burst_id))
        return_code = common._terrain_correction(
            '{}.dim'.format(out_haa), out_htc, haa_tc_log, 
            ard['resolution'], ard['dem'], ncores
        )

        # remove HAalpha tmp files
        h.delete_dimap(out_haa)
        
        # last check on the output files
        return_code = h.check_out_dimap(out_htc)
        if return_code != 0:
            h.delete_dimap(out_htc)
            h.delete_dimap(master_import)
            return return_code

        # move to final destination
        h.move_dimap(out_htc, opj(out_dir, '{}_pol'.format(master_burst_id)))

    # ---------------------------------------------------------------------
    # 3 Calibration
    out_cal = opj(temp_dir, '{}_cal'.format(master_burst_id))
    cal_log = opj(out_dir, '{}_cal.err_log'.format(master_burst_id))
    return_code = _calibration(imported, out_cal, cal_log, ard['product type'],ncores)
    
    # delete output if command failed for some reason and return
    if return_code != 0:
        h.delete_dimap(out_cal)
        h.delete_dimap(master_import)
        return return_code

    if not coherence:
        #  remove imports
        h.delete_dimap(master_import)

    # ---------------------------------------------------------------------
    # 4 Speckle filtering
    if ard['remove speckle']:
        speckle_import = opj(temp_dir, '{}_speckle_import'.format(
            master_burst_id))
        speckle_log = opj(out_dir, '{}_speckle.err_log'.format(
            master_burst_id))
        return_code = common._speckle_filter('{}.dim'.format(out_cal),
                                             speckle_import, speckle_log,
                                             ard['speckle filter'], ncores
                                             )
        
        # remove input 
        h.delete_dimap(out_cal)

        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(speckle_import)
            h.delete_dimap(master_import)
            return return_code

        # reset master_import for follwoing routine
        out_cal = speckle_import

    # ---------------------------------------------------------------------
    # 5 Terrain Flattening
    if ard['product type'] == 'RTC-gamma0':
        # define outfile
        out_rtc = opj(temp_dir, '{}_rtc'.format(master_burst_id))
        rtc_log = opj(out_dir, '{}_rtc.err_log'.format(
            master_burst_id))
        # do the TF
        return_code = common._terrain_flattening('{}.dim'.format(out_cal),
                                                 out_rtc, rtc_log, ard['dem'], ncores)
        
        # remove tmp files
        h.delete_dimap(out_cal)
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(out_rtc)
            h.delete_dimap(master_import)
            return return_code

        # set out_rtc to out_cal for further processing
        out_cal = out_rtc

    # ---------------------------------------------------------------------
    # 7 to dB scale
    if ard['to db']:
        out_db = opj(temp_dir, '{}_cal_db'.format(master_burst_id))
        db_log = opj(out_dir, '{}_cal_db.err_log'.format(master_burst_id))
        return_code = common._linear_to_db('{}.dim'.format(out_cal), out_db, db_log, ncores)
        
        # remove tmp files
        h.delete_dimap(out_cal)
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(out_db)
            h.delete_dimap(master_import)
            return return_code

        # set out_cal to out_db for further processing
        out_cal = out_db
 
    # ---------------------------------------------------------------------
    # 8 Geocode backscatter
    if ard['product type'] != "Coherence_only":
        out_tc = opj(temp_dir, '{}_bs'.format(master_burst_id))
        tc_log = opj(out_dir, '{}_bs_tc.err_log'.format(master_burst_id))
        return_code = common._terrain_correction(
            '{}.dim'.format(out_cal), out_tc, tc_log,
            ard['resolution'], ard['dem'], ncores)

        # last check on backscatter data
        return_code = h.check_out_dimap(out_tc)
        if return_code != 0:
            h.delete_dimap(out_tc)
            return return_code

        # we move backscatter to final destination
        h.move_dimap(out_tc, opj(out_dir, '{}_bs'.format(master_burst_id)))

    # ---------------------------------------------------------------------
    # 9 Layover/Shadow mask
    if ard['create ls mask']:
        
        out_ls = opj(temp_dir, '{}_LS'.format(master_burst_id))
        ls_log = opj(out_dir, '{}_LS.err_log'.format(master_burst_id))
        return_code = common._ls_mask('{}.dim'.format(out_cal), out_ls, ls_log,
                                      ard['resolution'], ard['dem'], ncores)
        if return_code != 0:
            h.delete_dimap(out_ls)
            return return_code

        # last check on ls data
        return_code = h.check_out_dimap(out_ls, test_stats=False)
        if return_code != 0:
            h.delete_dimap(out_ls)
            return return_code

        # move ls data to final destination
        h.move_dimap(out_ls, opj(out_dir, '{}_LS'.format(master_burst_id)))

    # remove calibrated files
    if ard['product type'] != "Coherence_only":
        h.delete_dimap(out_cal)

    if coherence:

        # import slave
        slave_import = opj(temp_dir, '{}_import'.format(slave_burst_id))
        import_log = opj(out_dir, '{}_import.err_log'.format(slave_burst_id))
        polars = ard['polarisation'].replace(' ', '')
        return_code = _import(slave_file, slave_import, import_log,
                              swath, slave_burst_nr, polars, ncores)

        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

        # co-registration
        #filelist = ['{}.dim'.format(master_import),
        #            '{}.dim'.format(slave_import)]
        #filelist = '\'{}\''.format(','.join(filelist))
        out_coreg = opj(temp_dir, '{}_coreg'.format(master_burst_id))
        coreg_log = opj(out_dir, '{}_coreg.err_log'.format(master_burst_id))
        # return_code = _coreg(filelist, out_coreg, coreg_log, dem)
        return_code = _coreg2('{}.dim'.format(master_import),
                              '{}.dim'.format(slave_import),
                               out_coreg,
                               coreg_log, ard['dem'], ncores)
        
        # remove imports
        h.delete_dimap(master_import)
        
        if remove_slave_import is True:
            h.delete_dimap(slave_import)
        
        # delete output if command failed for some reason and return   
        if return_code != 0:
            h.delete_dimap(out_coreg)
            h.delete_dimap(slave_import)
            return return_code

        # calculate coherence and deburst
        out_coh = opj(temp_dir, '{}_c'.format(master_burst_id))
        coh_log = opj(out_dir, '{}_coh.err_log'.format(master_burst_id))
        coh_polars = ard['coherence bands'].replace(' ', '')
        return_code = _coherence('{}.dim'.format(out_coreg),
                                 out_coh, coh_log, coh_polars, ncores)
        
        # remove coreg tmp files
        h.delete_dimap(out_coreg)
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(out_coh)
            h.delete_dimap(slave_import)
            return return_code

        # geocode
        out_tc = opj(temp_dir, '{}_coh'.format(master_burst_id))
        tc_log = opj(out_dir, '{}_coh_tc.err_log'.format(master_burst_id))
        return_code = common._terrain_correction(
            '{}.dim'.format(out_coh), out_tc, tc_log, 
            ard['resolution'], ard['dem'], ncores)
        
        # remove tmp files
        h.delete_dimap(out_coh)
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(out_tc)
            h.delete_dimap(slave_import)
            return return_code
        
        # last check on coherence data
        return_code = h.check_out_dimap(out_tc)
        if return_code != 0:
            h.delete_dimap(out_tc)
            return return_code

        # move to final destination
        h.move_dimap(out_tc, opj(out_dir, '{}_coh'.format(master_burst_id)))

    # write out check file for tracking that it is processed
    with open(opj(out_dir, '.processed'), 'w') as file:
        file.write('passed all tests \n')
    
    return return_code


if __name__ == "__main__":

    import argparse

    # write a description
    descript = """
               This is a command line client for the creation of
               Sentinel-1 ARD data from Level 1 SLC bursts

               to do
               """

    epilog = """
             Example:
             to do


             """


    # create a parser
    parser = argparse.ArgumentParser(description=descript, epilog=epilog)

    # search paramenters
    parser.add_argument('-m', '--master',
                        help=' (str) path to the master SLC',
                        required=True)
    parser.add_argument('-ms', '--master_swath',
                        help=' (str) The subswath of the master SLC',
                        required=True)
    parser.add_argument('-mn', '--master_burst_nr',
                        help=' (int) The index number of the master burst',
                        required=True)
    parser.add_argument('-mi', '--master_burst_id',
                        help=' (str) The OST burst id of the master burst')
    parser.add_argument('-o', '--out_directory',
                        help='The directory where the outputfiles will'
                             ' be written to.',
                        required=True)
    parser.add_argument('-t', '--temp_directory',
                        help='The directory where temporary files will'
                             ' be written to.',
                        required=True)
    parser.add_argument('-s', '--slave',
                        help=' (str) path to the slave SLC',
                        default=False)
    parser.add_argument('-sn', '--slave_burst_nr',
                        help=' (int) The index number of the slave burst',
                        default=False)
    parser.add_argument('-si', '--slave_burst_id',
                        help=' (str) The OST burst id of the slave burst',
                        default=False)
    parser.add_argument('-c', '--coherence',
                        help=' (bool) Set to True if the interferometric '
                        'coherence should be calculated.',
                        default=False)
    parser.add_argument('-p', '--proc_file',
                        help=' (str/path) Path to ARDprocessing parameters file',
                        required=True)
    parser.add_argument('-rsi', '--remove_slave_import',
                        help=' (bool) Select if during the coherence'
                             ' calculation the imported slave file should be'
                             ' deleted (for time-series it is advisable to'
                             ' keep it)',
                        default=False)
    parser.add_argument('-nc', '--cpu_cores',
                        help=' (int) Select the number of cpu cores'
                             ' for running each gpt process'
                             'if you wish to specify for parallelisation',
                        default=False)

    args = parser.parse_args()

    # execute processing
    burst_to_ard(args.master, args.master_swath, args.master_burst_nr, 
                 args.master_burst_id, args.proc_file, args.out_directory, args.temp_directory,
                 args.slave, args.slave_burst_nr, args.slave_burst_id,
                 args.coherence, args.remove_slave_import,args.cpu_cores)
