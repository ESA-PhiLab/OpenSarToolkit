# -*- coding: utf-8 -*-

# import stdlib modules
import os
import importlib

from os.path import join as opj
from ost.helpers import helpers as h


def _calibration(infile, outfile, logfile, calibrate_to):
    
    # transform calibration parameter to snap readable
    sigma0, beta0, gamma0 = 'false', 'false', 'false'
    
    if calibrate_to is 'gamma0':
        gamma0 = 'true'
    elif calibrate_to is 'beta0':
        beta0 = 'true'
    elif calibrate_to is 'sigma0':
        sigma0 = 'true'
        
    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    print(' INFO: Calibrating the product to {}.'.format(calibrate_to))
    # contrcut command string
    
    command = ('{} Calibration -x -q {}'
                   ' -PoutputBetaBand=\'{}\''
                   ' -PoutputGammaBand=\'{}\''
                   ' -PoutputSigmaBand=\'{}\''
                   ' -t \'{}\' \'{}\''.format(
                          gpt_file, 2 * os.cpu_count(),
                          beta0, gamma0, sigma0,
                          outfile, infile)
    )
    
    # run command and get return code
    return_code = h.run_command(command, logfile)

    # hadle errors and logs
    if return_code == 0:
        print(' INFO: Calibration to {} successful.'.format(calibrate_to))
    else:
        print(' ERROR: Calibration exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code

  
def _multi_look(infile, outfile, logfile, rg_looks, az_looks):
    
    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    print(' INFO: Multi-looking the image with {} looks in'
          ' azimuth and {} looks in range.'.format(az_looks, rg_looks))
    
    # construct command string
    command = ('{} Multilook -x -q {}'
                ' -PnAzLooks={}'
                ' -PnRgLooks={}'
                ' -t \'{}\' {}'.format(
                        gpt_file, 2 * os.cpu_count(), 
                        az_looks, rg_looks,
                        outfile, infile
                        )
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully multi-looked product.')
    else:
        print(' ERROR: Multi-look exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code

            
def _speckle_filter(infile, outfile, logfile, speckle_dict):
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
    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    print(' INFO: Applying speckle filtering.')
    # contrcut command string
    command = ('{} Speckle-Filter -x -q {}'
                  ' -PestimateENL=\'{}\''
                  ' -PanSize=\'{}\''
                  ' -PdampingFactor=\'{}\''
                  ' -Penl=\'{}\''
                  ' -Pfilter=\'{}\''
                  ' -PfilterSizeX=\'{}\''
                  ' -PfilterSizeY=\'{}\''
                  ' -PnumLooksStr=\'{}\''
                  ' -PsigmaStr=\'{}\''
                  ' -PtargetWindowSizeStr=\"{}\"'
                  ' -PwindowSize=\"{}\"'
                  ' -t \'{}\' \'{}\''.format(
                      gpt_file, 2 * os.cpu_count(),
                      speckle_dict['estimate ENL'],
                      speckle_dict['pan size'],
                      speckle_dict['damping'],
                      speckle_dict['ENL'],
                      speckle_dict['filter'],
                      speckle_dict['filter x size'],
                      speckle_dict['filter y size'],
                      speckle_dict['num of looks'],
                      speckle_dict['sigma'],
                      speckle_dict['target window size'],
                      speckle_dict['window size'],
                      outfile, infile)
              )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # hadle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully applied speckle filtering.')
    else:
        print(' ERROR: Speckle Filtering exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code


def _terrain_flattening(infile, outfile, logfile, dem_dict):
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

    '''

    # get gpt file
    gpt_file = h.gpt_path()

    print(' INFO: Correcting for the illumination along slopes'
          ' (Terrain Flattening).'
    )
    
    if not dem_dict['dem file']:
        dem_dict['dem file'] = " "
        
        
    command = ('{} Terrain-Flattening -x -q {}'
               ' -PadditionalOverlap=0.15'
               ' -PoversamplingMultiple=1.5'
               ' -PdemName=\'{}\''
               ' -PexternalDEMFile=\'{}\''
               ' -PexternalDEMNoDataValue=\'{}\''
               ' -PexternalDEMApplyEGM=\'{}\''
               ' -PdemResamplingMethod=\'{}\''
               ' -t {} {}'.format(
                   gpt_file, 2 * os.cpu_count(), 
                   dem_dict['dem name'], dem_dict['dem file'], 
                   dem_dict['dem nodata'], 
                   str(dem_dict['egm correction']).lower(),
                   dem_dict['dem resampling'],
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

    print(' INFO: Converting the image to dB-scale.')
    # construct command string
    command = '{} LinearToFromdB -x -q {} -t \'{}\' {}'.format(
        gpt_file, 2 * os.cpu_count(), outfile, infile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully converted product to dB-scale.')
    else:
        print(' ERROR: Linear to dB conversion exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code


def _terrain_correction(infile, outfile, logfile, resolution, dem_dict):
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

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()
    
    # make dem file snap readable in case of no external dem
    if not dem_dict['dem file']:
        dem_dict['dem file'] = " "
        
    command = ('{} Terrain-Correction -x -q {}'
            ' -PdemName=\'{}\''
            ' -PdemResamplingMethod=\'{}\''
            ' -PexternalDEMFile=\'{}\''
            ' -PexternalDEMNoDataValue={}'
            ' -PexternalDEMApplyEGM=\'{}\''
            ' -PimgResamplingMethod=\'{}\''
            #' -PmapProjection={}'
            ' -PpixelSpacingInMeter={}'
            ' -t \'{}\' \'{}\''.format(
                    gpt_file, 2 * os.cpu_count(), 
                    dem_dict['dem name'], dem_dict['dem resampling'],
                    dem_dict['dem file'], dem_dict['dem nodata'], 
                    str(dem_dict['egm correction']).lower(), 
                    dem_dict['image resampling'], 
                    resolution, outfile, infile
                    )
    )
    
    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully terrain corrected product')
    else:
        print(' ERROR: Terain Correction exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code


def _ls_mask(infile, outfile, logfile, resolution, dem_dict):
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

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    # get path to ost package
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]

    print(' INFO: Creating the Layover/Shadow mask')
    # get path to workflow xml
    graph = opj(rootpath, 'graphs', 'S1_GRD2ARD', '3_LSmap.xml')

    # construct command string
#    command = '{} {} -x -q {} -Pinput=\'{}\' -Presol={} -Pdem=\'{}\' \
#             -Poutput=\'{}\''.format(gpt_file, graph, 2 * os.cpu_count(),
#                                     infile, resolution, dem, outfile)
    command = ('{} {} -x -q {} -Pinput=\'{}\' -Presol={} ' 
                                 ' -Pdem=\'{}\'' 
                                 ' -Pdem_file=\'{}\''
                                 ' -Pdem_nodata=\'{}\'' 
                                 ' -Pdem_resampling=\'{}\''
                                 ' -Pimage_resampling=\'{}\''
                                 ' -Pegm_correction=\'{}\''
                                 ' -Poutput=\'{}\''.format(
            gpt_file, graph, 2 * os.cpu_count(), infile, resolution, 
            dem_dict['dem name'], dem_dict['dem file'], dem_dict['dem nodata'], 
            dem_dict['dem resampling'], dem_dict['image resampling'], 
            str(dem_dict['egm correction']).lower(), outfile)
    )
    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully created a Layover/Shadow mask')
    else:
        print(' ERROR: Layover/Shadow mask creation exited with an error. \
                See {} for Snap Error output'.format(logfile))
        
    return return_code