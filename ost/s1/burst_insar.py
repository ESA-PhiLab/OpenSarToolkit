# -*- coding: utf-8 -*-
import importlib
import json
import os
from os.path import join as opj

from ost import Sentinel1_Scene as S1Scene
from ost.helpers import helpers as h
from ost.s1 import slc_wrappers as slc


def _coreg_slave_out(master, slave, outfile, logfile, dem_dict):
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
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
    graph = opj(rootpath, 'graphs', 'S1_SLC2INSAR', 'S1_SLC_resample_slave.xml')

    if dem_dict['dem file'] is None:
        dem_dict['dem file'] = ''
        
    print(' INFO: Co-registering {} and {}'.format(master, slave))
    command = ('{} {} -x -q {} '
                ' -Pmaster={}'
                ' -Pslave={}'
                ' -Pdem=\'{}\'' 
                ' -Pdem_file=\'{}\''
                ' -Pdem_nodata=\'{}\'' 
                ' -Pdem_resampling=\'{}\''
                ' -Poutput={} '.format(
                    gpt_file, graph, 2 * os.cpu_count(), 
                    master, slave,
                    dem_dict['dem name'], dem_dict['dem file'], 
                    dem_dict['dem nodata'], dem_dict['dem resampling'], 
                    outfile)
    )
    
    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully resampled slave imagerz.')
    else:
        print(' ERROR: Co-registration exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code




def _make_elev_lat_lon(master, slave, outfile, logfile, dem_dict):
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
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
    graph = opj(rootpath, 'graphs', 'S1_SLC2INSAR', 'S1_SLC_elevLatLon.xml')

    if dem_dict['dem file'] is None:
        dem_dict['dem file'] = ''
        
    print(' INFO: Creating elevation, Lat and Lon from {} and {}'.format(master, slave))
    command = ('{} {} -x -q {} '
                ' -Pmaster={}'
                ' -Pslave={}'
                ' -Pdem=\'{}\'' 
                ' -Pdem_file=\'{}\''
                ' -Pdem_nodata=\'{}\'' 
                ' -Pdem_resampling=\'{}\''
                ' -Poutput={} '.format(
                    gpt_file, graph, 2 * os.cpu_count(), 
                    master, slave,
                    dem_dict['dem name'], dem_dict['dem file'], 
                    dem_dict['dem nodata'], dem_dict['dem resampling'], 
                    outfile)
    )
    
    return_code = h.run_command(command, logfile)

    if return_code == 0:
        print(' INFO: Succesfully resampled slave imagerz.')
    else:
        print(' ERROR: Co-registration exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code


def _resample_slave(master_file,
                     swath,
                     master_burst_nr,
                     master_burst_id,
                     proc_file,
                     out_dir,
                     temp_dir,
                     slave_file=None,
                     slave_burst_nr=None,
                     slave_burst_id=None):

 
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        ard = ard_params['single ARD']
    
    # ---------------------------------------------------------------------
    # 1 Import
    # import master
    master_import = opj(temp_dir, '{}_import'.format(master_burst_id))
    if not os.path.exists('{}.dim'.format(master_import)):
        import_log = opj(out_dir, '{}_import.err_log'.format(master_burst_id))
        return_code = slc._import(master_file, master_import, import_log,
                              swath, master_burst_nr, 'VV'
        )
        
        if return_code != 0:
            h.delete_dimap(master_import)
            return return_code

    # import slave
    slave_import = opj(temp_dir, '{}_import'.format(slave_burst_id))
    if not os.path.exists('{}.dim'.format(slave_import)):
        import_log = opj(out_dir, '{}_import.err_log'.format(slave_burst_id))
        return_code = slc._import(slave_file, slave_import, import_log,
                              swath, slave_burst_nr, 'VV')
        
        if return_code != 0:
            h.delete_dimap(master_import)
            return return_code
     
    # co-registration
    out_slave = opj(out_dir, '{}_slave'.format(slave_burst_id))
    slave_log = opj(out_dir, '{}_slave.err_log'.format(master_burst_id))
    # return_code = _coreg(filelist, out_coreg, coreg_log, dem)
    return_code = _coreg_slave_out('{}.dim'.format(master_import),
                          '{}.dim'.format(slave_import),
                           out_slave,
                           slave_log, ard['dem'])

    
    # remove imports
    h.delete_dimap(slave_import)      

def _create_elevation_lat_lon(master_file,
                     swath,
                     master_burst_nr,
                     master_burst_id,
                     proc_file,
                     out_dir,
                     temp_dir,
                     slave_file=None,
                     slave_burst_nr=None,
                     slave_burst_id=None):

 
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        ard = ard_params['single ARD']
    
    # ---------------------------------------------------------------------
    # 1 Import
    # import master
    master_import = opj(temp_dir, '{}_import'.format(master_burst_id))
    if not os.path.exists('{}.dim'.format(master_import)):
        import_log = opj(out_dir, '{}_import.err_log'.format(master_burst_id))
        return_code = slc._import(master_file, master_import, import_log,
                              swath, master_burst_nr, 'VV'
        )
        
        if return_code != 0:
            h.delete_dimap(master_import)
            return return_code

    # import slave
    slave_import = opj(temp_dir, '{}_import'.format(slave_burst_id))
    if not os.path.exists('{}.dim'.format(slave_import)):
        import_log = opj(out_dir, '{}_import.err_log'.format(slave_burst_id))
        return_code = slc._import(slave_file, slave_import, import_log,
                              swath, slave_burst_nr, 'VV')
        
        if return_code != 0:
            h.delete_dimap(master_import)
            return return_code
        
    # ---------------------------------------------------------------------
    
    # co-registration
    out_aux = opj(out_dir, '{}_elevLatLon'.format(master_burst_id))
    aux_log = opj(out_dir, '{}_elevLatLon.err_log'.format(master_burst_id))
    # return_code = _coreg(filelist, out_coreg, coreg_log, dem)
    return_code = _make_elev_lat_lon('{}.dim'.format(master_import),
                          '{}.dim'.format(slave_import),
                           out_aux,
                           aux_log, ard['dem'])

    # remove imports
    #h.delete_dimap(slave_import)


def resample_slaves(burst_inventory, download_dir, processing_dir, temp_dir, 
                    proc_file, data_mount=None):
    
    
    for burst in burst_inventory.bid.unique():      # ***
    
        # create a list of dates over which we loop
        dates = burst_inventory.Date[burst_inventory.bid == burst].sort_values().tolist()
    
        # loop through dates
        for idx, date in enumerate(dates):      # ******
    
            print(' INFO: Entering burst {} at date {}.'.format(burst, date))
            # get master date
            master_date = dates[0]
            
            # we set this for handling the end of the time-series
            end = False
            
            # read master burst
            master_burst = burst_inventory[
                (burst_inventory.Date == master_date) &
                (burst_inventory.bid == burst)]
    
            master_scene = S1Scene(master_burst.SceneID.values[0])
    
            # get path to file
            master_file = master_scene.get_path(download_dir, data_mount)
            # get subswath
            subswath = master_burst.SwathID.values[0]
            # get burst number in file
            master_burst_nr = master_burst.BurstNr.values[0]
            # create a fileId
            master_id = '{}_{}'.format(master_date, master_burst.bid.values[0])
    
            
            
            # try to get slave date
            try:
                slave_date = dates[idx + 1]    # last burst in timeseries?
            except IndexError:
                end = True
                #print(' INFO: Reached the end of the time-series.'
                #      ' Therefore no coherence calculation is done.')
            else:
                end = False
      
            # read slave burst
            slave_burst = burst_inventory[
                    (burst_inventory.Date == slave_date) &
                    (burst_inventory.bid == burst)]
    
            slave_scene = S1Scene(slave_burst.SceneID.values[0])
    
            # get path to slave file
            slave_file = slave_scene.get_path(download_dir,
                                              data_mount)
    
            # burst number in slave file (subswath is same)
            slave_burst_nr = slave_burst.BurstNr.values[0]
    
            # outfile name
            slave_id = '{}_{}'.format(slave_date,
                                      slave_burst.bid.values[0])
            
            # create out folder
            out_dir = opj(processing_dir, burst, slave_date)
            os.makedirs(out_dir, exist_ok=True)
            
            if idx == 0:
                
                # create master burst image
                master_import = opj(processing_dir, burst, master_date, '{}_master'.format(master_id))
                import_log = opj(processing_dir, burst, master_date, '{}_master.errLog'.format(master_id))
                slc._import(master_file, master_import, import_log,
                              subswath, master_burst_nr, 'VV')
                
                # define outfolder for geo data
                out_dir_elev = opj(processing_dir, burst, 'geo')
                
                # create geo data
                _create_elevation_lat_lon(master_file=master_file,
                             swath=subswath,
                             master_burst_nr=master_burst_nr,
                             master_burst_id=master_id,
                             proc_file=proc_file,
                             out_dir=out_dir_elev,
                             temp_dir=temp_dir,
                             slave_file=slave_file,
                             slave_burst_nr=slave_burst_nr,
                             slave_burst_id=slave_id)
            
            # resample masters 
            if not end:
                _resample_slave(master_file=master_file,
                                 swath=subswath,
                                 master_burst_nr=master_burst_nr,
                                 master_burst_id=master_id,
                                 proc_file=proc_file,
                                 out_dir=out_dir,
                                 temp_dir=temp_dir,
                                 slave_file=slave_file,
                                 slave_burst_nr=slave_burst_nr,
                                 slave_burst_id=slave_id)
    
    
    
    
