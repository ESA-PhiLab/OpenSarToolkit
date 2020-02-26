# -*- coding: utf-8 -*-



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
