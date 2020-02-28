# -*- coding: utf-8 -*-
import os
from os.path import join as opj
from tempfile import TemporaryDirectory
import json

from ost.helpers import helpers as h
from ost.snap_common import common
from ost.s1 import slc_wrappers as slc
import logging

logger = logging.getLogger(__name__)


def create_polarimetric_layers(import_file, ard, temp_dir, out_dir,
                               burst_id, ncores):
    """ Pipeline for Dual-polarimetric decomosition

    Args:
        import_file:
        ard:
        temp_dir:
        out_dir:
        burst_id:
        ncores:

    Returns:

    """


    # check if already processed
    if not os.path.exists(opj(out_dir, '.pol.processed')):

        # temp dir for intermediate files
        with TemporaryDirectory(prefix=temp_dir + '/') as temp:

            # -------------------------------------------------------
            # 1 Polarimetric Decomposition

            # create namespace for temporary decomposed product
            out_haa = opj(temp, '{}_h'.format(burst_id))

            # create namespace for decompose log
            haa_log = opj(out_dir, '{}_haa.err_log'.format(burst_id))

            # run polarimetric decomposition
            slc.ha_alpha(
                import_file, out_haa, haa_log, ard['remove_pol_speckle'],
                ard['pol_speckle_filter'], ncores
            )

            # -------------------------------------------------------
            # 2 Geocoding

            # create namespace for temporary geocoded product
            out_htc = opj(temp, '{}_pol'.format(burst_id))

            # create namespace for geocoding log
            haa_tc_log = opj(out_dir, '{}_haa_tc.err_log'.format(burst_id))

            # run geocoding
            common.terrain_correction(
                '{}.dim'.format(out_haa), out_htc, haa_tc_log,
                ard['resolution'], ard['dem'], ncores
            )

            # last check on the output files
            try:
                h.check_out_dimap(out_htc)
            except ValueError:
                pass

            # move to final destination
            h.move_dimap(
                out_htc, opj(out_dir, '{}_pol'.format(burst_id))
            )

            # write out check file for tracking that it is processed
            with open(opj(out_dir, '.pol.processed'), 'w') as file:
                file.write('passed all tests \n')


def create_backscatter_layers(import_file, proc_file, temp_dir, out_dir,
                              burst_id, ncores):
    """
    
    Args:
        import_file: 
        proc_file:
        temp_dir: 
        out_dir: 
        burst_id: 
        ncores: 

    Returns:

    """

    # load ards
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing_parameters']
        ard = ard_params['single_ARD']

    # temp dir for intermediate files
    with TemporaryDirectory(prefix=temp_dir + '/') as temp:
        # ---------------------------------------------------------------------
        # 1 Calibration

        # create namespace for temporary calibrated product
        out_cal = opj(temp, '{}_cal'.format(burst_id))

        # create namespace for calibrate log
        cal_log = opj(out_dir, '{}_cal.err_log'.format(burst_id))

        # run calibration on imported scene
        slc.calibration(
            import_file, out_cal, cal_log, proc_file,
            region = '', ncores=ncores
        )

        # ---------------------------------------------------------------------
        # 2 Speckle filtering
        if ard['remove_speckle']:

            # create namespace for temporary speckle filtered product
            speckle_import = opj(temp, '{}_speckle_import'.format(burst_id))

            # create namespace for speckle filter log
            speckle_log = opj(out_dir, '{}_speckle.err_log'.format(burst_id))

            # run speckle filter on calibrated input
            common.speckle_filter(
                '{}.dim'.format(out_cal), speckle_import, speckle_log,
                ard['speckle_filter'], ncores
            )

            # remove input
            h.delete_dimap(out_cal)

            # reset master_import for following routine
            out_cal = speckle_import

        # ---------------------------------------------------------------------
        # 3 dB scaling
        if ard['to_db']:

            # create namespace for temporary db scaled product
            out_db = opj(temp, '{}_cal_db'.format(burst_id))

            # create namespace for db scaling log
            db_log = opj(out_dir, '{}_cal_db.err_log'.format(burst_id))

            # run db scaling on calibrated/speckle filtered input
            common.linear_to_db(
                '{}.dim'.format(out_cal), out_db, db_log, ncores
            )

            # remove tmp files
            h.delete_dimap(out_cal)

           # set out_cal to out_db for further processing
            out_cal = out_db

        # ---------------------------------------------------------------------
        # 4 Geocoding

        # create namespace for temporary geocoded product
        out_tc = opj(temp, '{}_bs'.format(burst_id))

        # create namespace for geocoding log
        tc_log = opj(out_dir, '{}_bs_tc.err_log'.format(burst_id))

        # run terrain correction on calibrated/speckle filtered/db  input
        common.terrain_correction(
            '{}.dim'.format(out_cal), out_tc, tc_log,
            ard['resolution'], ard['dem'], ncores
        )

        # check for validity of final backscatter product
        try:
            h.check_out_dimap(out_tc)
        except ValueError:
            pass

        # move final backscatter product to actual output directory
        h.move_dimap(out_tc, opj(out_dir, '{}_bs'.format(burst_id)))

        # ---------------------------------------------------------------------
        # 9 Layover/Shadow mask
        if ard['create_ls_mask']:

            # create namespace for temporary LS map product
            out_ls = opj(temp, '{}_LS'.format(burst_id))

            # create namespace for LS map log
            ls_log = opj(out_dir, '{}_LS.err_log'.format(burst_id))

            # run ls mask generation on calibration
            common.ls_mask(
                '{}.dim'.format(out_cal), out_ls, ls_log,
                ard['resolution'], ard['dem'], ncores
            )

            # check for validity of final backscatter product
            try:
                h.check_out_dimap(out_ls)
            except ValueError:
                pass

            # move ls data to final destination
            h.move_dimap(out_ls, opj(out_dir, '{}_LS'.format(burst_id)))


        # write out check file for tracking that it is processed
        with open(opj(out_dir, '.bs.processed'), 'w') as file:
            file.write('passed all tests \n')


def create_coherence_layers(master_import,
                            slave_import,
                            ard, temp_dir, out_dir,
                            master_burst_id, remove_slave_import, ncores):
    """

    Args:
        master_import:
        slave_import:
        ard:
        temp_dir:
        out_dir:
        master_burst_id:
        remove_slave_import:
        ncores:

    Returns:

    """

    with TemporaryDirectory(prefix=temp_dir + '/') as temp:

        # ---------------------------------------------------------------
        # 1 Co-registration
        # filelist = ['{}.dim'.format(master_import),
        #            '{}.dim'.format(slave_import)]
        # filelist = '\'{}\''.format(','.join(filelist))

        # create namespace for temporary co-registered stack
        out_coreg = opj(temp, '{}_coreg'.format(master_burst_id))

        # create namespace for co-registration log
        coreg_log = opj(out_dir, '{}_coreg.err_log'.format(master_burst_id))

        # run co-registration
        # return_code = _coreg(filelist, out_coreg, coreg_log, dem)
        slc.coreg2(
            master_import, slave_import, out_coreg, coreg_log,
            ard['dem'], ncores
        )

        # remove imports
        h.delete_dimap(master_import)

        if remove_slave_import is True:
            h.delete_dimap(slave_import)

        # ---------------------------------------------------------------
        # 2 Coherence calculation

        # create namespace for temporary coherence product
        out_coh = opj(temp, '{}_coh'.format(master_burst_id))

        # create namespace for coherence log
        coh_log = opj(out_dir, '{}_coh.err_log'.format(master_burst_id))

        # run coherence estimation
        slc.coherence(
            '{}.dim'.format(out_coreg), out_coh, coh_log, ard, ncores
        )

        # remove coreg tmp files
        h.delete_dimap(out_coreg)

        # ---------------------------------------------------------------
        # 3 Geocoding

        # create namespace for temporary geocoded roduct
        out_tc = opj(temp, '{}_coh_tc'.format(master_burst_id))

        # create namespace for geocoded log
        tc_log = opj(out_dir, '{}_coh_tc.err_log'.format(master_burst_id))

        # run geocoding
        common.terrain_correction(
            '{}.dim'.format(out_coh), out_tc, tc_log,
            ard['resolution'], ard['dem'], ncores
        )

        # ---------------------------------------------------------------
        # 4 Checks and Clean-up

        # remove tmp files
        h.delete_dimap(out_coh)

        # check on coherence data
        try:
            h.check_out_dimap(out_tc)
        except ValueError:
            pass

        # move to final destination
        h.move_dimap(out_tc, opj(out_dir, '{}_coh'.format(master_burst_id)))

        # write out check file for tracking that it is processed
        with open(opj(out_dir, '.coh.processed'), 'w') as file:
            file.write('passed all tests \n')


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


    # import master (check if it already exists, bcause it might have been imported as a slave before)


    if type(remove_slave_import) == str:
        if remove_slave_import == 'True':
            remove_slave_import = True
        elif remove_slave_import == 'False':
            remove_slave_import = False

    # load ards
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing_parameters']
        ard = ard_params['single_ARD']

    # check if somethings already processed
    if (not os.path.exists(opj(out_dir, '.pol.processed')) and
            not os.path.exists(opj(out_dir, '.bs.processed')) and
            not os.path.exists(opj(out_dir, '.coh.processed'))):

        # ---------------------------------------------------------------------
        # 1 Import
        # import master
        master_import = opj(temp_dir, '{}_import'.format(master_burst_id))

        if not os.path.exists('{}.dim'.format(master_import)):
            import_log = opj(out_dir, '{}_import.err_log'.format(master_burst_id))
            polars = ard['polarisation'].replace(' ', '')
            return_code = slc.burst_import(
                master_file, master_import, import_log, swath,
                master_burst_nr, polars, ncores
            )
            if return_code != 0:
                h.delete_dimap(master_import)
                return return_code

        # ---------------------------------------------------------------------
        # 2 Product Generation
        if (ard['H-A-Alpha'] and
                not os.path.exists(opj(out_dir, '.pol.processed'))):

            create_polarimetric_layers(
                '{}.dim'.format(master_import), ard, temp_dir, out_dir,
                master_burst_id, ncores
            )

        if (ard['backscatter'] and
                not os.path.exists(opj(out_dir, '.bs.processed'))):

            create_backscatter_layers(
                '{}.dim'.format(master_import), proc_file, temp_dir, out_dir,
                master_burst_id, ncores
            )

        if (ard['coherence'] and
                not os.path.exists(opj(out_dir, '.coh.processed'))):

            # import slave
            slave_import = opj(temp_dir, '{}_import'.format(slave_burst_id))
            import_log = opj(out_dir, '{}_import.err_log'.format(slave_burst_id))
            polars = ard['polarisation'].replace(' ', '')
            return_code = slc.burst_import(
                slave_file, slave_import, import_log, swath, slave_burst_nr,
                polars, ncores
            )

            if return_code != 0:
                h.remove_folder_content(temp_dir)
                return return_code

            create_coherence_layers('{}.dim'.format(master_import),
                                    '{}.dim'.format(slave_import),
                                    ard, temp_dir, out_dir,
                                    master_burst_id, remove_slave_import, ncores)


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
