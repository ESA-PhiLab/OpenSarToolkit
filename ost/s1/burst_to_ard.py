# -*- coding: utf-8 -*-
import os
from os.path import join as opj
from tempfile import TemporaryDirectory
import json
import logging
from pathlib import Path

from ost.helpers import helpers as h
from ost.generic import common_wrappers as common
from ost.s1 import slc_wrappers as slc


logger = logging.getLogger(__name__)


def create_polarimetric_layers(import_file, ard, temp_dir, out_dir,
                               burst_prefix, cpus):
    """ Pipeline for Dual-polarimetric decomposition

    Args:
        import_file:
        ard:
        temp_dir:
        out_dir:
        burst_id:
        ncores:

    Returns:

    """

    # temp dir for intermediate files
    with TemporaryDirectory(prefix=f'{str(temp_dir)}/') as temp:

        temp = Path(temp)
        # -------------------------------------------------------
        # 1 Polarimetric Decomposition

        # create namespace for temporary decomposed product
        out_haa = temp.joinpath(f'{burst_prefix}_h')

        # create namespace for decompose log
        haa_log = out_dir.joinpath(f'{burst_prefix}_haa.err_log')

        # run polarimetric decomposition
        slc.ha_alpha(
            import_file, out_haa, haa_log, ard['remove_pol_speckle'],
            ard['pol_speckle_filter'], cpus
        )

        # -------------------------------------------------------
        # 2 Geocoding

        # create namespace for temporary geocoded product
        out_htc = temp.joinpath(f'{burst_prefix}_pol')

        # create namespace for geocoding log
        haa_tc_log = out_dir.joinpath(f'{burst_prefix}_haa_tc.err_log')

        # run geocoding
        common.terrain_correction(
            '{}.dim'.format(out_haa), out_htc, haa_tc_log,
            ard['resolution'], ard['dem'], cpus
        )

        # last check on the output files
        try:
            h.check_out_dimap(out_htc)
        except ValueError:
            pass

        # move to final destination
        h.move_dimap(out_htc, out_dir.joinpath(f'{burst_prefix}_pol'))

        # write out check file for tracking that it is processed
        with open(out_dir.joinpath('.pol.processed'), 'w+') as file:
            file.write('passed all tests \n')


def create_backscatter_layers(import_file, ard, temp_dir, out_dir,
                              burst_prefix, cpus):
    """

    :param import_file:
    :param ard:
    :param temp_dir:
    :param out_dir:
    :param burst_prefix:
    :param cpus:
    :return:
    """

    # temp dir for intermediate files
    with TemporaryDirectory(prefix=f'{str(temp_dir)}/') as temp:

        temp = Path(temp)
        # ---------------------------------------------------------------------
        # 1 Calibration

        # create namespace for temporary calibrated product
        out_cal = temp.joinpath(f'{burst_prefix}_cal')

        # create namespace for calibrate log
        cal_log = out_dir.joinpath(f'{burst_prefix}_cal.err_log')

        # run calibration on imported scene
        slc.calibration(
            import_file, out_cal, cal_log, ard, region='', ncores=cpus
        )

        # ---------------------------------------------------------------------
        # 2 Speckle filtering
        if ard['remove_speckle']:

            # create namespace for temporary speckle filtered product
            speckle_import = temp.joinpath(f'{burst_prefix}_speckle_import')

            # create namespace for speckle filter log
            speckle_log = out_dir.joinpath(f'{burst_prefix}_speckle.err_log')

            # run speckle filter on calibrated input
            common.speckle_filter(
                f'{out_cal}.dim', speckle_import, speckle_log,
                ard['speckle_filter'], cpus
            )

            # remove input
            h.delete_dimap(out_cal)

            # reset master_import for following routine
            out_cal = speckle_import

        # ---------------------------------------------------------------------
        # 3 dB scaling
        if ard['to_db']:

            # create namespace for temporary db scaled product
            out_db = temp.joinpath(f'{burst_prefix}_cal_db')

            # create namespace for db scaling log
            db_log = out_dir.joinpath(f'{burst_prefix}_cal_db.err_log')

            # run db scaling on calibrated/speckle filtered input
            common.linear_to_db(f'{out_cal}.dim', out_db, db_log, cpus)

            # remove tmp files
            h.delete_dimap(out_cal)

            # set out_cal to out_db for further processing
            out_cal = out_db

        # ---------------------------------------------------------------------
        # 4 Geocoding

        # create namespace for temporary geocoded product
        out_tc = temp.joinpath(f'{burst_prefix}_bs')

        # create namespace for geocoding log
        tc_log = out_dir.joinpath(f'{burst_prefix}_bs_tc.err_log')

        # run terrain correction on calibrated/speckle filtered/db  input
        common.terrain_correction(
            f'{out_cal}.dim', out_tc, tc_log,
            ard['resolution'], ard['dem'], cpus
        )

        # check for validity of final backscatter product
        try:
            h.check_out_dimap(out_tc)
        except ValueError:
            pass

        # move final backscatter product to actual output directory
        h.move_dimap(out_tc, out_dir.joinpath(f'{burst_prefix}_bs'))

        # ---------------------------------------------------------------------
        # 9 Layover/Shadow mask
        if ard['create_ls_mask']:

            # create namespace for temporary LS map product
            out_ls = temp.joinpath(f'{burst_prefix}_LS')

            # create namespace for LS map log
            ls_log = out_dir.joinpath(f'{burst_prefix}_LS.err_log')

            # run ls mask generation on calibration
            common.ls_mask(f'{out_cal}.dim', out_ls, ls_log, ard, cpus)

            # check for validity of final backscatter product
            try:
                h.check_out_dimap(out_ls)
            except ValueError:
                pass

            # move ls data to final destination
            h.move_dimap(out_ls, out_dir.joinpath(f'{burst_prefix}_LS'))

        # write out check file for tracking that it is processed
        with open(out_dir.joinpath('.bs.processed'), 'w+') as file:
            file.write('passed all tests \n')


def create_coherence_layers(
        master_import, slave_import, ard, temp_dir, out_dir,
        master_prefix, cpus
):
    """

    :param master_import:
    :param slave_import:
    :param ard:
    :param temp_dir:
    :param out_dir:
    :param master_prefix:
    :param remove_slave_import:
    :param cpus:
    :return:
    """

    with TemporaryDirectory(prefix=f'{str(temp_dir)}/') as temp:

        temp = Path(temp)
        # ---------------------------------------------------------------
        # 1 Co-registration
        # create namespace for temporary co-registered stack
        out_coreg = temp.joinpath(f'{master_prefix}_coreg')

        # create namespace for co-registration log
        coreg_log = out_dir.joinpath(f'{master_prefix}_coreg.err_log')

        # run co-registration
        slc.coreg(
            master_import, slave_import, out_coreg, coreg_log,
            ard['dem'], cpus
        )

        # remove imports
        h.delete_dimap(master_import)

        # if remove_slave_import is True:
        #    h.delete_dimap(slave_import)

        # ---------------------------------------------------------------
        # 2 Coherence calculation

        # create namespace for temporary coherence product
        out_coh = temp.joinpath(f'{master_prefix}_coh')

        # create namespace for coherence log
        coh_log = out_dir.joinpath(f'{master_prefix}_coh.err_log')

        # run coherence estimation
        slc.coherence(f'{out_coreg}.dim', out_coh, coh_log, ard, cpus)

        # remove coreg tmp files
        h.delete_dimap(out_coreg)

        # ---------------------------------------------------------------
        # 3 Geocoding

        # create namespace for temporary geocoded roduct
        out_tc = temp.joinpath(f'{master_prefix}_coh_tc')

        # create namespace for geocoded log
        tc_log = out_dir.joinpath(f'{master_prefix}_coh_tc.err_log')

        # run geocoding
        common.terrain_correction(
            f'{out_coh}.dim', out_tc, tc_log,
            ard['resolution'], ard['dem'], cpus
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
        h.move_dimap(out_tc, out_dir.joinpath(f'{master_prefix}_coh'))

        # write out check file for tracking that it is processed
        with open(out_dir.joinpath('.coh.processed'), 'w+') as file:
            file.write('passed all tests \n')


def burst_to_ard(burst, ard_params, project_dict):
    if isinstance(burst, tuple):
        i, burst = burst

    ard = ard_params['single_ARD']
    # creation of out_directory
    out_dir = Path(burst.out_directory)
    out_dir.mkdir(parents=True, exist_ok=True)

    # existence of processed files
    pol_file = out_dir.joinpath('.pol.processed').exists()
    bs_file = out_dir.joinpath('.bs.processed').exists()
    coh_file = out_dir.joinpath('.coh.processed').exists()

    # check if we need to produce coherence
    if ard['coherence']:
        # we check if there is actually a slave file or
        # if it is the end of the time-series
        coherence = True if burst.slave_file else False
    else:
        coherence = False

    # check if somethings already processed
    if (
            (ard['H-A-Alpha'] and not pol_file) or
            (ard['backscatter'] and not bs_file) or
            (coherence and not coh_file)
    ):
        # get temp_dir
        temp_dir = Path(project_dict['temp_dir'])
        cpus = project_dict['cpus_per_process']

        # ---------------------------------------------------------------------
        # 1 Import
        # import master

        # get info on master from GeoSeries
        master_prefix = burst['master_prefix']
        master_file = burst['file_location']
        master_burst_nr = burst['BurstNr']
        swath = burst['SwathID']

        # create namespace for master import
        master_import = temp_dir.joinpath(f'{master_prefix}_import')

        if not Path(f'{str(master_import)}.dim').exists():
            import_log = out_dir.joinpath(f'{master_prefix}_import.err_log')
            polars = ard['polarisation'].replace(' ', '')
            return_code = slc.burst_import(
                master_file, master_import, import_log, swath,
                master_burst_nr, polars, cpus
            )
            if return_code != 0:
                h.delete_dimap(master_import)
                return return_code

        # ---------------------------------------------------------------------
        # 2 Product Generation
        if ard['H-A-Alpha'] and not pol_file:

            create_polarimetric_layers(
                f'{master_import}.dim', ard, temp_dir,
                out_dir, master_prefix, cpus
            )

        if ard['backscatter'] and not bs_file:

            create_backscatter_layers(
                f'{master_import}.dim', ard, temp_dir,
                out_dir, master_prefix, cpus
            )

        if coherence and not coh_file:
            # get info on master from GeoSeries
            slave_prefix = burst['slave_prefix']
            slave_file = burst['slave_file']
            slave_burst_nr = burst['slave_burst_nr']

            # import slave
            slave_import = temp_dir.joinpath(f'{slave_prefix}_import')
            import_log = out_dir.joinpath(f'{slave_prefix}_import.err_log')
            polars = ard['polarisation'].replace(' ', '')
            return_code = slc.burst_import(
                slave_file, slave_import, import_log, swath, slave_burst_nr,
                polars, cpus
            )

            if return_code != 0:
                h.remove_folder_content(temp_dir)
                return return_code

            create_coherence_layers(
                f'{master_import}.dim', f'{slave_import}.dim', ard,
                temp_dir, out_dir, master_prefix, cpus
            )

        # remove master import
        h.delete_dimap(master_import)


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
                 args.coherence, args.remove_slave_import,args.cpu_cores
                 )
