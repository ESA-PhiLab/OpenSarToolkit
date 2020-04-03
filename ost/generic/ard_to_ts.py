# -*- coding: utf-8 -*-

import json
import logging
import gdal
from pathlib import Path
from datetime import datetime as dt
from tempfile import TemporaryDirectory

from ost.generic.common_wrappers import create_stack, mt_speckle_filter
from ost.helpers import raster as ras, helpers as h

logger = logging.getLogger(__name__)


def ard_to_ts(list_of_args):

    # -------------------------------------------
    # 1 unpack list of args
    list_of_files, burst, product, pol, project_file = list_of_args
    # convert list of files readable for snap
    list_of_files = f"\'{','.join(str(x) for x in list_of_files)}\'"

    # -------------------------------------------
    # 2 read config file
    with open(project_file, 'r') as file:
        project_params = json.load(file)
        processing_dir = project_params['project']['processing_dir']
        temp_dir = project_params['project']['temp_dir']
        cpus_per_process = project_params['project']['cpus_per_process']
        ard_params = project_params['processing_parameters']
        ard = ard_params['single_ARD']
        ard_mt = ard_params['time-series_ARD']

    # -------------------------------------------
    # 3 get namespace of directories and check if already processed
    # get the burst directory
    burst_dir = Path(processing_dir).joinpath(burst)

    # get timeseries directory and create if non existent
    out_dir = burst_dir.joinpath('Timeseries')
    Path.mkdir(out_dir, parents=True, exist_ok=True)

    # in case some processing has been done before, check if already processed
    check_file = out_dir.joinpath(f'.{product}.{pol}.processed')
    if Path.exists(check_file):
        logger.info(f'Timeseries of {burst} for {product} in {pol} '
                    f'polarisation already processed.')
        return

    # -------------------------------------------
    # 4 adjust processing parameters according to config
    # get the db scaling right
    to_db = ard['to_db']
    if to_db or product != 'bs':
        to_db = False
        logger.debug(f'Not converting to dB for {product}')
    else:
        to_db = ard_mt['to_db']
        logger.debug(f'Converting to dB for {product}')

    if ard_mt['apply_ls_mask']:
        extent = burst_dir.joinpath(f'{burst}.extent.masked.gpkg')
    else:
        extent = burst_dir.joinpath(f'{burst}.extent.gpkg')

    # -------------------------------------------
    # 5 SNAP processing
    with TemporaryDirectory(prefix=f'{temp_dir}/') as temp:

        # turn to Path object
        temp = Path(temp)

        # create namespaces
        temp_stack = temp.joinpath(f'{burst}_{product}_{pol}')
        out_stack = temp.joinpath(f'{burst}_{product}_{pol}_mt')
        stack_log = out_dir.joinpath(f'{burst}_{product}_{pol}_stack.err_log')

        # run stacking routine
        if pol in ['Alpha', 'Anisotropy', 'Entropy']:
            logger.info(
                f'Creating multi-temporal stack of images of burst/track '
                f'{burst} for the {pol} band of the polarimetric '
                f'H-A-Alpha decomposition.'
            )
            create_stack(list_of_files, temp_stack, stack_log, pattern=pol)
        else:
            logger.info(
                f'Creating multi-temporal stack of images of burst/track '
                f'{burst} for {product} product in {pol} polarization.'
            )
            create_stack(
                list_of_files, temp_stack, stack_log, polarisation=pol
            )

        # run mt speckle filter
        if ard_mt['remove_mt_speckle'] is True:
            ard_mt_speck = ard_params['time-series_ARD']['mt_speckle_filter']
            speckle_log = out_dir.joinpath(
                f'{burst}_{product}_{pol}_mt_speckle.err_log'
            )

            logger.info('Applying multi-temporal speckle filter')
            mt_speckle_filter(
                f'{temp_stack}.dim', out_stack, speckle_log,
                speckle_dict=ard_mt_speck, ncores=cpus_per_process
            )
            # remove tmp files
            h.delete_dimap(temp_stack)
        else:
            out_stack = temp_stack

        # -----------------------------------------------
        # 6 Conversion to GeoTiff

        # min max dict for stretching in case of 16 or 8 bit datatype
        mm_dict = {'bs': {'min': -30, 'max': 5},
                   'coh': {'min': 0.000001, 'max': 1},
                   'Alpha': {'min': 0.000001, 'max': 90},
                   'Anisotropy': {'min': 0.000001, 'max': 1},
                   'Entropy': {'min': 0.000001, 'max': 1}
                   }
        stretch = pol if pol in ['Alpha', 'Anisotropy', 'Entropy'] else product

        if product == 'coh':

            # get slave and master dates from file names and sort them
            mst_dates = sorted([
                dt.strptime(file.name.split('_')[3].split('.')[0], '%d%b%Y')
                for file in list(out_stack.with_suffix('.data').glob('*.img'))
            ])

            slv_dates = sorted([
                dt.strptime(file.name.split('_')[4].split('.')[0], '%d%b%Y')
                for file in list(out_stack.with_suffix('.data').glob('*.img'))
            ])

            # write them back to string for following loop
            mst_dates = [dt.strftime(ts, "%d%b%Y") for ts in mst_dates]
            slv_dates = [dt.strftime(ts, "%d%b%Y") for ts in slv_dates]

            outfiles = []
            for i, (mst, slv) in enumerate(zip(mst_dates, slv_dates)):

                # re-construct namespace for input file
                infile = list(
                    out_stack.with_suffix('.data').glob(
                        f'*{pol}*{mst}_{slv}*img'
                    )
                )[0]

                # rename dates to YYYYMMDD format
                mst = dt.strftime(dt.strptime(mst, '%d%b%Y'), '%y%m%d')
                slv = dt.strftime(dt.strptime(slv, '%d%b%Y'), '%y%m%d')

                # create namespace for output file with renamed dates
                outfile = out_dir.joinpath(
                    f'{i+1:02d}.{mst}.{slv}.{product}.{pol}.tif'
                )

                # produce final outputfile,
                # including dtype conversion and ls mask
                ras.mask_by_shape(
                    infile, outfile, extent, to_db=to_db,
                    datatype=ard_mt['dtype_output'],
                    min_value=mm_dict[stretch]['min'],
                    max_value=mm_dict[stretch]['max'],
                    ndv=0.0, description=True)

                # add ot a list for subsequent vrt creation
                outfiles.append(str(outfile))

        else:
            # get the dates of the files
            dates = sorted([dt.strptime(
                file.name.split('_')[-1][:-4], '%d%b%Y')
                for file in list(out_stack.with_suffix('.data').glob('*.img'))
            ])

            # write them back to string for following loop
            dates = [dt.strftime(ts, "%d%b%Y") for ts in dates]

            outfiles = []
            for i, date in enumerate(dates):

                # re-construct namespace for input file
                infile = list(
                    out_stack.with_suffix('.data').glob(f'*{pol}*{date}*img')
                )[0]

                # restructure date to YYMMDD
                date = dt.strftime(dt.strptime(date, '%d%b%Y'), '%y%m%d')

                # create namespace for output file
                outfile = out_dir.joinpath(
                    f'{i+1:02d}.{date}.{product}.{pol}.tif'
                )

                # run conversion routine
                ras.mask_by_shape(infile, outfile, extent,
                                  to_db=to_db,
                                  datatype=ard_mt['dtype_output'],
                                  min_value=mm_dict[stretch]['min'],
                                  max_value=mm_dict[stretch]['max'],
                                  ndv=0.0)

                # add ot a list for subsequent vrt creation
                outfiles.append(str(outfile))

    # -----------------------------------------------
    # 7 Filechecks
    for file in outfiles:
        return_code = h.check_out_tiff(file)
        if return_code != 0:
            Path(file).unlink()
            return return_code

    # write file, so we know this ts has been successfully processed
    with open(str(check_file), 'w') as file:
        file.write('passed all tests \n')

    # -----------------------------------------------
    # 8 Create vrts
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
    gdal.BuildVRT(
        str(out_dir.joinpath(f'Timeseries.{product}.{pol}.vrt')),
        outfiles,
        options=vrt_options
    )
