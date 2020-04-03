import os
import json
import itertools
import logging
import multiprocessing as mp

import geopandas as gpd
from pathlib import Path

from ost.helpers import scihub, vector as vec
from ost import Sentinel1Scene as S1Scene
from ost.helpers import raster as ras
from ost.generic import ard_to_ts, ts_extent, ts_ls_mask, timescan, mosaic

logger = logging.getLogger(__name__)

# ---------------------------------------------------
# Global variables

# a products list
PRODUCT_LIST = [
    'bs.HH', 'bs.VV', 'bs.HV', 'bs.VH',
    'coh.VV', 'coh.VH', 'coh.HH', 'coh.HV',
    'pol.Entropy', 'pol.Anisotropy', 'pol.Alpha'
]


def burst_inventory(inventory_df,
                    outfile,
                    download_dir=os.getenv('HOME'),
                    data_mount=None):
    """Creates a Burst GeoDataFrame from an OST inventory file

    Args:

    Returns:


    """
    # create column names for empty data frame
    column_names = ['SceneID', 'Track', 'Direction', 'Date', 'SwathID',
                    'AnxTime', 'BurstNr', 'geometry']

    # crs for empty dataframe
    crs = {'init': 'epsg:4326', 'no_defs': True}
    # create empty dataframe
    gdf_full = gpd.GeoDataFrame(columns=column_names, crs=crs)
    # uname, pword = scihub.askScihubCreds()

    for scene_id in inventory_df.identifier:
        # read into S1scene class
        scene = S1Scene(scene_id)

        logger.info('Getting burst info from {}.'.format(scene.scene_id))

        # get orbit direction
        orbit_direction = inventory_df[
            inventory_df.identifier == scene_id].orbitdirection.values[0]

        filepath = str(scene.get_path(download_dir, data_mount))
        single_gdf = None
        if filepath[-4:] == '.zip':
            single_gdf = scene.zip_annotation_get(download_dir, data_mount)
        elif filepath.suffix == '.SAFE':
            single_gdf = scene.safe_annotation_get(download_dir, data_mount)
        if single_gdf is None or single_gdf.empty:
            raise RuntimeError(
                'Cant get single_gdf for scene_id: {}'.format(scene_id)
            )
        # add orbit direction
        single_gdf['Direction'] = orbit_direction

        # append
        gdf_full = gdf_full.append(single_gdf, sort=True)

    gdf_full = gdf_full.reset_index(drop=True)
    for i in gdf_full['AnxTime'].unique():
        # get similar burst times
        idx = gdf_full.index[
            (gdf_full.AnxTime >= i - 1) &
            (gdf_full.AnxTime <= i + 1) &
            (gdf_full.AnxTime != i)
            ].unique().values

        # reset all to first value
        for j in idx:
            gdf_full.at[j, 'AnxTime'] = i

    # create the actual burst id
    gdf_full['bid'] = (
            gdf_full.Direction.str[0] +
            gdf_full.Track.astype(str) + '_' +
            gdf_full.SwathID.astype(str) + '_' +
            gdf_full.AnxTime.astype(str)
    )

    # save file to out
    gdf_full.to_file(outfile, driver="GPKG")
    return gdf_full


def refine_burst_inventory(aoi, burst_gdf, outfile, coverages=None):
    """Creates a Burst GeoDataFrame from an OST inventory file

    Args:

    Returns:


    """

    # turn aoi into a geodataframe
    aoi_gdf = gpd.GeoDataFrame(vec.wkt_to_gdf(aoi).buffer(0.05))
    aoi_gdf.columns = ['geometry']
    aoi_gdf.crs = {'init': 'epsg:4326', 'no_defs': True}

    # get columns of input dataframe for later return function
    cols = burst_gdf.columns

    # 1) get only intersecting footprints (double, since we do this before)
    burst_gdf = gpd.sjoin(burst_gdf, aoi_gdf, how='inner', op='intersects')

    # if aoi  gdf has an id field we need to rename the changed id_left field
    if 'id_left' in burst_gdf.columns:
        # rename id_left to id
        burst_gdf.columns = (['id' if x == 'id_left' else x
                              for x in burst_gdf.columns])

    # remove duplicates
    burst_gdf.drop_duplicates(['SceneID', 'Date', 'bid'], inplace=True)

    # check if number of bursts align with number of coverages
    if coverages:
        for burst in burst_gdf.bid.unique():
            if len(burst_gdf[burst_gdf.bid == burst]) != coverages:
                logging.info(
                    f'Removing burst {burst} because of unsuffcient coverage.'
                )

                burst_gdf.drop(
                    burst_gdf[burst_gdf.bid == burst].index, inplace=True
                )

    # save file to out
    burst_gdf.to_file(outfile, driver="GPKG")
    return burst_gdf[cols]


def prepare_burst_inventory(burst_gdf, project_dict):

    cols = [
        'AnxTime', 'BurstNr', 'Date', 'Direction', 'SceneID', 'SwathID',
        'Track', 'geometry', 'bid', 'master_prefix', 'out_directory',
        'slave_date', 'slave_scene_id', 'slave_file', 'slave_burst_nr',
        'slave_prefix'
    ]

    # create empty geodataframe
    proc_burst_gdf = gpd.GeoDataFrame(columns=cols, geometry='geometry',
                                      crs={'init': 'epsg:4326',
                                           'no_defs': True})

    for burst in burst_gdf.bid.unique():  # ***

        # create a list of dates over which we loop
        dates = burst_gdf.Date[
            burst_gdf.bid == burst].sort_values().tolist()

        # loop through dates
        for idx, date in enumerate(dates):  # ******

            # get master date
            burst_row = burst_gdf[
                (burst_gdf.Date == date) &
                (burst_gdf.bid == burst)].copy()

            # get parameters for master
            master_scene = S1Scene(burst_row.SceneID.values[0])
            burst_row['file_location'] = master_scene.get_path(
                Path(project_dict['download_dir']), Path(project_dict['data_mount'])
            )
            burst_row['master_prefix'] = f'{date}_{burst_row.bid.values[0]}'
            burst_row['out_directory'] = Path(project_dict['processing_dir']).joinpath(burst, date)

            # try to get slave date
            try:
                # get slave date and add column to burst row

                slave_date = dates[idx + 1]
                burst_row['slave_date'] = slave_date

                # read slave burst line
                slave_burst = burst_gdf[
                    (burst_gdf.Date == slave_date) &
                    (burst_gdf.bid == burst)]

                # get scene id and add into master row
                slave_scene_id = S1Scene(slave_burst.SceneID.values[0])
                burst_row['slave_scene_id'] = slave_scene_id.scene_id

                # get path to slave file
                burst_row['slave_file'] = slave_scene_id.get_path(
                    project_dict['download_dir'], project_dict['data_mount']
                )

                # burst number in slave file (subswath is same)
                burst_row['slave_burst_nr'] = slave_burst.BurstNr.values[0]

                # outfile name
                burst_row['slave_prefix'] = (
                    f'{slave_date}_{slave_burst.bid.values[0]}'
                )

            except IndexError:
                burst_row['slave_date'], burst_row[
                    'slave_scene_id'] = None, None
                burst_row['slave_file'], burst_row[
                    'slave_burst_nr'] = None, None
                burst_row['slave_prefix'] = None

            proc_burst_gdf = proc_burst_gdf.append(burst_row, sort=False)

    return proc_burst_gdf


def print_burst(input_list):
    # extract input list
    proc_burst_series, project_file = input_list[0], input_list[1]
    print('Processing burst:' + proc_burst_series.bid)
    print('Project_file:' + str(project_file)+ proc_burst_series.bid)


def _create_extents(burst_gdf, project_file):

    with open(project_file, 'r') as file:
        project_params = json.load(file)['project']
        processing_dir = project_params['processing_dir']
        temp_dir = project_params['temp_dir']

    # create extent iterable
    iter_list = []
    for burst in burst_gdf.bid.unique():  # ***

        # get the burst directory
        burst_dir = Path(processing_dir).joinpath(burst)

        # get common burst extent
        list_of_bursts = list(burst_dir.glob('**/*img'))
        list_of_bursts = [
            str(x) for x in list_of_bursts if 'layover' not in str(x)
        ]
        extent = burst_dir.joinpath(f'{burst}.extent.gpkg')

        # if the file does not already exist, add to iterable
        if not extent.exists():
            iter_list.append([list_of_bursts, extent, temp_dir, -0.0018])

    # parallelizing on all cpus
    concurrent = mp.cpu_count()
    pool = mp.Pool(processes=concurrent)
    pool.map(ts_extent.mt_extent, iter_list)


def _create_mt_ls_mask(burst_gdf, project_file):

    # read config file
    with open(project_file, 'r') as file:
        project_params = json.load(file)
        processing_dir = project_params['project']['processing_dir']
        temp_dir = project_params['project']['temp_dir']
        ard = project_params['processing_parameters']['time-series_ARD']

    # create layover
    iter_list = []
    for burst in burst_gdf.bid.unique():  # ***

        # get the burst directory
        burst_dir = Path(processing_dir).joinpath(burst)

        # get layover scenes
        list_of_scenes = list(burst_dir.glob('20*/*data*/*img'))
        list_of_layover = [
            str(x) for x in list_of_scenes if 'layover' in str(x)
            ]

        # we need to redefine the namespace of the already created extents
        extent = burst_dir.joinpath(f'{burst}.extent.gpkg')
        if not extent.exists():
            raise FileNotFoundError(
                f'Extent file for burst {burst} not found.'
            )

        # layover/shadow mask
        out_ls = burst_dir.joinpath(f'{burst}.ls_mask.tif')

        # if the file does not already exists, then put into list to process
        if not out_ls.exists():
            iter_list.append(
                [list_of_layover, out_ls, temp_dir, str(extent),
                 ard['apply_ls_mask']]
            )

    # parallelizing on all cpus
    concurrent = int(
        mp.cpu_count() / project_params['project']['cpus_per_process']
    )
    pool = mp.Pool(processes=concurrent)
    pool.map(ts_ls_mask.mt_layover, iter_list)


def _create_timeseries(burst_gdf, project_file):

    # we need a
    dict_of_product_types = {'bs': 'Gamma0', 'coh': 'coh', 'pol': 'pol'}
    pols = ['VV', 'VH', 'HH', 'HV', 'Alpha', 'Entropy', 'Anisotropy']

    # read config file
    with open(project_file, 'r') as file:
        project_params = json.load(file)
        processing_dir = project_params['project']['processing_dir']

    # create iterable
    iter_list = []
    for burst in burst_gdf.bid.unique():

        burst_dir = Path(processing_dir).joinpath(burst)

        for pr, pol in itertools.product(dict_of_product_types.items(), pols):

            # unpack items
            product, product_name = list(pr)

            # take care of H-A-Alpha naming for file search
            if pol in ['Alpha', 'Entropy', 'Anisotropy'] and product is 'pol':
                list_of_files = sorted(
                    list(burst_dir.glob(f'20*/*data*/*{pol}*img')))
            else:
                # see if there is actually any imagery for this
                # combination of product and polarisation
                list_of_files = sorted(
                    list(burst_dir.glob(
                        f'20*/*data*/*{product_name}*{pol}*img')
                    )
                )

            if len(list_of_files) <= 1:
                continue

            # create list of dims if polarisation is present
            list_of_dims = sorted(list(burst_dir.glob(f'20*/*{product}*dim')))
            iter_list.append([list_of_dims, burst, product, pol, project_file])

    # parallelizing on all cpus
    concurrent = int(
        mp.cpu_count() / project_params['project']['cpus_per_process']
    )
    pool = mp.Pool(processes=concurrent)
    pool.map(ard_to_ts.ard_to_ts, iter_list)


def ards_to_timeseries(burst_gdf, project_file):

    print('--------------------------------------------------------------')
    logger.info('Processing all burst ARDs time-series')
    print('--------------------------------------------------------------')

    # load ard parameters
    with open(project_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing_parameters']
        ard = ard_params['single_ARD']
        ard_mt = ard_params['time-series_ARD']


    # create all extents
    _create_extents(burst_gdf, project_file)

    # update extents in case of ls_mask
    if ard['create_ls_mask'] or ard_mt['apply_ls_mask']:
        _create_mt_ls_mask(burst_gdf, project_file)

    # finally create time-series
    _create_timeseries(burst_gdf, project_file)


# --------------------
# timescan part
# --------------------
def timeseries_to_timescan(burst_gdf, project_file):
    """Function to create a timescan out of a OST timeseries.

    """

    print('--------------------------------------------------------------')
    logger.info('Processing all burst ARDs time-series to ARD timescans')
    print('--------------------------------------------------------------')

    # -------------------------------------
    # 1 load project config
    with open(project_file, 'r') as ard_file:
        project_params = json.load(ard_file)
        processing_dir = project_params['project']['processing_dir']
        ard = project_params['processing_parameters']['single_ARD']
        ard_mt = project_params['processing_parameters']['time-series_ARD']
        ard_tscan = project_params['processing_parameters']['time-scan_ARD']

    # get the db scaling right
    if ard['to_db'] or ard_mt['to_db']:
        to_db = True

    # get datatype right
    dtype_conversion = True if ard_mt['dtype_output'] != 'float32' else False

    # -------------------------------------
    # 2 create iterable for parallel processing
    iter_list, vrt_iter_list = [], []
    for burst in burst_gdf.bid.unique():

        # get relevant directories
        burst_dir = Path(processing_dir).joinpath(burst)
        timescan_dir = burst_dir.joinpath('Timescan')
        timescan_dir.mkdir(parents=True, exist_ok=True)

        for product in PRODUCT_LIST:

            # check if already processed
            if timescan_dir.joinpath(f'.{product}.processed').exists():
                #logger.info(f'Timescans for burst {burst} already processed.')
                continue

            # get respective timeseries
            timeseries = burst_dir.joinpath(
                f'Timeseries/Timeseries.{product}.vrt'
            )

            # che if this timsereis exists ( since we go through all products
            if not timeseries.exists():
                continue

            # datelist for harmonics
            scenelist = list(burst_dir.glob(f'Timeseries/*{product}*tif'))
            datelist = [
                file.name.split('.')[1][:6] for file in sorted(scenelist)
            ]

            # define timescan prefix
            timescan_prefix = timescan_dir.joinpath(product)

            # get rescaling and db right (backscatter vs. coh/pol)
            if 'bs.' in str(timescan_prefix):
                to_power, rescale = to_db, dtype_conversion
            else:
                to_power, rescale = False, False

            iter_list.append(
                [timeseries, timescan_prefix, ard_tscan['metrics'],
                 rescale, to_power, ard_tscan['remove_outliers'], datelist]
            )

        vrt_iter_list.append([timescan_dir, project_file])

    concurrent = mp.cpu_count()
    pool = mp.Pool(processes=concurrent)
    pool.map(timescan.mt_metrics, iter_list)
    pool.map(ras.create_tscan_vrt, vrt_iter_list)


def mosaic_timeseries(burst_inventory, project_file):

    print(' -----------------------------------------------------------------')
    logger.info('Mosaicking time-series layers.')
    print(' -----------------------------------------------------------------')

    # -------------------------------------
    # 1 load project config
    with open(project_file, 'r') as ard_file:
        project_params = json.load(ard_file)
        processing_dir = project_params['project']['processing_dir']

    # create output folder
    ts_dir = Path(processing_dir).joinpath('Mosaic/Timeseries')
    ts_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------------------
    # 2 create iterable
    # loop through each product
    iter_list, vrt_iter_list = [], []
    for product in PRODUCT_LIST:

        #
        bursts = burst_inventory.bid.unique()
        nr_of_ts = len(list(
            Path(processing_dir).glob(
                f'{bursts[0]}/Timeseries/*.{product}.tif'
            )
        ))

        # in case we only have one layer
        if not nr_of_ts > 1:
            continue

        outfiles = []
        for i in range(1, nr_of_ts + 1):

            # create the file list for files to mosaic
            filelist = list(Path(processing_dir).glob(
                f'*/Timeseries/{i:02d}.*{product}.tif'
            ))

            # assure that we do not inlcude potential Mosaics
            # from anterior runs
            filelist = [file for file in filelist if 'Mosaic' not in str(file)]

            logger.info(f'Creating timeseries mosaic {i} for {product}.')

            # create dates for timseries naming
            datelist = []
            for file in filelist:
                if '.coh.' in str(file):
                    datelist.append(
                        f"{file.name.split('.')[2]}_{file.name.split('.')[1]}"
                    )
                else:
                    datelist.append(file.name.split('.')[1])

            # get start and endate of mosaic
            start, end = sorted(datelist)[0], sorted(datelist)[-1]
            filelist = ' '.join([str(file) for file in filelist])

            # create namespace for output file
            if start == end:
                outfile = ts_dir.joinpath(
                              f'{i:02d}.{start}.{product}.tif'
                )

            else:
                outfile = ts_dir.joinpath(
                              f'{i:02d}.{start}-{end}.{product}.tif'
                )

            # create nmespace for check_file
            check_file = outfile.parent.joinpath(
                f'.{outfile.name[:-4]}.processed'
            )

            if os.path.isfile(check_file):
                print('INFO: Mosaic layer {} already'
                      ' processed.'.format(outfile))
                continue

            # append to list of outfile for vrt creation
            outfiles.append(outfile)
            iter_list.append([filelist, outfile, project_file])

        vrt_iter_list.append([ts_dir, product, outfiles])

    concurrent = mp.cpu_count()
    pool = mp.Pool(processes=concurrent)
    pool.map(mosaic.mosaic, iter_list)
    pool.map(mosaic.create_timeseries_mosaic_vrt, vrt_iter_list)


def mosaic_timescan(burst_inventory, project_file):

    print(' -----------------------------------------------------------------')
    logger.info('Mosaicking time-scan layers.')
    print(' -----------------------------------------------------------------')

    with open(project_file, 'r') as ard_file:
        project_params = json.load(ard_file)
        processing_dir = project_params['project']['processing_dir']
        metrics = project_params['processing']['time-scan_ARD']['metrics']

    if 'harmonics' in metrics:
        metrics.remove('harmonics')
        metrics.extend(['amplitude', 'phase', 'residuals'])

    if 'percentiles' in metrics:
        metrics.remove('percentiles')
        metrics.extend(['p95', 'p5'])

    tscan_dir = Path(processing_dir).joinpath('Mosaic/Timescan')
    tscan_dir.mkdir(parents=True, exist_ok=True)

    iter_list, outfiles = [], []
    for product, metric in itertools.product(PRODUCT_LIST, metrics):

        filelist = list(Path(processing_dir).glob(
            f'*/Timescan/*{product}.{metric}.tif'
        ))

        if not len(filelist) >= 1:
            continue

        filelist = ' '.join([str(file) for file in filelist])

        outfile = tscan_dir.joinpath(f'{product}.{metric}.tif')
        check_file = outfile.parent.joinpath(
            f'.{outfile.name[:-4]}.processed'
        )

        if check_file.exists():
            logger.info(f'Mosaic layer {outfile.name} already processed.')
            continue

        logger.info(f'Mosaicking layer {outfile.name}.')
        outfiles.append(outfile)
        iter_list.append([filelist, outfile, project_file])

    concurrent = mp.cpu_count()
    pool = mp.Pool(processes=concurrent)
    pool.map(mosaic.mosaic, iter_list)
    ras.create_tscan_vrt([tscan_dir, project_file])
