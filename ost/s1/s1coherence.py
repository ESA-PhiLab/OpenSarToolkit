import os
from os.path import join as opj
import logging
from datetime import datetime
from tempfile import TemporaryDirectory

from shapely.geometry import box
from shapely.wkt import loads as shp_loads

from ost.s1.s1scene import Sentinel1Scene as S1scene
from ost.helpers import helpers as h
from ost.s1.burst_to_ard import _import, _coreg2, _coherence, _terrain_correction
from ost.helpers.helpers import _slc_zip_to_processing_dir

logger = logging.getLogger(__name__)


class Sentinel1Scenes:

    def __init__(self, filelist, processing_dir=None, ard_type=None, cleanup=False):
        slave_list = []
        for idx, s in zip(range(len(filelist)), filelist):
            scene_id = get_scene_id(product_path=s)
            if idx == 0:
                self.master = S1scene(scene_id=scene_id)
                # Copy files to processing dir
                _slc_zip_to_processing_dir(
                    processing_dir=processing_dir,
                    product=self.master,
                    product_path=s
                )
            else:
                slave_list.append(S1scene(scene_id=scene_id))
                # Copy files to processing dir
                _slc_zip_to_processing_dir(
                    processing_dir=processing_dir,
                    product=self.master,
                    product_path=s
                )
        self.slaves = slave_list
        self.cleanup = cleanup

        # ARD type is controled by master, kinda makes sense doesn't it
        if ard_type is not None:
            self.master.set_ard_parameters(ard_type)

    def s1_scenes_to_ard(self,
                         processing_dir,
                         dem='SRTM 1Sec HGT',
                         subset=None,
                         ):
        if self.master.ard_parameters['type'] is None:
            raise RuntimeError('Need to specify or setup ard_type')
        # more custom ARD params, see whats in s1scene.py and complement corespondingly
        self.master.ard_parameters['dem'] = dem
        out_files = []
        # process the amster first
        with TemporaryDirectory() as temp:
            out_file = self.master.create_ard(
                infile=None,
                out_dir=processing_dir,
                out_prefix='SLC_ARD',
                temp_dir=temp,
                subset=None,
                polar='VV,VH,HH,HV'
            )
        out_files.append(out_file)

        # Process all slaves as ARD
        # for s in self.slaves:
        #     out_file = s.create_ard()
        #     out_files.append(out_file)
        return out_files

    def get_weekly_pairs(self):
        master_date = datetime.strptime(s1.master.start_date, '%Y%m%d')

        slave_dates = [datetime.strptime(slave.start_date, '%Y%m%d')
                       for slave in s1.slaves
                       ]
        weekly_pairs = []
        for s, slave in zip(slave_dates, s1.slaves):
            if not check_for_orbit(master=self.master, slave=slave):
                logger.debug('Passing product with different relative orbit!')
                continue
            if not check_for_beam_mode(master=self.master, slave=slave):
                logger.debug('Passing product with different beam mode!')
                continue
            t_diff = abs((master_date-s).days)
            if 11 > t_diff > 4:
                weekly_pairs.append((self.master, slave))
        return weekly_pairs

    def get_biweekly_pairs(self):
        master_date = datetime.strptime(s1.master.start_date, '%Y%m%d')

        slave_dates = [datetime.strptime(slave.start_date, '%Y%m%d')
                       for slave in s1.slaves
                       ]
        biweekly_pairs = []
        for s, slave in zip(slave_dates, s1.slaves):
            if not check_for_orbit(master=self.master, slave=slave):
                logger.debug('Passing product with different relative orbit!')
                continue
            if not check_for_beam_mode(master=self.master, slave=slave):
                logger.debug('Passing product with different beam mode!')
                continue
            t_diff = abs((master_date-s).days)
            if 17 > t_diff > 10:
                biweekly_pairs.append((self.master, slave))
        return biweekly_pairs

    def create_coherence(
            self,
            processing_dir,
            temp_dir=None,
            timeliness='14days',
            dem='SRTM 1Sec HGT',
            dem_file='',
            resolution=20,
            resampling='BILINEAR_INTERPOLATION',
            subset=None,
    ):
        if self.master.product_type != 'SLC':
            raise TypeError('create_coherence needs SLC products')
        if timeliness == '14days':
            pairs = self.get_biweekly_pairs()
        elif timeliness == '7days':
            pairs = self.get_biweekly_pairs()
        else:
            raise RuntimeError(
                'Just weekly or biweekly are your options, take it or leave it!'
            )

        if subset is not None:
            try:
                processing_poly = shp_loads(subset)
            except Exception as e:
                raise e
        else:
            processing_poly = None

        for pair in pairs:
            # get file paths
            master_file = pair[0].get_path(processing_dir)
            slave_file = pair[1].get_path(processing_dir)

            # get bursts
            master_bursts = pair[0]._zip_annotation_get(download_dir=processing_dir)
            slave_bursts = pair[1]._zip_annotation_get(download_dir=processing_dir)

            bursts_dict = get_bursts_pairs(master_annotation=master_bursts,
                                           slave_annotation=slave_bursts,
                                           out_poly=processing_poly
                                           )
            for swath, b in bursts_dict.items():
                if b != []:
                    for burst in b:
                        m_nr, m_burst_id, sl_burst_nr, sl_burst_id, b_bbox = burst
                        return_code = _2products_coherence_tc(
                            master_file=master_file,
                            slave_file=slave_file,
                            out_dir=processing_dir,
                            temp_dir=temp_dir,
                            swath=swath,
                            master_burst_id=m_burst_id,
                            master_burst_nr=m_nr,
                            slave_burst_id=sl_burst_id,
                            slave_burst_nr=sl_burst_nr,
                            resolution=resolution,
                            dem=dem,
                            dem_file=dem_file,
                            resampling=resampling
                        )
                        if return_code != 0:
                            print(return_code)
                            raise RuntimeError

        return processing_dir


def _2products_coherence_tc(
        master_file,
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
    # import slave
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

    # co-registration
    out_coreg = opj(temp_dir, '{}_coreg'.format(master_burst_id))
    coreg_log = opj(out_dir, '{}_coreg.err_log'.format(master_burst_id))
    logger.debug('{}.dim'.format(master_import))
    logger.debug('{}.dim'.format(slave_import))
    return_code = _coreg2('{}.dim'.format(master_import),
                          '{}.dim'.format(slave_import),
                          out_coreg,
                          coreg_log,
                          dem
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
                             out_coh, coh_log)
    if return_code != 0:
        h.remove_folder_content(temp_dir)
        return return_code

    # remove coreg tmp files
    h.delete_dimap(out_coreg)

    # geocode
    out_tc = opj(temp_dir, '{}_coh'.format(master_burst_id))
    tc_log = opj(out_dir, '{}_coh_tc.err_log'.format(master_burst_id))
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
    h.move_dimap(out_tc, opj(out_dir, '{}_coh'.format(master_burst_id)))
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


def get_bursts_by_polygon(master_annotation, out_poly=None):
    master_bursts = master_annotation

    bursts_dict = {'IW1': [], 'IW2': [], 'IW3': []}
    for subswath, nr, id, b in zip(
            master_bursts['SwathID'],
            master_bursts['BurstNr'],
            master_bursts['AnxTime'],
            master_bursts['geometry']
    ):

        # Return all burst combinations if out poly is None
        if out_poly is None:
            if (nr, id) not in bursts_dict[subswath]:
                b_bounds = b.bounds
                burst_buffer = abs(b_bounds[2]-b_bounds[0])/75
                burst_bbox = box(
                    b_bounds[0], b_bounds[1], b_bounds[2], b_bounds[3]
                ).buffer(burst_buffer).envelope
                bursts_dict[subswath].append((nr, id, burst_bbox))
        elif b.intersects(out_poly):
            if (nr, id) not in bursts_dict[subswath]:
                b_bounds = b.bounds
                burst_buffer = abs(out_poly.bounds[2]-out_poly.bounds[0])/75
                burst_bbox = box(
                    b_bounds[0], b_bounds[1], b_bounds[2], b_bounds[3]
                ).buffer(burst_buffer).envelope
                bursts_dict[subswath].append((nr, id, burst_bbox))
    return bursts_dict


def get_bursts_pairs(master_annotation, slave_annotation, out_poly=None):
    master_bursts = master_annotation
    slave_bursts = slave_annotation

    bursts_dict = {'IW1': [], 'IW2': [], 'IW3': []}
    for subswath, nr, id, b in zip(
            master_bursts['SwathID'],
            master_bursts['BurstNr'],
            master_bursts['AnxTime'],
            master_bursts['geometry']
    ):
        for sl_subswath, sl_nr, sl_id, sl_b in zip(
                slave_bursts['SwathID'],
                slave_bursts['BurstNr'],
                slave_bursts['AnxTime'],
                slave_bursts['geometry']
        ):
            # Return all burst combinations if out poly is None
            if out_poly is None and b.intersects(sl_b):
                if subswath == sl_subswath and \
                        (nr, id, sl_nr, sl_id) not in bursts_dict[subswath]:
                    b_bounds = b.union(sl_b).bounds
                    burst_buffer = abs(b_bounds[2]-b_bounds[0])/75
                    burst_bbox = box(
                        b_bounds[0], b_bounds[1], b_bounds[2], b_bounds[3]
                    ).buffer(burst_buffer).envelope
                    bursts_dict[subswath].append((nr, id, sl_nr, sl_id, burst_bbox))
            elif b.intersects(sl_b) \
                    and b.intersects(out_poly) and sl_b.intersects(out_poly):
                if subswath == sl_subswath and \
                        (nr, id, sl_nr, sl_id) not in bursts_dict[subswath]:
                    b_bounds = b.union(sl_b).bounds
                    burst_buffer = abs(out_poly.bounds[2]-out_poly.bounds[0])/75
                    burst_bbox = box(
                        b_bounds[0], b_bounds[1], b_bounds[2], b_bounds[3]
                    ).buffer(burst_buffer).envelope
                    bursts_dict[subswath].append((nr, id, sl_nr, sl_id, burst_bbox))
    return bursts_dict


def check_for_beam_mode(master, slave):
    flag = False
    if master.mode_beam == slave.mode_beam:
        flag = True
    return flag


def check_for_orbit(master, slave):
    flag = False
    if master.rel_orbit == slave.rel_orbit:
        flag = True
    return flag


def get_scene_id(product_path):
    product_basename = os.path.basename(product_path)
    if product_basename == 'manifest.safe':
        product_basename = product_path.split('/')[-1]
    if product_basename.endswith('.zip'):
        scene_id = product_basename.replace('.zip', '')
    elif product_basename.endswith('.SAFE'):
        scene_id = product_basename.replace('.SAFE', '')
    else:
        scene_id = product_basename
    return scene_id


file1 = '/home/suprd/PycharmProjects/_Sentinel-1_mosaic_test/git/OpenSarToolkit/tests/testdata/cache/S1A_IW_SLC__1SDV_20190101T171515_20190101T171542_025287_02CC09_0A0B.zip'
file2 = '/home/suprd/PycharmProjects/_Sentinel-1_mosaic_test/git/OpenSarToolkit/tests/testdata/cache/S1A_IW_SLC__1SDV_20190113T171514_20190113T171541_025462_02D252_C063.zip'

out_dir = '/home/suprd/OST_out_test/'
os.makedirs(out_dir, exist_ok=True)

s1 = Sentinel1Scenes(filelist=[file1, file2], processing_dir=out_dir, cleanup=True)

with TemporaryDirectory() as temp:
    s1.create_coherence(
        processing_dir=out_dir,
        temp_dir=temp,
        timeliness='14days',
        dem='SRTM 1Sec HGT',
        dem_file='',
        resolution=20,
        resampling='BILINEAR_INTERPOLATION',
        subset='POLYGON ((8.0419921875 46.34033203125, 8.0419921875 46.3623046875, 8.02001953125 46.3623046875, 8.02001953125 46.34033203125, 8.0419921875 46.34033203125))',
        )
