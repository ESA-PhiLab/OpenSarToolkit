import os
from os.path import join as opj
import logging
import rasterio
import warnings
from datetime import datetime
from tempfile import TemporaryDirectory

from rasterio.errors import NotGeoreferencedWarning

from shapely.wkt import loads as shp_loads

from ost.s1.s1scene import Sentinel1Scene as S1scene
from ost.helpers import helpers as h
from ost.s1.burst_to_ard import _import, _coreg2, _coherence, _terrain_correction
from ost.helpers.helpers import _slc_zip_to_processing_dir
from ost.helpers.bursts import get_bursts_pairs

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
        self.ard_type = ard_type
        if ard_type is not None:
            self.master.set_ard_parameters(ard_type)
            self.master.ard_parameters['product_type'] = (ard_type)

    def s1_scenes_to_ard(self,
                         processing_dir,
                         subset=None,
                         ):
        if self.ard_type is None:
            raise RuntimeError('Need to specify or setup ard_type')
        if self.master.ard_parameters['type'] is None:
            raise RuntimeError('Need to specify or setup ard_type')
        # more custom ARD params, see whats in s1scene.py and complement corespondingly
        out_files = []
        # process the master to ARD first
        with TemporaryDirectory() as temp:
            out_file = self.master.create_ard(
                infile=None,
                out_dir=processing_dir,
                out_prefix=self.master.scene_id,
                temp_dir=temp,
                subset=subset,
                polar='VV,VH,HH,HV'
            )
        out_files.append(out_file)
        # Process all slaves as ARD
        for s in self.slaves:
            with TemporaryDirectory() as temp:
                s.ard_parameters = self.master.ard_parameters
                out_file = s.create_ard(
                    infile=None,
                    out_dir=processing_dir,
                    out_prefix=s.scene_id,
                    temp_dir=temp,
                    subset=subset,
                    polar='VV,VH,HH,HV'
                )
                out_files.append(out_file)
        self.ard_dimap = out_files
        return out_files

    def get_weekly_pairs(self):
        master_date = datetime.strptime(self.master.start_date, '%Y%m%d')

        slave_dates = [datetime.strptime(slave.start_date, '%Y%m%d')
                       for slave in self.slaves
                       ]
        weekly_pairs = []
        for s, slave in zip(slave_dates, self.slaves):
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
        master_date = datetime.strptime(self.master.start_date, '%Y%m%d')

        slave_dates = [datetime.strptime(slave.start_date, '%Y%m%d')
                       for slave in self.slaves
                       ]
        biweekly_pairs = []
        for s, slave in zip(slave_dates, self.slaves):
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
            subset=None,
    ):
        if self.master.product_type != 'SLC':
            raise TypeError('create_coherence needs SLC products')
        if timeliness == '14days':
            pairs = self.get_biweekly_pairs()
        elif timeliness == '7days':
            pairs = self.get_weekly_pairs()
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
        coh_list = []
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
            if self.master.ard_parameters['dem'] != 'External DEM':
                self.master.ard_parameters['dem_file'] = ''
            for swath, b in bursts_dict.items():
                if b != []:
                    for burst in b:
                        m_nr, m_burst_id, sl_burst_nr, sl_burst_id, b_bbox = burst
                        return_code = _2products_coherence_tc(
                            master_scene=pair[0],
                            master_file=master_file,
                            slave_scene=pair[1],
                            slave_file=slave_file,
                            out_dir=processing_dir,
                            temp_dir=temp_dir,
                            swath=swath,
                            master_burst_id=m_burst_id,
                            master_burst_nr=m_nr,
                            slave_burst_id=sl_burst_id,
                            slave_burst_nr=sl_burst_nr,
                            resolution=self.master.ard_parameters['resolution'],
                            dem=self.master.ard_parameters['dem'],
                            dem_file=self.master.ard_parameters['dem_file'],
                            resampling=self.master.ard_parameters['resampling']
                        )
                        if return_code == 333:
                            logger.debug('Burst %s is empty', m_burst_id)
                            continue
                        elif return_code != 0:
                            print(return_code)
                            raise RuntimeError
                        out_file = opj(processing_dir, str(m_burst_id)+'_coh.dim')
                        if os.path.isfile(out_file):
                            coh_list.append(out_file)
        self.coherece_dimap = coh_list
        return coh_list


def _2products_coherence_tc(
        master_scene,
        master_file,
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

from ost.log import setup_logfile, set_log_level
set_log_level(logging.DEBUG)
setup_logfile('/home/suprd/OST_out_test/log.log')
out_dir = '/home/suprd/OST_out_test/'
os.makedirs(out_dir, exist_ok=True)

s1 = Sentinel1Scenes(filelist=[file1, file2],
                     processing_dir=out_dir,
                     cleanup=True,
                     ard_type='RTC'
                     # ard_type='GTCgamma'
                     )

with TemporaryDirectory() as temp:
    s1.master.ard_parameters['to_db'] = True
    s1.master.ard_parameters['resampling'] = 'BILINEAR_INTERPOLATION'
    # s1.s1_scenes_to_ard(
    #     processing_dir=out_dir,
    #     subset='POLYGON ((8.0419921875 46.34033203125, 8.0419921875 46.3623046875, 8.02001953125 46.3623046875, 8.02001953125 46.34033203125, 8.0419921875 46.34033203125))',
    # )
    s1.create_coherence(
        processing_dir=out_dir,
        temp_dir=temp,
        timeliness='14days',
        subset='POLYGON ((8.0419921875 46.34033203125, 8.0419921875 46.3623046875, 8.02001953125 46.3623046875, 8.02001953125 46.34033203125, 8.0419921875 46.34033203125))',
        # subset=None,
        )
