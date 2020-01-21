import os
from os.path import join as opj
import logging
from datetime import datetime
from tempfile import TemporaryDirectory

from shapely.wkt import loads as shp_loads

from ost.s1.s1scene import Sentinel1Scene as S1scene
from ost.helpers.helpers import _slc_zip_to_processing_dir
from ost.helpers.bursts import get_bursts_pairs
from ost.s1.burst_to_ard import _2products_coherence_tc

logger = logging.getLogger(__name__)


class Sentinel1Scenes:

    def __init__(self, filelist, processing_dir=None, ard_type='OST', cleanup=False):
        slave_list = []
        for idx, s in zip(range(len(filelist)), filelist):
            if isinstance(s, S1scene):
                scene_id = s.scene_id
            else:
                scene_id = get_scene_id(product_path=s)
            if idx == 0:
                if isinstance(s, S1scene):
                    self.master = s
                else:
                    self.master = S1scene(scene_id=scene_id)
                    # Copy files to processing dir
                    _slc_zip_to_processing_dir(
                        processing_dir=processing_dir,
                        product=self.master,
                        product_path=s
                    )
            else:
                if isinstance(s, S1scene):
                    slave_list.append(s)
                else:
                    slave_list.append(S1scene(scene_id=scene_id))
                    # Copy files to processing dir
                    _slc_zip_to_processing_dir(
                        processing_dir=processing_dir,
                        product=S1scene(scene_id=scene_id),
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
            infile = self.master.get_path(download_dir=processing_dir)
            out_file = self.master.create_ard(
                infile=infile,
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
                infile = s.get_path(download_dir=processing_dir)
                out_file = s.create_ard(
                    infile=infile,
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
                            master_burst_poly=b_bbox,
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
