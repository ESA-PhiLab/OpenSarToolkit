import os
import logging

from godale._concurrent import Executor

from ost.s1.burst_to_ard import burst_to_ard
from ost.s1.burst import prepare_burst_inventory

logger = logging.getLogger(__name__)


def burst_to_ard_batch(burst_inv,
                       project_dict,
                       executor_type='billiard',
                       ncores=os.cpu_count()
                       ):
    proc_inventory = prepare_burst_inventory(burst_inv, project_dict)
    if ncores == 1:
        project_dict['cpus_per_process'] = os.cpu_count()
        for burst in proc_inventory.iterrows():
            burst_to_ard(burst=burst,
                         ard_params=project_dict['processing'],
                         project_dict=project_dict
                         )
    else:
        project_dict['cpus_per_process'] = 2
        executor = Executor(executor=executor_type, max_workers=ncores)
        for task in executor.as_completed(
                func=burst_to_ard,
                iterable=proc_inventory.iterrows(),
                fargs=[project_dict['processing_parameters'], project_dict]
        ):
            task.result()
