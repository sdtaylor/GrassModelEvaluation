import pandas as pd
import numpy as np
from GrasslandModels import utils, models

from scipy import optimize
from time import sleep
import toolz

from dask_jobqueue import SLURMCluster
from dask.distributed import Client, as_completed
from dask import delayed
import dask


ceres_workers = 200 # the number of slurm job that will be spun up
ceres_cores_per_worker = 1 # number of cores per job
ceres_mem_per_worker   = '500MB' # memory for each job
ceres_worker_walltime  = '08:00:00' # the walltime for each worker, HH:MM:SS
ceres_partition        = 'short'    # short: 48 hours, 55 nodes
                                    # medium: 7 days, 25 nodes
                                    # long:  21 days, 15 nodes

######################################################
# Setup dask cluster
######################################################
cluster = SLURMCluster(processes=1,queue=ceres_partition, cores=ceres_cores_per_worker, memory=ceres_mem_per_worker, walltime=ceres_worker_walltime,
                       job_extra=[],
                       death_timeout=600, local_directory='/tmp/')

print('Starting up workers')
workers = cluster.scale(n=ceres_workers)
dask_client = Client(cluster)

active_workers =  len(dask_client.scheduler_info()['workers'])
while active_workers < (ceres_workers-1):
    print('waiting on workers: {a}/{b}'.format(a=active_workers, b=ceres_workers))
    sleep(5)
    active_workers =  len(dask_client.scheduler_info()['workers'])
print('all workers online')

def dask_scipy_mapper(func, iterable, c=dask_client):
    chunked_iterable = toolz.partition_all(9, iterable)
    results = [func(x) for x in chunked_iterable]
    futures =  dask_client.compute(results)
    return list(toolz.concat([f.result() for f in futures]))

######################################################
# model fitting
######################################################
de_fitting_params = {'maxiter':500,
                     'popsize':200,
                     'mutation':(0.5,1),
                     'recombination':0.25,
                     'workers': dask_scipy_mapper,
                     'polish': False,
		     #'workers':2,
                     'disp':True}
    
def load_model():
    GCC, predictor_vars = utils.load_test_data()
    
    #GCC = np.repeat(GCC, 10, axis=0)
    #for k in ['precip', 'evap', 'Tm', 'Ra']:
    #    predictor_vars[k] = np.repeat(predictor_vars[k], 10, axis=0)
        
    m = models.PhenoGrass()
    m.fitting_predictors = predictor_vars
    m.obs_fitting = GCC
    m._set_loss_function('mean_cvmae')

    return m

if __name__=='__main__':

    model_future = dask_client.submit(load_model)
    local_model = model_future.result()
    dask_client.replicate(model_future)

    @delayed
    def minimize_me(x):
        return [model_future.result()._scipy_error(params) for params in x]

    scipy_bounds = local_model._scipy_bounds()

    params =  optimize.differential_evolution(minimize_me, bounds=scipy_bounds, **de_fitting_params)
    local_model._fitted_params = local_model._translate_scipy_parameters(params['x'])
    local_model.save_params('fitted_parameters.json', overwrite=True)
    
    _ = params.pop('x')
    models.utils.misc.write_saved_model(dict(params), model_file='scipy_output.json', overwrite=True)
