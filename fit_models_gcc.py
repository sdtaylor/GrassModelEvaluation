import pandas as pd
import numpy as np
import GrasslandModels

from tools.load_data import get_pixel_modis_data

from scipy import optimize
from time import sleep
import toolz

from dask_jobqueue import SLURMCluster
from dask.distributed import Client, as_completed
from dask import delayed
import dask

from tools.tools import load_config
config = load_config()

ceres_workers = 140 # the number of slurm job that will be spun up
ceres_cores_per_worker = 1 # number of cores per job
ceres_mem_per_worker   = '2GB' # memory for each job
ceres_worker_walltime  = '144:00:00' # the walltime for each worker, HH:MM:SS
ceres_partition        = 'medium'    # short: 48 hours, 55 nodes
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

# This is the callable sent to scipy.
def dask_scipy_mapper(func, iterable, c=dask_client):
    chunked_iterable = toolz.partition_all(10, iterable)
    results = [func(x) for x in chunked_iterable]
    futures =  dask_client.compute(results)
    return list(toolz.concat([f.result() for f in futures]))

######################################################
# model fitting delayed(func)(x)
######################################################
de_fitting_params = {
                     #'maxiter':500,
                     #'popsize':200,
                     'maxiter':20,
                     'popsize':2,
                     'mutation':(0.5,1),
                     'recombination':0.25,
                     'workers': dask_scipy_mapper,
                     'polish': False,
		     #'workers':2,
                     'disp':True}

# the search ranges for the model parameters
parameter_ranges = {'CholerPR1':{'b1':(0,200),
                                 'b2':(0,10),
                                 'b3':(0,10),
                                 'L' :(0,6)},
                    'CholerPR2':{'b1':(0,200),
                                 'b2':(0,10),
                                 'b3':(0,10),
                                 'b4':(0,200),
                                 'L' :(0,6)},
                    'CholerPR3':{'b1':(0,200),
                                 'b2':(0,10),
                                 'b3':(0,10),
                                 'b4':(0,200),
                                 'L' :(0,6)},
                    'PhenoGrassNDVI':{'b1': -1, # b1 is a not actually used in phenograss at the moment, 
                                                # see https://github.com/sdtaylor/GrasslandModels/issues/2
                                                # Setting to -1 makes it so the optimization doesn't waste time on b1
                                      'b2': (0,100), 
                                      'b3': (0,100),
                                      'b4': (0,100),
                                      'Phmax': (1,50),
                                      'Phmin': (1,50),
                                      'Topt': (0,45), 
                                      'L': (0,6)},
                    'Naive' : {'b1':(0,100),
                               'b2':(0,100),
                               'L': (0,6)}
                    }

optimal_nodes = {model:len(params)*200/10 for model, params in parameter_ranges.items()}

def load_model_and_data(model_name):
    ndvi, predictor_vars = GrasslandModels.utils.load_test_data()
    
    m = GrasslandModels.utils.load_model(model_name)(parameters = parameter_ranges[model_name])
    this_model_predictors = {p:predictor_vars[p] for p in m.required_predictors()}

    m.fit_load(ndvi, this_model_predictors, loss_function = 'mean_cvmae')

    return m

if __name__=='__main__':

    for model_name in ['CholerPR1','CholerPR2','CholerPR3','PhenoGrassNDVI','Naive']:
        
        # This future is the model, with fitting data, being loaded on all
        # the nodes by replicate()
        model_future = dask_client.submit(load_model_and_data, model_name = model_name)
        dask_client.replicate(model_future)
        
        # Keep a local model for some scipy fitting stuff
        local_model = model_future.result()
        scipy_bounds = local_model._scipy_bounds()
        
        # A wrapper to put into the scipy optimizer, it accepts a list of candidate
        # parameter sets to evaluate, and returns a list of scores.
        # The delayed wrapper and the use of the model future lets it be run across all the dask distributed nodes. 
        @delayed
        def minimize_me(scipy_parameter_sets):
            return [model_future.result()._scipy_error(param_set) for param_set in scipy_parameter_sets]

        # This kicks off all the parallel work
        params =  optimize.differential_evolution(minimize_me, bounds=scipy_bounds, **de_fitting_params)
        
        # Map the scipy results output back to model parameters and save results
        local_model._fitted_params = local_model._translate_scipy_parameters(params['x'])
        
        model_filename = 'GCC_{m}.json'.format(m = model_name)
        local_model.save_params('fitted_models/' + model_filename, overwrite=True)

        # Also write the scipy output which logs the fitting details
        scipy_output_filename  = 'scipy_output_' + model_filename
        _ = params.pop('x')
        GrasslandModels.models.utils.misc.write_saved_model(dict(params), model_file='fitted_models/' + scipy_output_filename, overwrite=True)
