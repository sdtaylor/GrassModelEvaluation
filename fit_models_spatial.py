import pandas as pd
import numpy as np
import GrasslandModels

from tools.load_data import get_pixel_modis_data

# from scipy import optimize
# from time import sleep
# import toolz

# from dask_jobqueue import SLURMCluster
# from dask.distributed import Client, as_completed
# from dask import delayed
# import dask

from tools.tools import load_config
config = load_config()

# ceres_workers = 140 # the number of slurm job that will be spun up
# ceres_cores_per_worker = 1 # number of cores per job
# ceres_mem_per_worker   = '2GB' # memory for each job
# ceres_worker_walltime  = '144:00:00' # the walltime for each worker, HH:MM:SS
# ceres_partition        = 'medium'    # short: 48 hours, 55 nodes
#                                     # medium: 7 days, 25 nodes
#                                     # long:  21 days, 15 nodes



######################################################
# Setup fitting/testing clusters
pixel_info = pd.read_csv('data/random_points.csv')
pixel_info = pixel_info[pixel_info.percent_years_as_grass == 1]

# subset to pixels just in WY & Eastern MT
wy_pixels = pixel_info[pixel_info.lat.between(41,49) & pixel_info.lon.between(-110,-104)].pixel_id.values

#training_pixels = pixel_info[pixel_info.is_training].pixel_id.values

training_year_sets = [range(year_set['start'],year_set['end']+1) for year_set in config['training_year_sets']]
######################################################
# Setup dask cluster
######################################################
# cluster = SLURMCluster(processes=1,queue=ceres_partition, cores=ceres_cores_per_worker, memory=ceres_mem_per_worker, walltime=ceres_worker_walltime,
#                        job_extra=[],
#                        death_timeout=600, local_directory='/tmp/')

# print('Starting up workers')
# workers = cluster.scale(n=ceres_workers)
# dask_client = Client(cluster)

# active_workers =  len(dask_client.scheduler_info()['workers'])
# while active_workers < (ceres_workers-1):
#     print('waiting on workers: {a}/{b}'.format(a=active_workers, b=ceres_workers))
#     sleep(5)
#     active_workers =  len(dask_client.scheduler_info()['workers'])
# print('all workers online')

# # This is the callable sent to scipy.
# def dask_scipy_mapper(func, iterable, c=dask_client):
#     chunked_iterable = toolz.partition_all(10, iterable)
#     results = [func(x) for x in chunked_iterable]
#     futures =  dask_client.compute(results)
#     return list(toolz.concat([f.result() for f in futures]))

######################################################
# model fitting delayed(func)(x)
######################################################
de_fitting_params = {
                     'maxiter':500,
                     'popsize':200,
                     #'maxiter':20,
                     #'popsize':4,
                     'mutation':(0.5,1),
                     'recombination':0.25,
                     'workers': 8,
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


if __name__=='__main__':

    for training_years in training_year_sets:
        ndvi, predictor_vars, _, _ = get_pixel_modis_data(years = training_years, pixels = wy_pixels)
        for model_name in ['PhenoGrassNDVI','Naive']:
            local_model = GrasslandModels.utils.load_model(model_name)(parameters = parameter_ranges[model_name])            
            this_model_predictors = {p:predictor_vars[p] for p in local_model.required_predictors()}
            
            local_model.fit(ndvi, this_model_predictors, optimizer_params = de_fitting_params, loss_function = 'mean_cvmae')
            
            model_filename = 'WY_{m}_{y1}{y2}.json'.format(m = model_name, y1=min(training_years), y2=max(training_years))
            local_model.save_params('fitted_models/' + model_filename, overwrite=True)
    
