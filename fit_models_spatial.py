import pandas as pd
import numpy as np
import GrasslandModels

from tools.load_data import get_pixel_modis_data
from tools import load_models

from scipy import optimize
from time import sleep
import toolz

from tools.tools import load_config
config = load_config()




######################################################
# Setup fitting/testing clusters
pixel_info = pd.read_csv('data/random_points.csv')
pixel_info = pixel_info[pixel_info.percent_years_as_grass == 1]

pixel_info = pixel_info[pixel_info.lat.between(41,49) & pixel_info.lon.between(-110,-104)]

training_pixels = pixel_info[pixel_info.is_training].pixel_id.values

wy_pixels = pixel_info[pixel_info.lat.between(41,49) & pixel_info.lon.between(-110,-104)].pixel_id.values
training_pixels = [86]
training_year_sets = [range(year_set['start'],year_set['end']+1) for year_set in config['training_year_sets']]

######################################################
# model fitting delayed(func)(x)
######################################################


def mae_loss(obs, pred):
    return np.nanmean(np.abs(obs-pred))

loss_function = 'mean_cvmae'

de_fitting_params = {
                     'maxiter':1000,
                     'popsize':50,
                     #'maxiter':3,
                     #'popsize':20,
                     'mutation':(0.5,1),
                     'recombination':0.25,
                     'workers': 1,
                     'polish': False,
		     #'workers':2,
                     'disp':True}

# the search ranges for the model parameters
parameter_ranges = {'CholerPR1':{'b1':(0,200),
                                 'b2':(0,10),
                                 'b3':(0,10),
                                 'L' : 2},
                    'CholerPR2':{'b1':(0,200),
                                 'b2':(0,10),
                                 'b3':(0,10),
                                 'b4':(0,200),
                                 'L' : 2},
                    'CholerPR3':{'b1':(0,200),
                                 'b2':(0,10),
                                 'b3':(0,10),
                                 'b4':(0,200),
                                 'L' : 2},
                    'CholerMPR2':{'b2':(0,10),
                                  'b3':(0,10),
                                  'b4':(0,200),
                                  'L' :2},
                    'CholerMPR3':{'b2':(0,10),
                                  'b3':(0,10),
                                  'b4':(0,200),
                                  'L' :2},
                    'CholerM1A':  {'a1':(0,10),
                                  'a2':(0,10),
                                  'a3':0,
                                  'L': 2},
                    'PhenoGrassNDVI':{'b1': -1, # b1 is a not actually used in phenograss at the moment, 
                                                # see https://github.com/sdtaylor/GrasslandModels/issues/2
                                                # Setting to -1 makes it so the optimization doesn't waste time on b1
                                      'b2': (0,100), 
                                      'b3': (0,100),
                                      'b4': (0,100),
                                      'Phmax': (1,50),
                                      'Phmin': (1,50),
                                      'Topt': (0,45), 
                                      'L': (0,8)},
                    'Naive' : {'b1':(0,100),
                               'b2':(0,100),
                               'L': (0,8)}
                    }


if __name__=='__main__':
    fit_models = []
    
    training_years = training_year_sets[0]
    for training_years in training_year_sets[0:1]:
        ndvi, predictor_vars, _, _ = get_pixel_modis_data(years = training_years, pixels = training_pixels)
        
        #for model_name in ['Naive','CholerPR1','CholerPR2','CholerPR3','PhenoGrassNDVI']:
        for model_name in ['Naive','CholerMPR2','CholerMPR3','CholerPR1','CholerPR2','CholerPR3']:
            
            local_model = GrasslandModels.utils.load_model(model_name)(parameters = parameter_ranges[model_name])            
            this_model_predictors = {p:predictor_vars[p] for p in local_model.required_predictors()}
            
            local_model.fit(ndvi, this_model_predictors, optimizer_params = de_fitting_params, loss_function = 'rmse')
            
            fit_models.append(local_model)
    
    
    # compile all the models into a set and save
    model_set = load_models.make_model_set(fit_models, note='fitting with only pixel_id==86, (in WY). still using custom smoothed (but not scaled) ndvi data.')
    load_models.save_model_set(model_set)


