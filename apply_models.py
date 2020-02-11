import pandas as pd
import numpy as np
import GrasslandModels
from glob import glob
import re

from tools.load_data import get_pixel_modis_data, marry_array_with_metadata
from tools import load_models

from tools.tools import load_config
config = load_config()

######################################################
# Setup fitting/testing clusters
pixel_info = pd.read_csv('data/random_points.csv')
pixel_info = pixel_info[pixel_info.percent_years_as_grass == 1]

training_pixels = pixel_info[pixel_info.is_training].pixel_id.values
testing_pixels = pixel_info[pixel_info.is_testing].pixel_id.values
pixel_sets = {'training':training_pixels,
              'testing' :testing_pixels}

training_year_sets = [range(year_set['start'],year_set['end']+1) for year_set in config['training_year_sets']]
######################################################
# fitted models to apply
model_set_id = ''
model_set = load_models.load_model_set(model_set_id)

fitted_models = [{'model':m,'model_name':n,'fitting_years':'20012009'} for m,n in zip(model_set['models'],model_set['model_names'])]


##########################
# Optionally put in the original parameters from the papers
# uncomment append statements below to add them.
cholerpr1_original = {}
cholerpr1_original['model'] = GrasslandModels.utils.load_prefit_model('CholerPR1-original')
cholerpr1_original['model_name'] = 'CholerPR1-original'
cholerpr1_original['fitting_years'] = '19902000'

cholerpr2_original = {}
cholerpr2_original['model'] = GrasslandModels.utils.load_prefit_model('CholerPR2-original')
cholerpr2_original['model_name'] = 'CholerPR2-original'
cholerpr2_original['fitting_years'] = '19902000'

#fitted_models.append(cholerpr1_original)
#fitted_models.append(cholerpr2_original)

######################################################
# Go thru all fitted models and apply them to all testing/training data

all_data = pd.DataFrame()
for year_set in training_year_sets:
    year_set_label = '{start}{end}'.format(start = min(year_set), end = max(year_set))
    
    for pixel_set, pixels in pixel_sets.items():
        
        ndvi_observed, predictor_data, site_cols, date_rows = get_pixel_modis_data(pixels = pixels, years = year_set)
        for m in fitted_models:
            
            model_predictors = {p:predictor_data[p] for p in m['model'].required_predictors()}
            model_output = m['model'].predict(predictors = model_predictors, return_variables='all')
            
            # Each model outputs several state variabes, this creates a data.frame of the form
            # pixel_id, date, ndvi_predicted, ndvi_observed, state_var1, state_var2, ....
            predicted_df = marry_array_with_metadata(model_output.pop('V'), site_cols, date_rows, new_variable_colname='ndvi_predicted')
            observed_df  = marry_array_with_metadata(ndvi_observed, site_cols, date_rows, new_variable_colname='ndvi_observed')
            all_variables = predicted_df.merge(observed_df, how='left', on=['date','pixel_id'])
            
            for variable_name, variable_array in model_output.items():
                variable_df = marry_array_with_metadata(variable_array, site_cols, date_rows, new_variable_colname=variable_name)
                all_variables = all_variables.merge(variable_df, how='left', on=['date','pixel_id'] )
            
            precip_df = marry_array_with_metadata(predictor_data['precip'], site_cols, date_rows, new_variable_colname='precip')
            et_df = marry_array_with_metadata(predictor_data['evap'], site_cols, date_rows, new_variable_colname='et')
            all_variables = all_variables.merge(precip_df, how='left', on=['date','pixel_id']).merge(et_df, how='left', on=['date','pixel_id'])

            all_variables['fitting_years'] = m['fitting_years']
            all_variables['years'] = year_set_label
            all_variables['model'] = m['model_name']
            all_variables['pixel_set'] = pixel_set
            
            all_data = all_data.append(all_variables)

all_data.to_csv(config['full_model_predictions_file'], index=False)
