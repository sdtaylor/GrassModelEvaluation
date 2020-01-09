import pandas as pd
import numpy as np
import GrasslandModels

from tools.load_data import get_pixel_modis_data, marry_array_with_metadata

from tools.tools import load_config
config = load_config()


######################################################
# Setup fitting/testing clusters
pixel_info = pd.read_csv('data/random_points.csv')
pixel_info = pixel_info[pixel_info.percent_years_as_grass == 1]

training_pixels = pixel_info[pixel_info.is_training].pixel_id.values
testing_pixels = pixel_info[pixel_info.is_testing].pixel_id.values

training_year_sets = [range(year_set['start'],year_set['end']+1) for year_set in config['training_year_sets']]
######################################################


ndvi, predictor_vars, site_columns, date_rows = get_pixel_modis_data(years = training_year_sets[0],
                                                                     pixels = testing_pixels)

m1 = GrasslandModels.utils.load_saved_model('fitted_models/CholerPR1_20012009.json')
m1_predictor_vars = {p:predictor_vars[p] for p in m1.required_predictors()}
ndvi_predicted = m1.predict(m1_predictor_vars)

ndvi_predicted_df = marry_array_with_metadata(ndvi_predicted, site_columns, date_rows, new_variable_colname='ndvi_predicted')
ndvi_predicted_df['model'] = m1._get_model_info()['model_name']


# pseudo code
for model in fitted_model:
    load model
    get model fitting years
    
    for years in training_year_sets:
        for pixel_set in [training_pixels, fitting_pixels]:
            
            get_predictor_data
            subset to model_predictors
            
            predicted_ndvi = model.predict()
            
            make_predicted_df
            make_observed_df and join
            
            log training/testing pixels
            log testing years
            log fitting years
            log model
            
            append to master_df




