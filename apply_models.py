import pandas as pd
import numpy as np
import GrasslandModels
from glob import glob
import re

from tools.load_data import get_pixel_modis_data, marry_array_with_metadata

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
# TODO: make this a bit better with a metadata file
model_files = glob('fitted_models/*.json')
model_files = [f for f in model_files if 'scipy_output' not in f]

fitted_models = []
for f in model_files:
    m={}
    m['model'] = GrasslandModels.utils.load_saved_model(f)
    m['model_name'] = m['model']._get_model_info()['model_name']
    m['fitting_years'] = re.search(r'\d{8}', f).group()
    fitted_models.append(m)
    
######################################################
# Go thru all fitted models and apply them to all testing/training data

all_data = pd.DataFrame()
for year_set in training_year_sets:
    year_set_label = '{start}{end}'.format(start = min(year_set), end = max(year_set))
    
    for pixel_set, pixels in pixel_sets.items():
        for m in fitted_models:
            
            ndvi_observed, predictor_data, site_cols, date_rows = get_pixel_modis_data(pixels = pixels, years = year_set)
            
            model_predictors = {p:predictor_data[p] for p in m['model'].required_predictors()}
            
            ndvi_predicted = m['model'].predict(model_predictors)

            predicted_df = marry_array_with_metadata(ndvi_predicted, site_cols, date_rows, new_variable_colname='ndvi_predicted')
            observed_df  = marry_array_with_metadata(ndvi_observed, site_cols, date_rows, new_variable_colname='ndvi_observed')
            both = predicted_df.merge(observed_df, how='left', on=['date','pixel_id'])
            
            both['fitting_years'] = m['fitting_years']
            both['years'] = year_set_label
            both['model'] = m['model_name']
            both['pixel_set'] = pixel_set
            
            all_data = all_data.append(both)

all_data.to_csv(config['full_model_predictions_file'], index=False)
