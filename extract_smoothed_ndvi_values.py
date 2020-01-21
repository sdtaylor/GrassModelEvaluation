import pandas as pd
import xarray as xr

from glob import glob

from tools.tools import load_config
config = load_config()

pixel_info = pd.read_csv(config['random_point_file'])
pixel_info = pixel_info[pixel_info.percent_years_as_grass == 1]


file_list = glob(config['smoothed_ndvi_folder'] + '*.nc4')

all_ndvi_values = pd.DataFrame()

tots_pixels = len(pixel_info)

for i, pixel in pixel_info.reset_index().iterrows():
    for ndvi_file_i, ndvi_file in enumerate(file_list):
        ndvi_obj = xr.open_dataset(ndvi_file)
        ndvi_obj.load()
        
        print('pixel {n}/{N}, ndvi year {y}/{Y}'.format(n=i, N=tots_pixels, y=ndvi_file_i, Y=len(file_list)))
        ndvi_subset = ndvi_obj.sel(x=pixel['lon'], y=pixel['lat'], method='nearest')
        
        ndvi_subset = ndvi_subset.to_dataframe().reset_index()[['time','NDVI']]
    
        ndvi_subset.rename(columns = {'NDVI':'ndvi', 'time':'date'}, inplace=True)
        
        # The date represents the midpoint of the 8 day time period, add 3 days so
        # it represents the last day of the 8 day time window. This allows climate 
        # data to be summarized correctly (ie. the total precip over the 8 day window)
        ndvi_subset['date'] = ndvi_subset.date + pd.Timedelta('3d')
    
        ndvi_subset['pixel_id'] = pixel['pixel_id']
        
        all_ndvi_values = all_ndvi_values.append(ndvi_subset)

all_ndvi_values['year'] = pd.DatetimeIndex(all_ndvi_values.date).year
all_ndvi_values['doy'] = pd.DatetimeIndex(all_ndvi_values.date).dayofyear

all_ndvi_values.to_csv('data/processed_ndvi.csv', index=False)