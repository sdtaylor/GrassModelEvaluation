import pandas as pd
import numpy as np
from GrasslandModels import et_utils



##############################
# Create bins to make averages/summations of climatic data at the 16 day
# scale of modis data
#############################
modis_doys = np.array([1,17,33,49,65,81,97,113,129,145,161,177,193,209,225,241,257,273,289,305,321,337,353])
all_doys = list(range(1,366))
doy_bins = []
for d in all_doys:
    if d > 353:
        this_bin = 1 # days at the end of the year get aggregated to doy 1 of the next year
    else:
        this_bin = modis_doys[modis_doys>=d].min()
    doy_bins.append(this_bin)

doy_bin_info = pd.DataFrame({'doy':all_doys, 'doy_bin':doy_bins})
#############################

def filter_data(df, y, p):
    return df[(df.year.isin(y)) & (df.pixel_id.isin(p))]

def long_to_wide(df, index_column, value_column):
    return df[['pixel_id',index_column,value_column]].pivot_table(index = index_column, columns='pixel_id', 
                                                                  values=value_column, dropna=False)

def get_pixel_modis_data(years = range(2001,2019), pixels = 'all', predictor_lag = 5):
    """
    Load MODIS NDVI and associated predictor data (daymet preicp & temp, ET, daylength, etc)
    Parameters
    ----------
    years : array of years or 'all', optional
        whichs years to of NDVI and associated predictor 
        data to return. The default is 'all'.
    pixels : array of pixel_ids or 'all', optional
        whichs pixels to return. The default is 'all'.
    predictor_lag : int, optional
        how many years of predictor variables (ie. precip, temp,
        evap) prior to the start of NDVI values to keep.
        This allows spin up of state variables leading up to actual NDVI values 
        to fit. The default is 5.
        NDVI for these preceding years will be NA such that the array shape
        is consistant throughout.

    Returns
    -------
    Tuple of NDVI, {'evap': evap, 'precip':precip, ...} where everything is
    a (timestep,pixel_id) array. 

    """
    pixel_info = pd.read_csv('data/random_points.csv')    
    ndvi_data = pd.read_csv('data/processed_ndvi.csv').drop('date', axis=1)
    daymet_data = pd.read_csv('data/daymet_data.csv')
    soil_data = pd.read_csv('data/processed_soil_data.csv')
    
    if years == 'all':
        years = ndvi_data.year.unique()
    else:
        years = np.array(years)
    if pixels == 'all':
        pixels = ndvi_data.pixel_id.unique()
    else:
        pixels = np.array(pixels)
    
    ndvi_data = filter_data(ndvi_data, years, pixels)
    predictor_years = np.append(list(range(min(years) - predictor_lag, min(years))),years)
    daymet_data = filter_data(daymet_data, predictor_years, pixels)
    soil_data = soil_data[soil_data.pixel_id.isin(pixels)]
    
    # Make sure everything is accounted for
    assert daymet_data.groupby(['year','pixel_id']).count().doy.unique()[0] == 365, 'daymet data has some years with < 365 days'
    assert np.isin(daymet_data.year.unique(), predictor_years).all(), 'extra years in daymet data'
    assert np.isin(predictor_years,daymet_data.year.unique()).all(), 'not all predictor years in daymet data'
    assert ndvi_data.groupby(['year','pixel_id']).count().doy.unique()[0] == 23, 'MODIS NDVI has some years with < 23 entries'
    assert np.isin(ndvi_data.year.unique(), years).all(), 'extra years in MODIS NDVI data'
    assert np.isin(years,ndvi_data.year.unique()).all(), 'not all years in MODIS NDVI data'
    
       
    # daily mean temperature
    daymet_data['tmean'] = (daymet_data.tmin + daymet_data.tmax) / 2

    # pull in latitude for ET calculation
    daymet_data = daymet_data.merge(pixel_info[['pixel_id','lat']], how='left', on='pixel_id')
    
    # Estimate ET from tmin, tmax and latitude
    latitude_radians = et_utils.deg2rad(daymet_data.lat.values)
    solar_dec = et_utils.sol_dec(daymet_data.doy.values)
    sha = et_utils.sunset_hour_angle(latitude_radians, solar_dec)
    ird = et_utils.inv_rel_dist_earth_sun(daymet_data.doy.values)
    daymet_data['radiation'] = et_utils.et_rad(latitude_radians, solar_dec, sha, ird)
    daymet_data['et'] = et_utils.hargreaves(tmin = daymet_data.tmin.values, tmax = daymet_data.tmax.values,
                                         et_rad = daymet_data.radiation.values)


    # Aggregate everything to the 16 day modis scale. Not fluxes are sums
    # First assign end of year data to the NDVI values for doy=1 of the *next* year
    year_plus1 = daymet_data.year + 1
    assign_to_next_year = daymet_data.doy > 353
    daymet_data = daymet_data.merge(doy_bin_info, how='left', on='doy')
    daymet_data.loc[assign_to_next_year, 'year'] = year_plus1[assign_to_next_year]
    
    daymet_data_aggregated = daymet_data.groupby(['pixel_id','year','doy_bin']).agg({'precip':np.sum, 
                                                                                     'et':np.sum,
                                                                                     'radiation': np.mean,
                                                                                     'daylength': np.mean,
                                                                                     'tmean': np.mean,
                                                                                     'tmax': np.mean,
                                                                                     'tmin': np.mean}).reset_index()
    daymet_data_aggregated.rename(columns={'doy_bin':'doy'}, inplace=True)
    combined_year_doy = daymet_data_aggregated.year.astype(str) + '-' + daymet_data_aggregated.doy.astype(str)
    daymet_data_aggregated['date'] = pd.to_datetime(combined_year_doy, format='%Y-%j')
    
    # Full outer join to combine all selected years + the preceding predictor years
    # This will make NA ndvi values for those preceding years
    everything = ndvi_data.merge(daymet_data_aggregated, how='outer', on=['year','doy','pixel_id'])
    everything.sort_values(['pixel_id','date'], inplace=True)
    
    # Soil data is a single value/pixel, so it doesn't need combining with everything else.
    # Makes 1d arrays of length n_pixels, just have to make sure they're ordered
    soil_data.sort_values('pixel_id', inplace=True)
    
    # Ensure pixel are being aligned in the arrays correctly
    assert (long_to_wide(everything, index_column = 'date', value_column = 'ndvi').columns == soil_data.pixel_id).all(), 'predictor data.frame not aligning with soil data.frame'
    assert (long_to_wide(everything, index_column = 'date', value_column = 'ndvi').columns == long_to_wide(everything, index_column = 'date', value_column = 'tmean').columns).all(), 'tmean and ndvi columns not lining up'
    # produce time x site arrays.
    ndvi_array = long_to_wide(everything, index_column = 'date', value_column = 'ndvi').values

    predictor_vars = {}
    predictor_vars['precip'] = long_to_wide(everything, index_column = 'date', value_column = 'precip').values
    predictor_vars['evap'] = long_to_wide(everything, index_column = 'date', value_column = 'et').values
    predictor_vars['Tm'] = long_to_wide(everything, index_column = 'date', value_column = 'tmean').values
    predictor_vars['Ra'] = long_to_wide(everything, index_column = 'date', value_column = 'radiation').values
    predictor_vars['Wp'] = soil_data.Wp.values
    predictor_vars['Wcap'] = soil_data.Wcap.values
    
    return ndvi_array, predictor_vars
