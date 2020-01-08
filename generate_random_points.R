library(MODISTools)
library(rgdal)
library(tidyverse)
library(rnaturalearth)

source('tools/tools.R')
config = load_config()

###########################
# Make a geojson of random points within the western USA 
###########################
set.seed(100)
random_point_count = config$random_point_count
percent_training = config$percent_training

us_states = c('Washington','Oregon','California','Montana','Idaho','Nevada',
              'Arizona','Wyoming','Colorado','Utah','New Mexico','Texas',
              'Oklahoma','Nebraska','Kansas','South Dakota','North Dakota')
ca_states = c('British Columbia','Alberta','Saskatchewan','Manitoba')
mx_states = c('Baja California','Sonora','Chihuahua','Coahuila')

western_na = rnaturalearth::ne_states(country = c('mexico','canada','united states of america'), returnclass = 'sp')
#western_na = rnaturalearth::ne_states(country = 'united states of america', returnclass = 'sp')
western_na = subset(western_na, name %in% c(ca_states, us_states, mx_states))

random_points = sp::spsample(western_na, n=random_point_count, type='random')
random_points = SpatialPointsDataFrame(random_points, data=data.frame(pixel_id = 1:length(random_points)))

#sf::st_write(random_points, dsn='data/random_points.shp', driver='ESRI Shapefile', delete_dsn=T)
#sf::st_write(random_points, dsn='./data/random_points.geojson', driver='GEOJson', delete_dsn=T)

###############################
###############################
#random_points = readOGR(dsn='data/random_points.geojson')


# MODISTools::mt_dates('MCD12Q1', lat=40, lon=-100)
# MODISTools::mt_bands('MCD12Q1')

batch_df = data.frame(random_points@coords)
colnames(batch_df) = c('lon','lat')
batch_df$site_name = random_points$pixel_id
  
bands_to_get = c('LC_Type1')

get_band_data=function(b){
  mt_batch_subset(product = 'MCD12Q1',
                    band = b,
                    df = batch_df,
                    start='2001-01-01',
                    end = '2017-01-01'
                    )
}

pixel_landcover = purrr::map_dfr(bands_to_get, get_band_data)

# Keep the original yearly pixel values for safekeeping
write_csv(pixel_landcover, 'data/random_pixel_yearly_landcover.csv')

pixel_landcover = pixel_landcover %>%
  mutate(year = as.numeric(substr(modis_date, 2,5))) %>%
  mutate(site = as.numeric(site)) %>%
  select(pixel_id = site, year, landcover_id = value)

# Percentage of time the pixel was a grassland/savanah
grass_percent =pixel_landcover %>%
  group_by(pixel_id) %>%
  summarise(percent_years_as_grass = mean(landcover_id==10)) %>%
  ungroup() 

random_points@data = random_points@data %>%
  left_join(grass_percent, by='pixel_id')

# Mark testing/training subsets
training_pixels = 1:as.integer(random_point_count * percent_training)
random_points@data$is_training = F
random_points@data$is_training[training_pixels] = T
random_points@data$is_testing = !random_points@data$is_training

# save as shapefile and plain csv
writeOGR(random_points, 'data/random_points.geojson',layer='', driver='GeoJSON')

random_points@coords %>%
  as.data.frame() %>%
  rename(lon = x, lat=y) %>%
  bind_cols(random_points@data) %>%
  write_csv('data/random_points.csv')
