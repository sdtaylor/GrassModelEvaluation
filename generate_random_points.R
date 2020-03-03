library(MODISTools)
library(rgdal)
library(tidyverse)

source('tools/tools.R')
config = load_config()


###########################
# Make a geojson of random points within AUS grasslands
###########################
# This is a conversion of the original landcover raster to just class
# 34 using QGIS
aus_grassland = raster::raster('data/landcover/aus_grassland_class34.tif')

set.seed(100)
random_point_count = config$random_point_count
percent_training = config$percent_training

all_grassland_points = which(raster::values(aus_grassland)==1)
random_points = sample(all_grassland_points, size=400)

random_point_locations <- as_tibble(raster::xyFromCell(aus_grassland, random_points))
library(sf)
samp_sf <- st_as_sf(random_point_locations, coords = c('x', 'y'), crs = 3577)

samp_sf = st_transform(samp_sf, crs=4326)

st_write(samp_sf, dsn='data/aus_random_points.geojson', driver='GEOJson', delete_dsn=T)

random_points = sp::spsample(aus_grassland, n=random_point_count, type='random')
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
