library(raster)
library(rgdal)

# Extract wilting point and water holding capactiy from the Global Gridded Surfaces of Selected Soil Characteristics (IGBP-DIS)


Wp_raster = raster('./data/soil_rasters/wiltpont.dat')
Wcap_raster = raster('./data/soil_rasters/fieldcap.dat')

random_points = rgdal::readOGR('data/random_points.geojson')
random_points = random_points[random_points$percent_years_as_grass == 1,]

random_points$Wp = raster::extract(Wp_raster, random_points)
random_points$Wcap = raster::extract(Wcap_raster, random_points)


soil_df = dplyr::select(random_points@data, -percent_years_as_grass)

readr::write_csv(soil_df, 'data/processed_soil_data.csv')
