library(daymetr)
library(tidyverse)

random_points = read_csv('data/random_points.csv') %>%
  filter(percent_years_as_grass == 1)

get_daymet_data = function(lon,lat,pixel_id){
  print(pixel_id)
  download_output = daymetr::download_daymet(lat = lat, lon = lon,
                                             start = config$daymet_years_start, end = config$daymet_years_end,
                                             silent = T)
  
  df = download_output$data
  colnames(df) <- c('year','doy','daylength','precip','radiation','swe','tmax','tmin','vp')
  df$pixel_id = pixel_id
  
  return(df)
}

daymetr_output = purrr::pmap_dfr(random_points[1:3,c('lon','lat','pixel_id')], get_daymet_data)

write_csv(daymetr_output, './data/daymet_data.csv')
