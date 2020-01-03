library(dplyr)
library(tidyr)
library(readr)
library(purrr)

# take the raw output from MODISTools downloads and process
# to ingest into models.
ndvi_files = list.files('./data/ndvi/', pattern = '*csv$', full.names = T)

read_in_ndvi = function(f){
 read_csv(f) %>%
  select(band, site, date=calendar_date, value)
}

ndvi = purrr::map_df(ndvi_files, read_in_ndvi)

# nicer names for the different bands
ndvi = ndvi %>%
  mutate(band = case_when(
    band == "250m_16_days_composite_day_of_the_year" ~ 'actual_doy',
    band == "250m_16_days_NDVI" ~ 'ndvi',
    band == "250m_16_days_VI_Quality" ~ 'qa'
  ))

# conver to 3 columns of ndvi, actual_doy, qa
ndvi = spread(ndvi, band, value)

ndvi = ndvi[!is.na(ndvi$ndvi),]

# convert back to -1 - 1 scale
ndvi$ndvi = ndvi$ndvi * 0.0001

ndvi = ndvi %>%
  mutate(year = lubridate::year(date),
         doy = lubridate::yday(date)) %>%
  rename(pixel_id = site)

# TODO: filter for good quality pixels only
source('modis_qa_stuff.R')
qa_info = ndvi %>%
  select(pixel_id, year, doy, date, qa)

qa_info = qa_info %>%
  bind_cols(get_mod13_qa(qa_info$qa))

#######################
# Keep only observations with the highest quality
pixels_to_keep_vi_filter = qa_info %>%
  filter(vi_quality == 'VI Produced with good quality') %>%
  select(pixel_id, date)

ndvi = ndvi %>%
  filter(interaction(pixel_id, date) %in% with(pixels_to_keep_vi_filter, interaction(pixel_id, date)))

#######################
# Keep only pixels with >= 10 observations in all years
# ie. drop the pixel entirely if it doesn't have this robust timeseries
#  See https://github.com/sdtaylor/GrassModelEvaluation/issues/2
pixels_to_keep_min_sample_filter =  ndvi %>%
  group_by(pixel_id, year) %>%
  summarise(n_dates = n_distinct(date)) %>%
  ungroup() %>%
  group_by(pixel_id) %>%
  summarise(n_years_with_10_dates = sum(n_dates>=10)) %>%
  ungroup() %>%
  filter(n_years_with_10_dates==19)

ndvi = ndvi %>%
  filter(pixel_id %in% pixels_to_keep_min_sample_filter$pixel_id)

#####################
# Scaling to 0-1 range for models, using a global maximum and
# pixel specific minimum. See https://github.com/sdtaylor/GrassModelEvaluation/issues/1
absolute_max_ndvi = max(ndvi$ndvi)

pixel_absolute_low = ndvi %>%
  group_by(pixel_id) %>%
  summarise(pixel_abs_low = min(ndvi)) %>%
  ungroup()

ndvi = ndvi %>%
  left_join(pixel_absolute_low, by='pixel_id') %>%
  mutate(ndvi = (ndvi - pixel_abs_low) / (absolute_max_ndvi - pixel_abs_low)) %>%
  select(-pixel_abs_low, -qa)

write_csv(ndvi, 'data/processed_ndvi.csv')
