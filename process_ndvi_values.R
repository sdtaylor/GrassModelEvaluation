

# take the raw output from MODISTools downloads and process
# to ingest into models.
ndvi_files = list.files('./data/ndvi/', pattern = '*csv', full.names = T)

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

# convert back to -1 - 1 scale
ndvi$ndvi = ndvi$ndvi * 0.0001

ndvi = ndvi %>%
  mutate(year = lubridate::year(date),
         doy = lubridate::yday(date)) %>%
  rename(pixel_id = site)

# TODO: filter for good quality pixels only
# source('modis_qa_stuff.R')
# qa_info = get_mod13_qa(ndvi$qa)

write_csv(ndvi, 'data/processed_ndvi.csv')
