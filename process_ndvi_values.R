library(dplyr)
library(tidyr)
library(readr)
library(purrr)

source('tools/tools.R')
config = load_config()

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

####################################################
# Apply smoothing weights according to pixel quality
source('modis_qa_stuff.R')

quality_weights = tribble(
  ~vi_quality,                                          ~w,
  'VI Produced with good quality',                       1.0,
  'VI produced, but check other QA',                     0.5,
  'Pixel produced, but most probably cloudy',            0.1,
  'Pixel not produced due to other reasons than clouds', 0.1
  )


qa_info = ndvi %>%
  select(pixel_id, year, doy, date, qa)

qa_info = qa_info %>%
  bind_cols(get_mod13_qa(qa_info$qa)) %>%
  select(pixel_id, date, vi_quality) %>%
  left_join(quality_weights, by='vi_quality')

ndvi = ndvi %>%
  left_join(qa_info, by=c('pixel_id','date'))
################################################

smooth_ndvi = function(df, window_wize = 5, iters=10){
  fit = phenofit::wSG(y = df$ndvi, w = df$w, nptperyear = 23, frame=window_wize, iters=iters)
  df$smoothed_ndvi = fit$zs$ziter10
  return(df)
}

ndvi = ndvi %>%
  group_by(pixel_id) %>%
  do(smooth_ndvi(df=.)) %>%
  ungroup() %>%
  rename(raw_ndvi = ndvi)


# Plotting the smoothed timeseries with original NDVI values and qual flags
# ndvi2 %>%
#   filter(pixel_id %in% c(1674,1034,826,1989)) %>%
# ggplot(aes(x= date)) +
#   geom_line(aes(y=raw_ndvi), color='black') +
#   geom_point(aes(y=raw_ndvi, color=vi_quality), size=3) +
#   geom_line(aes(y=smoothed_ndvi), color='red') +
#   geom_point(aes(y=smoothed_ndvi), color='red') +
#   geom_line(aes(y=smoothed_scaled_ndvi), color='limegreen', size=2) +
#   geom_hline(yintercept = 0, linetype='dotted', size=1) + 
#   scale_x_date(date_breaks = '1 year', limits = lubridate::ymd(c('2003-01-01','2015-12-31'))) +
#   scale_color_brewer(palette = 'Dark2') +
#   theme(panel.grid.minor.x = element_blank()) +
#   facet_wrap(~pixel_id, ncol=1)
####################################################
# Scaling to 0-1 range for models, using a global maximum and
# pixel specific minimum. See https://github.com/sdtaylor/GrassModelEvaluation/issues/1
winter_months = c(12,1,2)

absolute_max_ndvi = max(ndvi$smoothed_ndvi)

pixel_average_winter_low = ndvi %>%
  mutate(month = lubridate::month(date)) %>%
  filter(month %in% winter_months) %>%
  group_by(pixel_id) %>%
  summarise(pixel_avg_low = mean(smoothed_ndvi)) %>%
  ungroup()

ndvi = ndvi %>%
  left_join(pixel_average_winter_low, by='pixel_id') %>%
  mutate(smoothed_scaled_ndvi = (smoothed_ndvi - pixel_avg_low) / (absolute_max_ndvi - pixel_avg_low)) %>%
  mutate(smoothed_scaled_ndvi = ifelse(smoothed_scaled_ndvi <0, 0, smoothed_scaled_ndvi)) %>%
  select(-pixel_avg_low, -qa)

ndvi = ndvi %>%
  filter(pixel_id != 214) # This one has issues

ndvi$ndvi = ndvi$smoothed_scaled_ndvi

write_csv(ndvi, 'data/processed_ndvi.csv')
