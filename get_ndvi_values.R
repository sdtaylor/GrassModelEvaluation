library(MODISTools)
library(tidyverse)

random_points = read_csv('data/random_points.csv') %>%
  filter(percent_years_as_grass == 1)

# MODISTools::mt_dates('MOD13Q1', lat=40, lon=-100)
# MODISTools::mt_bands('MOD13Q1')

years_to_get = 2001:2019
bands_to_get = c('250m_16_days_VI_Quality','250m_16_days_NDVI','250m_16_days_composite_day_of_the_year')

for(year in years_to_get){
  year_start = paste0(year,'-01-01')
  year_end   = paste0(year, '-12-31')
  for(b in bands_to_get){
    print(paste(year, b,sep=','))
    ndvi_output = mt_batch_subset(product = 'MOD13Q1',
                                  band = b,
                                  df = random_points,
                                  start=year_start,
                                  end = year_end,
                                  ncores=10)
    output_filename = paste0('./data/ndvi/',year,'_',b,'.csv')
    write_csv(ndvi_output, output_filename)
  }
}