

vi_quality       = list('00'='VI Produced with good quality',
                        '01'='VI produced, but check other QA',
                        '10'='Pixel produced, but most probably cloudy',
                        '11'='Pixel not produced due to other reasons than clouds')

vi_usefulness    = list('0000' = 'Highest quality',
                        '0001' = 'Lower quality',
                        '0010' = 'Decreasing quality',
                        '0100' = 'Decreasing quality',
                        '1000' = 'Decreasing quality',
                        '1001' = 'Decreasing quality',
                        '1010' = 'Decreasing quality',
                        '1100' = 'Lowest quality',
                        '1101' = 'Quality so low that is is not useful',
                        '1110' = 'L1B data faulty',
                        '1111' = 'Not useful for any other reason/ not processed')

aerosol_quantity = list('00' = 'Climatology',
                        '01' = 'Low',
                        '10' = 'Intermediate',
                        '11' = 'High')

binary_yes_no    = list('0' = 'No',
                        '1' = 'Yes')
 
adjacent_cloud_detected    = binary_yes_no
atmosphere_brdf_correction = binary_yes_no
mixed_clouds               = binary_yes_no
possible_snow_ice          = binary_yes_no
possible_shadow            = binary_yes_no

land_water_mask =  list('000' = 'Shallow ocean',
                        '001' = 'Land (Nothing else but land)',
                        '010' = 'Ocean coastlines and lake shorelines',
                        '011' = 'Shallow inland water',
                        '100' = 'Ephemeral water',
                        '101' = 'Deep inland water',
                        '110' = 'Moderate or continental ocean',
                        '111' = 'Deep ocean')
####################################
mod13_bit_info  = list('vi_quality'                 = list(bit_locations = c(1:2),   bit_values = vi_quality),
                       'vi_usefulness'              = list(bit_locations = c(3:6),   bit_values = vi_usefulness),
                       'aerosol_quantity'           = list(bit_locations = c(7:8),   bit_values = aerosol_quantity),
                       'adjacent_cloud_detected'    = list(bit_locations = c(9),     bit_values = adjacent_cloud_detected),
                       'atmosphere_brdf_correction' = list(bit_locations = c(10),    bit_values = atmosphere_brdf_correction),  
                       'mixed_clouds'               = list(bit_locations = c(11),    bit_values = mixed_clouds),
                       'land_water_mask'            = list(bit_locations = c(12:14), bit_values = land_water_mask), 
                       'possible_snow_ice'          = list(bit_locations = c(15),    bit_values = possible_snow_ice), 
                       'possible_shadow'            = list(bit_locations = c(16),    bit_values = possible_shadow))  

# flag_info = list with bit_locations and bit_values
# full_qa   = the original integer qa value
# returns the description of the specific bit combination
get_flag_value = function(flag_info, int_qa){
  extracted_bits = paste0(as.numeric(intToBits(int_qa))[flag_info$bit_locations], collapse = '')
  return(flag_info$bit_values[[extracted_bits]])
}

get_mod13_qa = function(qa_values){
  get_single_qa_value = function(qa){
    lapply(mod13_bit_info, FUN = function(f){get_flag_value(flag_info = f, int_qa = qa)})
  }
  purrr::map(qa_values, get_single_qa_value)
}



