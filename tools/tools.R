
###################################################################
# Prepend the root data folder to all files and folders
# specified. 
load_config = function(){
  config = yaml::yaml.load_file('config.yaml')
  
  hostname = Sys.info()['nodename']
  # Check if we're on a ceres node,
  # which can have many different prefixes.
  if(grepl('scinet', hostname)){
    hostname = 'scinet'
  }
  
  if(hostname %in% names(config$data_folder)){
    data_folder = config$data_folder[hostname][[1]]
  } else {
    data_folder = config$data_folder['default'][[1]]
  }
  
  config$data_folder = data_folder
  
  config_attributes = names(config)
  # Don't prepend the root data_folder
  config_attributes = config_attributes[-which('data_folder' %in% config_attributes)]
  
  for(a in config_attributes){
    is_dir = grepl('folder',a)
    is_file= grepl('file',a)
    if(is_dir | is_file){
      config[[a]] = paste0(data_folder,config[[a]])
    }
    if(is_dir){
      if(!dir.exists(config[[a]])) dir.create(config[[a]])
    }
  }
  return(config)
}
