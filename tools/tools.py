import yaml
import json
import pandas as pd
import os
import platform


def make_folder(f):
    if not os.path.exists(f):
        os.makedirs(f)
        
def load_config(data_folder=None):
    with open('config.yaml', 'r') as f:
        config = yaml.load(f)

    if data_folder is None:
        hostname = platform.node()
        
        # Check if we're on a hipergator node,
        # which can have many different prefixes.
        if 'scinet' in hostname:
            hostname = 'scinet'
        
        try:
            data_folder = config['data_folder'][hostname]
        except KeyError:
            data_folder = config['data_folder']['default']

    config['data_folder'] = data_folder
    
    make_folder(data_folder)

    for key, value in config.items():
        is_file = 'file' in key
        is_folder = 'folder' in key
        if (is_file or is_folder) and key != 'data_folder':
            config[key] = data_folder + value
        if (is_folder):
            make_folder(config[key])
    
    return config

# This appends csv's while keeping the header intact
# or creates a new file if it doesn't already exist.
def append_csv(df, filename):
    old_data = pd.read_csv(filename)
    all_old_columns_in_new = old_data.columns.isin(df.columns).all()
    all_new_columns_in_old = df.columns.isin(old_data.columns).all()
    if not all_old_columns_in_new or not all_new_columns_in_old:
        raise RuntimeError('New dataframe columns do not match old dataframe')
    
    appended = old_data.append(df)
    appended.to_csv(filename, index=False)

# Re-write a csv with updated info
def update_csv(df, filename):
    os.remove(filename)
    df.to_csv(filename, index=False)

def write_json(obj, filename, overwrite=False):
    if os.path.exists(filename) and not overwrite:
        raise RuntimeWarning('File {f} exists. User overwrite=True to overwite'.format(f=filename))
    else:
        with open(filename, 'w') as f:
            json.dump(obj, f, indent=4)

def read_json(filename):
    with open(filename, 'r') as f:
        m = json.load(f)
    return m
