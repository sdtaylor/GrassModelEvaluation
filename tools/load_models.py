import uuid
import os
import json
import datetime

from GrasslandModels.utils import load_saved_model

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

def append_json(obj, filename):
    j = read_json(filename)
    j.append(obj)
    write_json(j, filename=filename, overwrite=True)

def make_folder(f):
    if not os.path.exists(f):
        os.makedirs(f)

def new_hash():
    return uuid.uuid4().hex[:16]

"""
model_info = {'model':model_object,
              'filename_prefix':'modelname'}

model_set = {'models':[model_info1, model_info1],
             'model_filenames':[1,2,3],
             'fit_datetime':'2020-02-01',
             'model_set_id':'asdfasdf097',
             'fit_notes': 'blah blah blah'}

csv
model_set_id,fit_datetime,model_filenames
"""

def load_model_set(model_set_id, folder='./fitted_models/', model_metdata_filename = 'model_sets.json'):
    model_set_metadata = read_json(folder + model_metdata_filename)
    selected_set = [ms for ms in model_set_metadata if ms['model_set_id']==model_set_id]

    if len(selected_set) > 1:
        raise RuntimeError('More than 1 match found for model set hash: ' + model_set_id)
    elif len(selected_set) == 0:
        raise RuntimeError('Model set hash not found: ' + model_set_id)
    else:
        selected_set = selected_set[0]
    
    # Load the models
    saved_model_file_folder = folder + selected_set['model_set_folder']
    selected_set['models'] = [load_save_model(saved_model_file_folder + f) for f in selected_set['model_filenames']]
    
    
def save_model_set(model_set, folder='./fitted_models/', model_metdata_filename = 'model_sets.json'): 
    # Save the parameterized models
    save_folder = folder+model_set['model_set_folder']
    make_folder(save_folder)
    for m, f in zip(model_set['models'], model_set['model_filenames']):
        m.save_params(save_folder + f)
    
    # Save the rest of the model set info
    _ = model_set.pop('models')
    
    append_json(model_set,folder + model_metdata_filename)

def make_model_set(model_list, note='', model_set_id=None):
    """
    from a list of fit models, generate a model set for saving
    """
    if model_set_id is None:
        model_set_id = new_hash()
    now = datetime.datetime.today().strftime('%Y-%m-%d %X')
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    
    
    model_names = [m._get_model_info()['model_name'] for m in model_list]
    model_filenames = [name+'-'+model_set_id+'.json' for name in model_names]
    
    # put the model info into the model list
    model_set = {'models': model_list,
                 'model_names': model_names,
                 'model_filenames': model_filenames,
                 'model_set_folder' : today +'_'+ model_set_id+'/',
                 'fit_datetime': now,
                 'model_set_id': model_set_id,
                 'note': note}
    
    return model_set