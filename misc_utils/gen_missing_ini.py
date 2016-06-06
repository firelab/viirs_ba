"""Generates ini files missing from a viirs_brute run. 
You should know that this is the only thing wrong with the
run before turning this code loose on the directory structure."""

import os
import pandas as pd
import viirs_config as vc
import viirs_brute as vb

def make_int_from_dir(dirname) : 
    return int(dirname[6:])

def find_no_ini(base) : 
    """find all subdirectories having no ini file"""
    no_ini = [ ]
    for p, subdirs, files in os.walk(base) : 
        has_ini = False
        for f in files : 
            has_ini |= f.endswith(".ini")
            if has_ini: break
        if not has_ini : 
            no_ini.append(p)

    no_ini_runs = map(make_int_from_dir, no_ini)
    return no_ini, no_ini_runs


def select_zero_fom(info) : 
    """load the schema info file and extract only the missed FOMs"""
    data = pd.read_csv(info, index_col=0)

    subset = data[ data["fom"]==0 ] 
    return subset

def write_missing_ini(template, spreadsheet) : 
    """Given a template file and spreadsheet file, produce the missing inis"""
    missing = select_zero_fom(spreadsheet)
    t = vc.VIIRSConfig.load(template)

    config_list = vb.csv_vectors(t, missing) 

    for cfg in config_list : 
        cfg.save(os.path.join(cfg.DBschema, "derived.ini"))

    
