"""
Seeds a batch of VIIRS calibration runs. 

This script prepares a batch of VIIRS calibration runs, on-disk and in-database.
A template configuration (INI file) and a csv spreadsheet of values (where 
each row represents the parameters of an individual run) are required. 

A directory is created on disk for each run. If it previously existed, it 
will be deleted along with all of its contents. This directory will contain
an INI file for the run associated with that directory.

A schema will be initialized in the database for each run. This schema will 
contain empty tables with properly initialized keys and indices.

This script will initialize the active_fire and threshold_burned tables with
data from tables having the same name in a specified master schema.

Upon completion, the database and directories are ready for the confirmation code
to run.
"""
import viirs_config as vc
import VIIRS_threshold_reflCor_Bulk as vt
import pandas as pd
import shutil
import os
import os.path
import sys

def init_directories(configs) : 
    """Creates Run_nnnn directories and saves configuration ini in them.
    If the directory already exists, it is deleted and recreated. This 
    function is intended to cause you to start with a clean slate."""
    for cfg in configs : 
        # if directory exists, delete it so we can start from scratch
        if os.access(cfg.ShapePath, os.F_OK) : 
            shutil.rmtree(cfg.ShapePath)

        # create the directory
        os.mkdir(cfg.ShapePath)

        # save config file in the directory
        cfg_fname = os.path.join(cfg.ShapePath, 'config.ini')
        cfg.save(cfg_fname)

def init_schemas(configs) : 
    """Drops (if necessary) and re-creates a schema for each of the
    runs in the configuration list."""
    for cfg in configs : 
        vt.initialize_schema_for_postgis(cfg)
        
def copy_data(configs, master_schema='master') : 
    """Copies the active_fire and threshold_burned tables from the master schema to this one."""
    af_columns = 'fid, latitude, longitude, collection_date, geom, event_fid, pixel_size, band_i_m, masked, geom_nlcd'
    tb_columns = 'fid, latitude, longitude, collection_date, geom, confirmed_burn, pixel_size, band_i_m, masked, geom_nlcd'

    copy_query = 'INSERT INTO {tgt_schema}.{table}({columns}) SELECT {columns} FROM {master_schema}.{table}'

    for cfg in configs : 
        # copy active fire
        vt.execute_query(cfg, copy_query.format(tgt_schema=cfg.DBschema,
              table='active_fire', columns=af_columns, master_schema=master_schema))
        # copy threshold_burned
        vt.execute_query(cfg, copy_query.format(tgt_schema=cfg.DBschema,
              table='threshold_burned', columns=tb_columns, master_schema=master_schema))
              
        
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "Usage: %s <template.ini> <runs.csv>" % sys.argv[0]
        sys.exit()

    # load in the template and create the plan of work
    template_ini = vc.VIIRSConfig.load(sys.argv[1])
    table = pd.read_csv(sys.argv[2])

    # produce an array of configuration objects
    p = vc.VIIRSConfig.batch(template_ini, table)
    
    init_directories(p)
    init_schemas(p)
    copy_data(p)

