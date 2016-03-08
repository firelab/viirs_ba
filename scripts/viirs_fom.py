"""Figure of merit code

In general, this module contains components to implement the following flow: 
    + add a column to the fire_events table to store the points projected into
      the NLCD projection.
    + create a fire_events_raster serving as a fire mask
    + perform intersections and unions between the satellite fire mask and 
      the ground truth fire mask.
    + sum all the active fire pixels in the intersection and the union.
    + ratio the sum of the intersection pixels to the sum  of union pixels.
"""

import glob
import os.path
import numpy as np
import pandas as pd
import psycopg2
import VIIRS_threshold_reflCor_Bulk as vt
import viirs_config as vc

def project_fire_events_nlcd(config) :
    """Creates a new geometry column in the fire_events table and project to
    NLCD coordinates."""
    
    # start clean
    query = 'ALTER TABLE "{0}".fire_events DROP COLUMN IF EXISTS geom_nlcd'.format(config.DBschema)
    vt.execute_query(config, query)
    
    # make new column
    query = 'ALTER TABLE "{0}".fire_events ADD COLUMN geom_nlcd geometry(MultiPoint,{1})'.format(config.DBschema,vt.srids["NLCD"])
    vt.execute_query(config, query)
    
    # project existing data into new column
    query = 'UPDATE "{0}".fire_events SET geom_nlcd=ST_Transform(geom,{1})'.format(config.DBschema, vt.srids["NLCD"])
    vt.execute_query(config, query)
                            
def create_fire_events_raster(config, gt_schema, gt_table) : 
    """dumps the ground truth fire mask to disk, overwrites by rasterizing fire_events, 
    reloads the table to postgis
    This ensures that the result is aligned to the specified ground truth 
    table."""
    query = "SELECT viirs_rasterize('{0}', '{1}', '{2}')".format(
          config.DBschema, gt_schema, gt_table)
    vt.execute_query(config, query)
    
def mask_sum(config, gt_schema, gt_table) : 
    """adds the mask values from the fire_events_raster to the values in the ground truth raster.
    
    The fire events raster is in config.DBschema. The ground truth raster to use
    is provided in gt_schema and gt_table. If the supplied masks have only 0 and 1
    in them, as they should, then the sum raster should have only 0, 1, and 2.
    The logical "or" function between the two masks is the set of pixels having a
    nonzero value. The logical "and" function is the set of pixels having the value
    two."""
    query = "SELECT viirs_mask_sum('{0}', '{1}', '{2}')".format(
         config.DBschema, gt_schema, gt_table)
    vt.execute_query(config,query)
    
def calc_ioveru_fom(config) :  
    """calculates the intersection over union figure of merit.
    This function assumes that the mask_sum raster already exists in the 
    database. Returns a floating point number from 0..1"""
    query = "SELECT viirs_calc_fom('{0}')".format(config.DBschema)
    
    # now, need to execute a query that returns a single result.
    ConnParam = vt.postgis_conn_params(config)
    conn = psycopg2.connect(ConnParam)
    # Open a cursor to perform database operations
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()

    conn.commit()
    # Close communication with the database
    cur.close()
    conn.close()
    
    return rows[0][0]
    
    
def do_ioveru_fom(config, gt_schema, gt_table) : 
    """performs the complete process for calculating the intersection over union
    figure of merit."""
    
    project_fire_events_nlcd(config)
    create_fire_events_raster(config, gt_schema, gt_table)
    mask_sum(config, gt_schema, gt_table)
    return calc_ioveru_fom(config)
    
def calc_all_ioveru_fom(run_datafile, gt_schema, gt_table) : 
    """calculates the i over u figure of merit for a batch of previously
    completed runs.
    User supplies the path name of a previously written CSV file which 
    describes a batch of runs. The directory containing this file must
    also contain a bunch of run directories, each of which contains an
    *.ini file for the run."""
    # find where we are
    base_dir = os.path.dirname(run_datafile)
    if base_dir == '' : 
        base_dir = '.'

    runlist = pd.read_csv(run_datafile) 
    fomdata = np.zeros_like(runlist['run_id'],dtype=np.float)

    ini_files = glob.glob('{0}/*/*.ini'.format(base_dir))
    for f in ini_files : 
        config = vc.VIIRSConfig.load(f)
        row = np.where(runlist['run_id'] == config.run_id)

        fomdata[row] = do_ioveru_fom(config, gt_schema, gt_table)
 
    runlist['fom'] = pd.Series(fomdata, index=runlist.index)
    newname = 'new_{0}'.format(os.path.basename(run_datafile))
    runlist.to_csv(os.path.join(base_dir, newname))
