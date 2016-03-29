"""Figure of merit code

In general, this module contains components to implement the following flow: 
    + add a column to the fire_events table to store the points projected into
      the NLCD projection.
    + create two fire_events_rasters serving as a fire mask for 750m and 
      375m fire events points.
    + combine the two fire events rasters on a 375m grid.
    + perform intersections and unions between the combined satellite 
      fire mask and the ground truth fire mask.
    + sum all the active fire pixels in the intersection and the union.
    + ratio the sum of the intersection pixels to the sum  of union pixels.

A second figure of merit flow is as follows: 
    + add a column to the fire_events table to store the points projected into
      the NLCD projection.
    + create two fire_events_rasters serving as a fire mask for 750m and 
      375m fire events points.
    + combine the two fire events rasters on a 375m grid.
    + sum all "true" points in the combined fire events mask on a per zone
      basis, storing the results in a table
"""

import os.path
import numpy as np
import pandas as pd
import psycopg2
import multiprocessing as mp
import functools as ft
import VIIRS_threshold_reflCor_Bulk as vt
import viirs_config as vc


def extract_fire_mask(config, gt_schema, gt_table,rast_col='rast',geom_col='geom') : 
    """Creates a geometry column and populates with pixel centers where pixel value=1"""
    query="SELECT viirs_get_mask_pts('{0}', '{1}', '{2}', '{3}', {4})".format(
         gt_schema, gt_table, rast_col, geom_col, vt.srids['NLCD'])
    vt.execute_query(config, query) 

def project_fire_events_nlcd(config) :
    """Creates a new geometry column in the fire_events table and project to
    NLCD coordinates."""
    
    query = "SELECT viirs_nlcd_geom('{0}', 'fire_events', {1})".format(config.DBschema, vt.srids["NLCD"])
    vt.execute_query(config, query)

def create_fire_events_raster(config, tbl, 
                              gt_schema, rast_table, geom_table, 
                              filt_dist=-1) : 
    """dumps the ground truth fire mask to disk, overwrites by rasterizing fire_events, 
    reloads the table to postgis
    This ensures that the result is aligned to the specified ground truth 
    table."""

    query = "SELECT viirs_rasterize_375('{0}', '{1}', '{2}', '{3}', '{4}', {5})".format(
          config.DBschema, tbl, gt_schema, rast_table, geom_table, filt_dist)
    vt.execute_query(config, query)

    query = "SELECT viirs_rasterize_750('{0}', '{1}', '{2}', '{3}', '{4}', {5})".format(
          config.DBschema, tbl, gt_schema, rast_table, geom_table, filt_dist)
    vt.execute_query(config, query)

    query = "SELECT viirs_rasterize_merge('{0}')".format(config.DBschema)
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
    
    
def do_ioveru_fom(gt_schema, gt_table, config) : 
    """performs the complete process for calculating the intersection over union
    figure of merit."""
    
    project_fire_events_nlcd(config)
    create_fire_events_raster(config, 'fire_events',  
                              gt_schema, gt_table, gt_table,
                              filt_dist=config.SpatialProximity)
    mask_sum(config, gt_schema, gt_table)
    return calc_ioveru_fom(config)

def zonetbl_init(zone_schema, zone_tbl, zone_col, config) : 
    """drops and re-creates the table in which results are accumulated"""
    query = "SELECT viirs_zonetbl_init('{0}', '{1}', '{2}', {3})".format(
           zone_schema, zone_tbl, zone_col, vt.srids['NLCD'])
    vt.execute_query(config, query)

def zonetbl_run(zone_schema, zonedef_tbl, zone_tbl, zone_col, config,nearest=False) : 
    """collects fire events raster points onto the zone results table for
    a single run, optionally recalculating the fire events raster
    zone_schema.zonedef_tbl  : names the zone definition table
    zone_col                 : names the zone column in the def table
    zone_schema.zone_tbl     : names the zone results table
    gt_schema.gt_table       : names the ground truth raster (for alignment)
    config                   : connection/run specific information
    
    This function will call one of two stored procedures in the database: 
        * viirs_zonetbl_run; or 
        * viirs_nearest_zonetbl_run
    depending on the value of 'nearest'.
    """
    run_schema = config.DBschema
    
    if nearest : 
        function_name = 'viirs_nearest_zonetbl_run'
    else : 
        function_name = 'viirs_zonetbl_run'

    query="SELECT {5}('{0}','{1}','{2}','{3}','{4}')".format(
        zone_schema, zone_tbl, zonedef_tbl, run_schema, zone_col, function_name)
    vt.execute_query(config, query)

def create_events_view(config,year) : 
    """creates a view of the fire_events table, only showing 2013 data."""
    view_name = 'fire_events_{0}'.format(year)
    query="""CREATE OR REPLACE VIEW "{0}".{1} AS
          SELECT * FROM "{0}".fire_events
          WHERE collection_date BETWEEN '{2}-01-01' AND '{3}-01-01'
          """.format(config.DBschema, view_name,year,year+1)

    vt.execute_query(config, query)
    return view_name

def find_missing_zonetbl_runs(gt_schema, zone_tbl, config) : 
    """locates missing run_ids in the zone_tbl by comparing with the list of schemas"""
    query = """select b.runs from
      (select nspname runs from pg_namespace where nspname LIKE 'Run_%') b
      where b.runs not in (select distinct run_id from "{0}"."{1}")""".format(
                gt_schema, zone_tbl)

    # now, need to execute a query that returns multiple results.
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

    runs = [ i[0] for i in rows ] 

    return runs

def do_one_zonetbl_run(gt_schema, gt_table, 
                       zonedef_tbl, zone_tbl, zone_col, config,
                       year=2013, spatial_filter=False):
    """accumulates fire points from a single run into one or more zone tables.
    The zone definition table, results accumulation table, and column names
    are specified as parallel lists in zonedef_tbls, zone_tbls, zone_cols.
    There are two primary cases where this is run. Either no filtering is 
    desired, or we want to include only those points which are within one 
    SpatialProximity of the provided polygons.
    """

    if spatial_filter : 
        filt_dist = config.SpatialProximity 
        nearest = True
    else: 
        filt_dist = -1.
        nearest = False

    view_name = create_events_view(config, year)
    create_fire_events_raster(config, view_name,
                                gt_schema, gt_table, zonedef_tbl,
                                filt_dist=filt_dist)
    
    # fire_events raster is always the product of the above, no matter
    # which year is selected.
    extract_fire_mask(config, config.DBschema, 'fire_events_raster',
                    geom_col='geom_nlcd')

    zonetbl_run(gt_schema, zonedef_tbl, zone_tbl, zone_col, config, nearest)
    
    
def calc_all_ioveru_fom(run_datafile, gt_schema, gt_table, workers=1) : 
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

    runlist = pd.read_csv(run_datafile, index_col=0) 
    fomdata = np.zeros_like(runlist['run_id'],dtype=np.float)
    config_list = vc.VIIRSConfig.load_batch(base_dir)

    # prep ground truth table, giving each row a multipoint
    # geometry of the centroids of "true" pixels in the mask
    extract_fire_mask(config_list[0], gt_schema, gt_table)

    workerfunc = ft.partial(do_ioveru_fom, gt_schema, gt_table)

    if workers == 1 : 
        fom = map(workerfunc, config_list)

    else : 
        mypool = mp.Pool(processes=workers) 
        fom = mypool.map(workerfunc, config_list)

    for i in range(len(config_list)) : 
        row = np.where(runlist['run_id'] == config_list[i].run_id)
        fomdata[row] = fom[i]
 
    runlist['fom'] = pd.Series(fomdata, index=runlist.index)
    newname = 'new_{0}'.format(os.path.basename(run_datafile))
    runlist.to_csv(os.path.join(base_dir, newname))

def do_all_zonetbl_runs(base_dir, gt_schema, gt_table, 
                       zonedef_tbl='dissolve_eval_zones',
                       zone_tbl='eval_zone_counts', 
                       zone_col='zone',
                       year=2013, 
                       workers=1, only_missing=False,
                       spatial_filter=False) : 
    """accumulates fire event raster points by polygon-defined zones.
    This function can optionally use the rasterized fire events tables 
    created by the do_ioveru_fom() method. It can also recover from an
    interrupted run by only processing the missing runs.
    """
    config_list = vc.VIIRSConfig.load_batch(base_dir)

    if only_missing : 
        runs = find_missing_zonetbl_runs(gt_schema, zone_tbl, config_list[0])
        run_configs = [ cfg for cfg in config_list if cfg.DBschema in runs]
        config_list = run_configs

    else : 
        # prepare the table to accumulate results
        zonetbl_init(gt_schema, zone_tbl, zone_col, config_list[0])

    workerfunc = ft.partial(do_one_zonetbl_run, gt_schema, gt_table,
                      zonedef_tbl,
                      zone_tbl,
                      zone_col,
                      year=year,
                      spatial_filter=spatial_filter)

    if workers == 1 : 
        map(workerfunc, config_list)
    else:  
        mypool = mp.Pool(processes=workers)
        mypool.map(workerfunc,config_list)

