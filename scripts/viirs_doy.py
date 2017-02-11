import os.path
import numpy as np
import pandas as pd
import psycopg2
import subprocess as sub
import multiprocessing as mp
import functools as ft
import VIIRS_threshold_reflCor_Bulk as vt
import viirs_config as vc

def create_fire_events_raster(config, tbl, 
                              gt_schema, rast_table, geom_table, 
                              filt_dist=-1,
                              filter_table=None,
                              result_tbl=None) : 
    """dumps the ground truth fire mask to disk, overwrites by rasterizing fire_events, 
    reloads the table to postgis
    This ensures that the result is aligned to the specified ground truth 
    table."""

    query = "SELECT viirs_rasterize_375_mindoy('{0}', '{1}', '{2}', '{3}', '{4}', {5})".format(
          config.DBschema, tbl, gt_schema, rast_table, geom_table, filt_dist)
    vt.execute_query(config, query)

    query = "SELECT viirs_rasterize_750_mindoy('{0}', '{1}', '{2}', '{3}', '{4}', {5})".format(
          config.DBschema, tbl, gt_schema, rast_table, geom_table, filt_dist)
    vt.execute_query(config, query)
    
    if result_tbl is None :
        result_tbl = 'rast_doy'
    query = "SELECT viirs_rasterize_merge_doy('{0}', '{1}')".format(config.DBschema, result_tbl)
    vt.execute_query(config, query)

    query = 'DROP TABLE IF EXISTS "{0}".{1}'.format(config.DBschema, result_tbl)
    vt.execute_query(config, query)

    query = 'SELECT rid, {1} as rast INTO "{0}".{1} FROM "{0}".fire_events_raster'.format(config.DBschema, result_tbl)
    vt.execute_query(config, query)
              
def create_events_view(config,year) : 
    """creates a view of the fire_events table, only showing 2013 data."""
    view_name = 'fire_events_{0}'.format(year)
    query="""CREATE OR REPLACE VIEW "{0}".{1} AS
          SELECT * FROM "{0}".fire_events
          WHERE collection_date BETWEEN '{2}-01-01' AND '{3}-01-01' 
              AND NOT (source = 'ActiveFire' AND pixel_size=750)
          """.format(config.DBschema, view_name,year,year+1)

    vt.execute_query(config, query)
    return view_name

def export_raster(config, raster_tbl, raster_col='rast') : 
    """Dumps the raster table to disk as a tif file."""
    connstr = 'PG:dbname=\'{0}\' user=\'{1}\' password=\'{6}\' host=\'{5}\' schema=\'\\"{2}\\"\' table=\'{3}\' column=\'{4}\' mode=2'.format(config.DBname, config.DBuser, config.DBschema, raster_tbl, raster_col, config.DBhost, config.pwd)

    outfile = os.path.join(config.ShapePath, '{0}.tif'.format(raster_tbl))
    cmd = r'/usr/bin/gdal_translate -ot UInt16 -of GTiff -co "COMPRESS=DEFLATE" {0} "{1}"'.format(connstr, outfile)
    args = [
       '/usr/bin/gdal_translate',
       '-ot', 'UInt16',
       '-of', 'GTiff', 
       '-co', 'COMPRESS=DEFLATE',
       '-co', 'PREDICTOR=2',
       connstr,
       '{0}'.format(outfile) ]
    sub.call(args)
    
    
def rasterize(config) :  
    create_events_view(config,2013)
    create_fire_events_raster(config, 'fire_events_2013', 'gt', 'burnmask13', '', result_tbl='rast_doy_2013')

    create_events_view(config,2014)
    create_fire_events_raster(config, 'fire_events_2014', 'gt', 'burnmask13', '', result_tbl='rast_doy_2014')

    
