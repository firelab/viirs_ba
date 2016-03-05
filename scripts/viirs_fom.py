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

import subprocess
import pipes
import os.path
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
    
def get_ogr_pg_connection(config, schema=None, tablename=None) :
    if (schema is not None) and (tablename is not None) :  
        conn = "PG:host={0} dbname={1} user={2} password={3} schema={4} table={5} mode=2".format(
            pipes.quote(config.DBhost),
            pipes.quote(config.DBname),
            pipes.quote(config.DBuser),
            pipes.quote(config.pwd),
            pipes.quote(config.schema),
            pipes.quote(tablename))
    else :
        conn = "PG:host={0} dbname={1} user={2} password={3}".format(
            pipes.quote(config.DBhost),
            pipes.quote(config.DBname),
            pipes.quote(config.DBuser))
    return conn

def get_ogr_layername(schema, table, geom_column='geom') : 
    return '"{0}".{1}({2})'.format(schema, table, geom_column)        
                        
def create_fire_events_raster(config) : 
    """dumps the ground truth fire mask to disk, overwrites by rasterizing fire_events, 
    reloads the table to postgis"""
    
    # on-disk fire events raster file
    fire_tiff = os.path.join(config.ShapePath, 'fire_events_raster.tif')
    
    # dump ground truth to disk
    pg_connection = get_ogr_pg_connection(config, 'gt', 'burnmaskyy')
    command = 'gdal_translate -of GTIFF {0} {1}'.format(
        pipes.quote(pg_connection),
        pipes.quote(fire_tiff))
    print command
    subprocess.call(command)
    
    # rasterize fire events
    pg_connection = get_ogr_pg_connection(config)
    pg_layer      = get_ogr_layername(config.DBschema, 'fire_events', 'geom_nlcd')
    command = 'gdal_rasterize -burn 1 -init 0 -l {0} -tr 375 375 {1} {2}'.format(
       pipes.quote(pg_layer),
       pipes.quote(pg_connection), 
       pipes.quote(fire_tiff))
    print command
    subprocess.call(command)
    
    # reload tiff to postgis raster, in the current schema.
    #
    # make a sql file
    fire_sql_file = os.path.join(config.ShapePath, 'fire_events_raster.sql')
    r2pgsql_exe = os.path.join(config.PostBin, 'raster2pgsql')
    command='{0} -s {1} -t 100x100 {2} -q -I -C -Y {3}.fire_events_raster'.format(
        pipes.quote(r2pgsql_exe),
        vt.srids['NLCD'],
        pipes.quote(fire_tiff),
        config.DBschema)
    fire_sql = open(fire_sql_file, 'w')
    print command
    subprocess.call(command, stdout=fire_sql)
    fire_sql.close()
    
    # upload the sql file
    vt.execute_sql_file(config, fire_sql_file)
    
        
    
    
    
    