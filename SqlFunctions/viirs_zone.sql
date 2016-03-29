CREATE OR REPLACE FUNCTION viirs_zonetbl_init(schema text, tbl text, col text,
     srid int)
   RETURNS void AS
$BODY$
    BEGIN

    EXECUTE 'DROP TABLE IF EXISTS ' || 
           quote_ident(schema) ||'.'||quote_ident(tbl) ; 

    EXECUTE 'CREATE TABLE ' || 
           quote_ident(schema) || '.' || quote_ident(tbl) || 
           ' (geom geometry(Multipoint, ' ||
            srid::text || '), ' || 
           quote_ident(col) || ' int, run_id text, ' ||
           'cells375 int, area_km real)' ; 
           
    END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100 ;
ALTER FUNCTION viirs_zonetbl_init(schema text, tbl text, col text, srid int)
  OWNER to postgres ;

-- viirs_zonetbl_run() aggregates rasterized fire_events from a 
--   single run spatially, using polygons defined in a zone 
--   definition table. It requires that the table in which 
--   results are accumulated already exist (i.e., call viirs_zonetbl_init).
--   While the terminology used here is "zone", any table having a 
--   Polygon geometry column and a unique id column will do. This 
--   code considers each row in such a table to be a zone. This variant 
--   pairs points to zones based on a strict intersection.
-- zone_schema.zonedef_tbl defines the reference polygon set
-- zone_schema.zone_tbl    is where the results from all the runs are 
--                         accumulated
-- run_schema.fire_events_raster must exist and must have a geom column
--
-- The zone_col parameter is the name of the unique identifier column.
CREATE OR REPLACE FUNCTION viirs_zonetbl_run(zone_schema text, zone_tbl text, 
        zonedef_tbl text, run_schema text, zone_col text)
   RETURNS void AS
$BODY$
    BEGIN

    -- Carve out only the points which intersect the reference polygons
    EXECUTE 'CREATE TEMPORARY TABLE intersections ON COMMIT DROP AS ' ||
            'SELECT ST_Multi(ST_Intersection(a.geom,b.geom_nlcd)) as geom, '||
                   'a.' || quote_ident(zone_col)  || ' ' ||
            'FROM ' || quote_ident(zone_schema)||'.'||quote_ident(zonedef_tbl)||' a,'||
                    quote_ident(run_schema)||'.fire_events_raster b ' || 
            'WHERE ST_Intersects(a.geom, b.geom_nlcd)' ; 

    -- Group the intersecting points by zone and insert
    -- into the master table.
    EXECUTE 'INSERT INTO ' || 
            quote_ident(zone_schema) ||'.'|| quote_ident(zone_tbl) || 
            ' (geom, '||quote_ident(zone_col)||', run_id, cells375, area_km) '||
            'SELECT ST_Multi(ST_Collect(f.geom)), ' || 
                   'f.zone, ' || quote_literal(run_schema) || ', ' ||
                   'SUM(ST_NPoints(f.geom)), ' ||
                   'SUM(ST_NPoints(f.geom))*0.140625 ' ||
            'FROM (SELECT ' || quote_ident(zone_col) ||
                    ' as zone, (ST_Dump(geom)).geom as geom ' ||
                   'FROM intersections) as f ' || 
            'GROUP BY f.zone' ;

    END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100 ;
ALTER FUNCTION viirs_zonetbl_run(text, text, text, text, text)
  OWNER to postgres ;
  
-- viirs_nearest_zonetbl_run() aggregates rasterized fire_events from a 
--   single run spatially, using polygons defined in a zone 
--   definition table. It requires that the table in which 
--   results are accumulated already exist (i.e., call viirs_zonetbl_init).
--   While the terminology used here is "zone", any table having a 
--   Polygon geometry column and a unique id column will do. This 
--   code considers each row in such a table to be a zone.
--   This variant of the code pairs every rasterized fire_event to a 
--   unique zone, based on nearness (i.e., points outside the polygon may
--   be paired with the polygon.) No distance threshold is specified here.
--   If a maximum distance is desired, it should have been implemented earlier,
--   during the rasterization process (or in general, the production of the geom
--   column on fire_events_raster.)
-- zone_schema.zonedef_tbl defines the reference polygon set
-- zone_schema.zone_tbl    is where the results from all the runs are 
--                         accumulated
-- run_schema.fire_events_raster must exist and must have a geom column
--
-- The zone_col parameter is the name of the unique identifier column.
CREATE OR REPLACE FUNCTION viirs_nearest_zonetbl_run(zone_schema text, zone_tbl text, 
        zonedef_tbl text, run_schema text, zone_col text)
   RETURNS void AS
$BODY$
    BEGIN
    
    CREATE TEMPORARY SEQUENCE rast_pt_seq  ;

    -- Each row in the raster table is an entire 100x100 raster tile,
    -- need to bust out each individual point.
    EXECUTE 'CREATE TEMPORARY TABLE rast_points ON COMMIT DROP AS ' ||
      'SELECT nextval('||quote_literal('rast_pt_seq')||') gid, rid, ' || 
         '(ST_DumpPoints(geom_nlcd)).geom as geom ' || 
      'FROM ' || quote_ident(run_schema)||'.fire_events_raster' ;

    CREATE INDEX rast_points_idx ON rast_points USING GIST (geom) ;

    -- For each "True" pixel in the raster mask, locate one and only one
    -- fire with which it is to be associated. Use the two-stage
    -- hybrid query suggested on the PostGIS <-> operator doc page,
    -- first to get the nearest 10 zones based on centroid distance,
    -- then to compute the actual distance based on boundary and get
    -- the single nearest zone.
    EXECUTE 'CREATE TEMPORARY TABLE fire_assignment ON COMMIT DROP AS ' ||
      'SELECT a.gid, ' || 
         '(WITH index_query AS (' ||
           'SELECT b.'||quote_ident(zone_col)||
               ', ST_Distance(a.geom,b.geom) d ' ||
           'FROM ' ||
            quote_ident(zone_schema) ||'.'||quote_ident(zonedef_tbl)||' b '||
           'ORDER BY a.geom <-> b.geom LIMIT 10) '||
         'SELECT '||quote_ident(zone_col)||' FROM index_query ' ||
         'ORDER BY d LIMIT 1) closest_zone ' ||
      'FROM rast_points a';
      
    -- Group the intersecting points by zone and insert
    -- into the master table.
    EXECUTE 'INSERT INTO ' || 
            quote_ident(zone_schema) ||'.'|| quote_ident(zone_tbl) || 
            ' (geom, '||quote_ident(zone_col)||', run_id, cells375, area_km) '||
            'SELECT ST_Multi(ST_Collect(rp.geom)), ' || 
                   'f.closest_zone, ' || quote_literal(run_schema) || ', ' ||
                   'SUM(ST_NPoints(rp.geom)), ' ||
                   'SUM(ST_NPoints(rp.geom))*0.140625 ' ||
            'FROM fire_assignment f, rast_points rp ' || 
            'WHERE rp.gid=f.gid ' ||
            'GROUP BY f.closest_zone' ;
    END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100 ;
ALTER FUNCTION viirs_nearest_zonetbl_run(text, text, text, text, text)
  OWNER to postgres ;
