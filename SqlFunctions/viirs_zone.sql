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
--   code considers each row in such a table to be a zone.
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
