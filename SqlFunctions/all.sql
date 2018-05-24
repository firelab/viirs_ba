INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext) values ( 102008, 'ESRI', 102008, '+proj=aea +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs ', 'PROJCS["North_America_Albers_Equal_Area_Conic",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Albers"],PARAMETER["False_Easting",0],PARAMETER["False_Northing",0],PARAMETER["central_meridian",-96],PARAMETER["Standard_Parallel_1",20],PARAMETER["Standard_Parallel_2",60],PARAMETER["latitude_of_origin",40],UNIT["Meter",1]]');
INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext) values ( 96630, 'sr-org', 6630, '+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs ', 'PROJCS["NAD_1983_Albers",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9108"]],AUTHORITY["EPSG","4269"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_center",23],PARAMETER["longitude_of_center",-96],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["meters",1]]');
CREATE OR REPLACE FUNCTION init_schema(name text) 
   RETURNS void AS
$BODY$
    BEGIN

    EXECUTE 'DROP SCHEMA IF EXISTS ' || quote_ident(name) || ' CASCADE' ;
    EXECUTE 'CREATE SCHEMA ' || quote_ident(name) ; 
    
    --
    -- Name: active_fire; Type: TABLE; Schema: public; Owner: postgres
    --
    EXECUTE 'CREATE TABLE ' || quote_ident(name) || '.active_fire (' ||
        'fid bigint NOT NULL, ' ||
        'latitude real, ' || 
        'longitude real, ' || 
        'collection_date timestamp without time zone, ' || 
        'geom geometry(Point,4326), ' || 
        'event_fid integer, ' || 
        'pixel_size integer NOT NULL, ' ||
        'band_i_m character(1) NOT NULL, ' ||
        'masked boolean DEFAULT FALSE, ' || 
        'geom_nlcd geometry)';
    
    
    EXECUTE 'ALTER TABLE ' || quote_ident(name) || '.active_fire OWNER TO postgres';
    
    --
    -- Name: active_fire_fid_seq; Type: SEQUENCE; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE SEQUENCE ' || quote_ident(name) || '.active_fire_fid_seq ' ||
        'START WITH 1 ' ||
        'INCREMENT BY 1 ' || 
        'NO MINVALUE ' || 
        'NO MAXVALUE ' ||
        'CACHE 1';
    
    
    EXECUTE 'ALTER TABLE ' || quote_ident(name) || '.active_fire_fid_seq OWNER TO postgres';
    
    --
    -- Name: active_fire_fid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
    --
    
    EXECUTE 'ALTER SEQUENCE ' || quote_ident(name) || '.active_fire_fid_seq OWNED BY '||
        quote_ident(name) || '.active_fire.fid';
    
    
    --
    -- Name: fire_collections; Type: TABLE; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE TABLE ' || quote_ident(name) || '.fire_collections (' ||
        'fid bigint NOT NULL, ' ||
        'active boolean, ' ||
        'initial_fid bigint, ' ||
        'last_update timestamp without time zone, ' || 
        'initial_date timestamp without time zone)';
    
    
    EXECUTE 'ALTER TABLE ' || quote_ident(name) || '.fire_collections OWNER TO postgres';
    
    --
    -- Name: fire_collections_fid_seq; Type: SEQUENCE; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE SEQUENCE ' || quote_ident(name) || '.fire_collections_fid_seq ' ||
        'START WITH 1 ' ||
        'INCREMENT BY 1 ' ||
        'NO MINVALUE ' ||
        'NO MAXVALUE ' ||
        'CACHE 1';
    
    
    EXECUTE 'ALTER TABLE ' || quote_ident(name) || '.fire_collections_fid_seq OWNER TO postgres';
    
    --
    -- Name: fire_collections_fid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
    --
    
    EXECUTE 'ALTER SEQUENCE ' || quote_ident(name) || '.fire_collections_fid_seq OWNED BY ' ||
         quote_ident(name) || '.fire_collections.fid';
    
    
    --
    -- Name: fire_events; Type: TABLE; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE TABLE ' || quote_ident(name) || '.fire_events (' ||
        'fid bigint NOT NULL, ' || 
        'latitude real, ' || 
        'longitude real, ' || 
        'geom geometry(Point,102008), ' || 
        'source character(10), ' || 
        'collection_id bigint, ' || 
        'collection_date timestamp without time zone, ' ||
        'pixel_size integer NOT NULL, ' ||
        'band_i_m character(1) NOT NULL)';
    
    
    EXECUTE 'ALTER TABLE ' || quote_ident(name) || '.fire_events OWNER TO postgres';
    
    --
    -- Name: fire_events_fid_seq; Type: SEQUENCE; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE SEQUENCE ' || quote_ident(name) || '.fire_events_fid_seq ' || 
        'START WITH 1 ' ||
        'INCREMENT BY 1 ' || 
        'NO MINVALUE ' ||
        'NO MAXVALUE ' ||
        'CACHE 1';
    
    
    EXECUTE 'ALTER TABLE ' || quote_ident(name) || '.fire_events_fid_seq OWNER TO postgres';
    
    --
    -- Name: fire_events_fid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
    --
    
    EXECUTE 'ALTER SEQUENCE ' || quote_ident(name) || '.fire_events_fid_seq OWNED BY ' ||
         quote_ident(name) || '.fire_events.fid';
    
    
    --
    -- Name: threshold_burned; Type: TABLE; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE TABLE ' || quote_ident(name) || '.threshold_burned (' || 
        'fid bigint NOT NULL, ' ||
        'latitude real, ' ||
        'longitude real, ' ||
        'collection_date timestamp without time zone, ' || 
        'geom geometry(Point,4326), ' ||
        'confirmed_burn boolean DEFAULT false, ' ||
        'pixel_size integer NOT NULL, ' ||
        'band_i_m character(1) NOT NULL, ' || 
        'masked boolean DEFAULT FALSE, ' ||
        'geom_nlcd geometry)';
    
    
    EXECUTE 'ALTER TABLE ' || quote_ident(name) || '.threshold_burned OWNER TO postgres';
    
    --
    -- Name: threshold_burned_fid_seq; Type: SEQUENCE; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE SEQUENCE ' || quote_ident(name) || '.threshold_burned_fid_seq ' || 
        'START WITH 1 ' ||
        'INCREMENT BY 1 ' ||
        'NO MINVALUE ' || 
        'NO MAXVALUE ' ||
        'CACHE 1';
    
    
    EXECUTE 'ALTER TABLE ' || quote_ident(name) || '.threshold_burned_fid_seq OWNER TO postgres';
    
    --
    -- Name: threshold_burned_fid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
    --
    
    EXECUTE 'ALTER SEQUENCE ' || quote_ident(name) || '.threshold_burned_fid_seq OWNED BY ' ||
        quote_ident(name) || '.threshold_burned.fid';
    
    
    --
    -- Name: fid; Type: DEFAULT; Schema: public; Owner: postgres
    --
    
    EXECUTE 'ALTER TABLE ONLY ' || quote_ident(name) || 
      '.active_fire ALTER COLUMN fid SET DEFAULT ' ||
      'nextval(' || quote_literal(quote_ident(name) || '.active_fire_fid_seq') || '::regclass)';
    
    
    --
    -- Name: fid; Type: DEFAULT; Schema: public; Owner: postgres
    --
    
    EXECUTE 'ALTER TABLE ONLY ' || quote_ident(name) || 
      '.fire_collections ALTER COLUMN fid SET DEFAULT ' ||
      'nextval(' || quote_literal(quote_ident(name) || '.fire_collections_fid_seq') || '::regclass)';
    
    
    --
    -- Name: fid; Type: DEFAULT; Schema: public; Owner: postgres
    --
    
    EXECUTE 'ALTER TABLE ONLY ' || quote_ident(name) ||
       '.fire_events ALTER COLUMN fid SET DEFAULT ' ||
       'nextval(' || quote_literal(quote_ident(name) || '.fire_events_fid_seq') || '::regclass)';
    
    
    --
    -- Name: fid; Type: DEFAULT; Schema: public; Owner: postgres
    --
    
    EXECUTE 'ALTER TABLE ONLY ' || quote_ident(name) || 
      '.threshold_burned ALTER COLUMN fid SET DEFAULT ' ||
      'nextval(' || quote_literal(quote_ident(name) || '.threshold_burned_fid_seq') || '::regclass)';
    
    
    --
    -- Name: active_fire_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
    --
    
    EXECUTE 'ALTER TABLE ONLY ' || quote_ident(name) || '.active_fire ' || 
        'ADD CONSTRAINT ' || quote_ident(name || '_active_fire_pkey') || ' PRIMARY KEY (fid)';
    
    
    --
    -- Name: fire_collections_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
    --
    
    EXECUTE 'ALTER TABLE ONLY ' || quote_ident(name) || '.fire_collections ' || 
        'ADD CONSTRAINT ' || quote_ident(name || '_fire_collections_pkey') || ' PRIMARY KEY (fid)';
    
    
    --
    -- Name: fire_events_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
    --
    
    EXECUTE 'ALTER TABLE ONLY ' || quote_ident(name) || '.fire_events ' || 
        'ADD CONSTRAINT ' || quote_ident(name || '_fire_events_pkey') || ' PRIMARY KEY (fid)';
    
    
    --
    -- Name: threshold_burned_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
    --
    
    EXECUTE 'ALTER TABLE ONLY ' || quote_ident(name) || '.threshold_burned ' || 
        'ADD CONSTRAINT ' || quote_ident(name || '_threshold_burned_pkey') || ' PRIMARY KEY (fid)';
    
    
    --
    -- Name: idx_active_fire_geom; Type: INDEX; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE INDEX ' || quote_ident('idx_' || name || '_active_fire_geom') || ' ON ' || 
       quote_ident(name) || '.active_fire USING gist (geom)';
    
    EXECUTE 'CREATE INDEX ' || quote_ident('idx_'||name||'_active_fire_geom_nlcd') || 
           ' ON ' || quote_ident(name) || '.active_fire USING GIST (geom_nlcd)' ;
    
    --
    -- Name: idx_fire_events_geom; Type: INDEX; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE INDEX ' || quote_ident('idx_' || name || '_fire_events_geom') || ' ON ' || 
       quote_ident(name) || '.fire_events USING gist (geom)';
    
    
    --
    -- Name: idx_threshold_burned_geom; Type: INDEX; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE INDEX ' || quote_ident('idx_' || name || '_threshold_burned_geom') || ' ON ' || 
       quote_ident(name) || '.threshold_burned USING gist (geom)';
    EXECUTE 'CREATE INDEX ' || quote_ident('idx_'||name||'_threshold_burned_geom_nlcd') || 
            ' ON ' || quote_ident(name) || '.threshold_burned USING GIST (geom_nlcd)' ;

    --
    -- Name: idx_fire_collections_last_update; Type: INDEX; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE INDEX ' || quote_ident('idx_' || name || '_fire_collections_last_update') || ' ON ' || 
       quote_ident(name) || '.fire_collections (last_update DESC NULLS LAST)';

    --
    -- Name: idx_fire_collections_fid; Type: INDEX; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE INDEX ' || quote_ident('idx_' || name || '_fire_collections_fid') || ' ON ' || 
       quote_ident(name) || '.fire_collections (fid)';

    --
    -- Name: idx_fire_events_collection_id; Type: INDEX; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE INDEX ' || quote_ident('idx_' || name || '_fire_events_collection_id') || ' ON ' || 
       quote_ident(name) || '.fire_events (collection_id)';

    --
    -- Name: idx_fire_events_source; Type: INDEX; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE INDEX ' || quote_ident('idx_' || name || '_fire_events_source') || ' ON ' || 
       quote_ident(name) || '.fire_events (source)';

    --
    -- Name: idx_active_fire_collection_date; Type: INDEX; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE INDEX ' || quote_ident('idx_' || name || '_active_fire_collection_date') || ' ON ' || 
       quote_ident(name) || '.active_fire (collection_date)';

    --
    -- Name: idx_threshold_burned_collection_date; Type: INDEX; Schema: public; Owner: postgres
    --
    
    EXECUTE 'CREATE INDEX ' || quote_ident('idx_' || name || '_threshold_burned_collection_date') || ' ON ' || 
       quote_ident(name) || '.threshold_burned (collection_date)';
              
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION init_schema(text)
  OWNER to postgres ;

-- Function: viirs_activefire_2_fireevents(timestamp without time zone, interval, integer)

-- DROP FUNCTION viirs_activefire_2_fireevents(timestamp without time zone, interval, integer);

CREATE OR REPLACE FUNCTION viirs_activefire_2_fireevents(
    varchar(200),
    timestamp without time zone,
    interval,
    integer,
    text DEFAULT NULL,
    text DEFAULT NULL
    )
  RETURNS void AS
$BODY$
DECLARE
  schema varchar(200) = $1; 
  collection timestamp without time zone := $2; 
  recent interval := $3;
  distance integer := $4; 
  lm_schema text := $5 ; 
  lm_table  text:= $6 ;  
--   a_row active_fire%rowtype;
  a_row RECORD;
  ret RECORD;
  dumrec RECORD;
  dumint integer;
  query_str text;
  tempFID integer;
  select_collection text ; 
  append_point_to_collection text ; 
  update_existing_collection text  ;
  create_new_collection text ; 
  insert_first_point text  ;
  loop_query text ;
  
BEGIN
  -- selects currently active collections, to which the fire point should belong.
  select_collection := 'SELECT * from (SELECT fe.fid as fe_fid, ' || 
                       'fe.geom, fc.fid as fc_fid ' || 
                   'FROM ' || quote_ident(schema) || '.fire_events fe, ' || 
                              quote_ident(schema) || '.fire_collections fc ' || 
                   'WHERE fe.collection_id = fc.fid ' ||
                     'AND fc.last_update >= $1 - $2 ' || 
                     'AND fc.last_update <= $1) as tf ' ||  
    'WHERE ST_DWithin(ST_Transform($4, 102008), tf.geom, $3) LIMIT 1' ;
    
  append_point_to_collection := 'INSERT INTO ' || quote_ident(schema) || '.fire_events ' ||
        '(latitude, longitude, geom, source, collection_id, ' ||
        'collection_date, pixel_size, band_i_m) ' || 
        'VALUES($1, $2, ' ||
        'ST_Transform($3, 102008), ' ||
        quote_literal('ActiveFire') ||
        ' , $4, $5, $6, $7)';

  update_existing_collection := 'UPDATE ' || quote_ident(schema) || '.fire_collections ' ||
      'SET last_update = $1 ' || 
      'WHERE $2 = ' || quote_ident(schema) || '.fire_collections.fid';

  create_new_collection := 'INSERT INTO ' || quote_ident(schema) || '.fire_collections '||
     '(initial_date, last_update, active, initial_fid) ' || 
     'VALUES($1, $1, TRUE, $2) ' ||
     'RETURNING fid as collectionFid';
     
  insert_first_point := 'INSERT INTO ' || quote_ident(schema) || '.fire_events ' ||
      '(latitude, longitude, geom, source, collection_id, ' || 
      'collection_date, pixel_size, band_i_m) ' || 
      'VALUES($1, $2, ' ||
      'ST_Transform($3, 102008), ' ||
      quote_literal('ActiveFire') || 
      ', $4, $5, $6, $7)';    


      
  -- loops over all candidate active fire pixels from the specified collection.
  loop_query := 'SELECT a.* FROM ' || quote_ident(schema)||'.active_fire a ' ||
                'WHERE collection_date = $1 AND NOT masked' ; 

  -- masks active fire points in the current collection by the landmask
  IF lm_schema IS NOT NULL THEN
    PERFORM viirs_collection_mask_points(schema, 'active_fire', lm_schema, 
                             lm_table, 'geom', collection) ; 
  END IF ; 
  
  FOR a_row IN EXECUTE loop_query USING collection 
  LOOP
  EXECUTE select_collection INTO dumrec USING collection, recent, distance, a_row.geom ;
  IF dumrec IS NOT NULL THEN 
    RAISE NOTICE 'found a match' ;
    EXECUTE append_point_to_collection USING a_row.latitude, a_row.longitude, a_row.geom, dumrec.fc_fid,
        a_row.collection_date, a_row.pixel_size, a_row.band_i_m ; 
    EXECUTE update_existing_collection USING a_row.collection_date, dumrec.fc_fid ; 
  ELSE
    EXECUTE create_new_collection INTO tempFID USING a_row.collection_date, a_row.fid ; 
    EXECUTE insert_first_point USING a_row.latitude, a_row.longitude, a_row.geom, tempFID,
        a_row.collection_date, a_row.pixel_size, a_row.band_i_m ; 
  END IF;
  END LOOP;
return;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_activefire_2_fireevents(varchar(200), timestamp without time zone, interval, integer, text, text)
  OWNER TO postgres;
CREATE OR REPLACE FUNCTION viirs_calc_fom(schema text) 
   RETURNS float AS
$BODY$
    DECLARE
    totals record ;
    BEGIN
	EXECUTE 'WITH tile_totals AS (' || 
	  'SELECT rid, ST_ValueCount(rast, 0.) zeros, ' ||
		      'ST_ValueCount(rast,1.) ones, ' ||
		      'ST_ValueCount(rast, 2.) twos ' || 
	  'FROM ' || quote_ident(schema) || '.mask_sum ) ' ||  
	 'SELECT SUM(zeros) all_zeros, SUM(ones) all_ones, SUM(twos) all_twos ' ||
	 'FROM tile_totals' INTO totals ;

	return (totals.all_twos::float) / (totals.all_ones + totals.all_twos);
   END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_calc_fom(schema text)
  OWNER to postgres ;
-- Function: viirs_check_4_activity(timestamp without time zone, interval)

-- DROP FUNCTION viirs_check_4_activity(timestamp without time zone, interval);

CREATE OR REPLACE FUNCTION viirs_check_4_activity(
    varchar(200),
    timestamp without time zone,
    interval)
  RETURNS void AS
$BODY$
DECLARE
  schema varchar(200) := $1;
  collection timestamp without time zone := $2;
  recent interval := $3;
BEGIN
EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_collections SET active = FALSE where age($1, last_update) > $2'
   USING collection, recent ;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_check_4_activity(varchar(200), timestamp without time zone, interval)
  OWNER TO postgres;
CREATE OR REPLACE FUNCTION viirs_collection_mask_points(
    varchar(200),
    text,
    text,
    text,
    text,
    timestamp without time zone)
  RETURNS void AS
$BODY$
DECLARE
  schema varchar(200) := $1;
  point_tbl text := $2 ;
  landcover_schema text := $3;
  no_burn_table text := $4 ; 
  no_burn_geom text := $5 ; 
  collection timestamp without time zone := $6 ;
  no_burn_res real ;
  dumint int ;  

BEGIN

  -- determine the srid of the landcover mask for projection
  EXECUTE 'SELECT ST_SRID(rast) FROM ' || 
     quote_ident(landcover_schema)||'.'||quote_ident(no_burn_table)||' nb ' || 
     'LIMIT 1' INTO dumint ; 

  -- reproject and index the points
  PERFORM viirs_collection_nlcd_geom(schema, point_tbl, dumint, collection) ;


  -- determine resolution of "no-burn" mask
  EXECUTE 'SELECT scale_x/2 FROM raster_columns WHERE r_table_schema = ' || 
      quote_literal(landcover_schema) || 
      ' AND r_table_name = ' || quote_literal(no_burn_table) || 
      ' AND r_raster_column = ' || quote_literal('rast') INTO no_burn_res ;

  -- Populate the masked column
  EXECUTE 'UPDATE ' || quote_ident(schema) || '.'||quote_ident(point_tbl)|| ' a '  || 
           'SET masked=TRUE ' ||
           'FROM ' ||quote_ident(landcover_schema)||'.'||quote_ident(no_burn_table)||' nb ' || 
           'WHERE a.geom_nlcd && nb.rast AND ' ||
        'ST_DWithin(a.geom_nlcd, nb.geom, $1) AND ' ||
        'collection_date = $2' 
    USING  no_burn_res, collection ; 
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_collection_mask_points(varchar(200),text,text,text,text,timestamp without time zone)
  OWNER TO postgres;

CREATE OR REPLACE FUNCTION viirs_collection_nlcd_geom(schema text, tbl text, 
                                  srid int, coll timestamp without time zone) 
   RETURNS void AS
$BODY$
    BEGIN
    EXECUTE 'UPDATE ' || quote_ident(schema) || '.' ||
             quote_ident(tbl) || 
             ' SET geom_nlcd = ST_Transform(geom, $1) ' ||
             'WHERE collection_date = $2' USING srid, coll ;
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_collection_nlcd_geom(schema text, tbl text, 
                                   srid int, coll timestamp without time zone)
  OWNER to postgres ;
-- given a schema/table/column containing raster data, creates a 
-- geometry multipoint column contaning pixel centers for only those 
-- points where the pixel value == 1. (mask is true) 
CREATE OR REPLACE FUNCTION viirs_get_mask_pts(schema text, tbl text, rast text, geom text, srid int, mask_val int = 1) 
   RETURNS void AS
$BODY$
  BEGIN

	EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.' ||
	                          quote_ident(tbl) || 
	                          ' DROP COLUMN IF EXISTS ' || quote_ident(geom); 

	EXECUTE 'ALTER TABLE '  || quote_ident(schema) || '.' ||
	                          quote_ident(tbl) || 
	                          ' ADD COLUMN ' || quote_ident(geom) || 
	                          ' Geometry(MultiPoint, ' || srid || ')' ;

	EXECUTE 'WITH ' || 
	  'p as (SELECT rid, ST_Multi(ST_Collect((pixel).geom)) as multi_p ' ||
		'FROM (SELECT rid, ST_PixelAsCentroids(' || quote_ident(rast) ||
		       ') as pixel ' || 
		'FROM ' || quote_ident(schema) || '.' || quote_ident(tbl) || 
		') dummy ' || 
		'WHERE (pixel).val=' || mask_val || ' ' || 
		'GROUP BY rid) ' || 
	  'UPDATE ' || quote_ident(schema) || '.' || quote_ident(tbl) || ' a ' ||
	  'SET ' || quote_ident(geom) || ' = p.multi_p ' || 
	  'FROM p ' || 
	  'WHERE p.rid = a.rid ' ;

        EXECUTE 'CREATE INDEX ' || quote_ident('idx_' || schema || '_' || tbl || '_' || geom) || ' ON ' || 
             quote_ident(schema) || '.' || quote_ident(tbl) || 
             ' USING gist (' || quote_ident(geom) || ')';

   END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_get_mask_pts(schema text, tbl text, rast text, geom text, srid int, mask_val int )
  OWNER to postgres ;
CREATE OR REPLACE FUNCTION viirs_mask_points(
    varchar(200),
    text,
    text,
    text,
    text)
  RETURNS void AS
$BODY$
DECLARE
  schema varchar(200) := $1;
  point_tbl text := $2 ;
  landcover_schema text := $3;
  no_burn_table text := $4 ; 
  no_burn_geom text := $5 ; 
  no_burn_res real ;
  dumint int ;  

BEGIN

  -- delete and recreate masked column
  EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.' ||
           quote_ident(point_tbl) || 
          ' DROP COLUMN IF EXISTS masked ' ;
  EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.' ||
           quote_ident(point_tbl) || 
          ' ADD COLUMN masked boolean DEFAULT FALSE' ;

  -- determine resolution of "no-burn" mask
  EXECUTE 'SELECT scale_x/2 FROM raster_columns WHERE r_table_schema = ' || 
      quote_literal(landcover_schema) || 
      ' AND r_table_name = ' || quote_literal(no_burn_table) || 
      ' AND r_raster_column = ' || quote_literal('rast') INTO no_burn_res ;

  -- determine the srid of the landcover mask for projection
  EXECUTE 'SELECT ST_SRID(rast) FROM ' || 
     quote_ident(landcover_schema)||'.'||quote_ident(no_burn_table)||' nb ' || 
     'LIMIT 1' INTO dumint ; 
        
  -- reproject and index the points
  PERFORM viirs_nlcd_geom(schema, point_tbl, dumint) ;

  -- Populate the masked column
  EXECUTE 'UPDATE ' || quote_ident(schema) || '.'||quote_ident(point_tbl)|| ' a '  || 
           'SET masked=TRUE ' ||
           'FROM ' ||quote_ident(landcover_schema)||'.'||quote_ident(no_burn_table)||' nb ' || 
           'WHERE a.geom_nlcd && nb.rast AND ' ||
        'ST_DWithin(a.geom_nlcd, nb.geom, $1)' 
    USING  no_burn_res ; 
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_mask_points(varchar(200),text,text,text,text)
  OWNER TO postgres;

CREATE OR REPLACE FUNCTION viirs_mask_sum(schema text, gt_schema text, gt_table text) 
   RETURNS void AS
$BODY$
    BEGIN
	EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(schema) || '.mask_sum' ; 

	EXECUTE 'CREATE TABLE ' || quote_ident(schema) || '.mask_sum AS ' || 
	   'SELECT a.rid, ST_MapAlgebra(a.rast, b.rast, ' || 
		 quote_literal('[rast1]+[rast2]') || ', ' || 
		 quote_literal('8BUI') || '::text, ' ||  
		 quote_literal('FIRST') || ', ' || 
		 quote_literal('[rast2]') || ',' || 
		 quote_literal('[rast1]') || ',' || 
		 quote_literal('0') || ') rast ' || 
	   'FROM ' || quote_ident(gt_schema) || '.' || quote_ident(gt_table)|| ' a, ' || 
		   quote_ident(schema)||'.fire_events_raster b ' || 
	   'WHERE a.rid = b.rid ' ;

   
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_mask_sum(schema text, gt_schema text, gt_table text)
  OWNER to postgres ;

CREATE OR REPLACE FUNCTION viirs_nlcd_geom(schema text, tbl text, srid int) 
   RETURNS void AS
$BODY$
    BEGIN

    EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.' ||
              quote_ident(tbl) || 
              ' DROP COLUMN IF EXISTS geom_nlcd CASCADE' ;
    EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.' || 
              quote_ident(tbl) || 
              ' ADD COLUMN geom_nlcd geometry' ;
              
    EXECUTE 'UPDATE ' || quote_ident(schema) || '.' ||
             quote_ident(tbl) || 
             ' SET geom_nlcd = ST_Transform(geom, $1)' USING srid ;

    EXECUTE 'CREATE INDEX ' || quote_ident('idx_'||schema||'_'||tbl||'geom_nlcd') || 
            ' ON ' || quote_ident(schema) || '.' || quote_ident(tbl) ||
            ' USING GIST (geom_nlcd)' ;
    
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_nlcd_geom(schema text, tbl text, srid int)
  OWNER to postgres ;

-- Functions to rasterize a polygon table, given a raster table to 
-- which the result should be aligned, optionally filtering by the
-- geometry objects in a third table. This function produces a
-- new table called "schema".fire_events_raster and populates it 
-- with the result.
-- The operation assumes that the input geometry is a table containing
-- viirs fire events, which may be a mixture of 375m and 750m pixels.
-- The code in this file assumes that rasterization occurs in three phases: 
-- 1] Rasterization of the 375m pixels in a newly created table, aligned to 
--     the specified raster (assumed to be defined at 375m resolution).
-- 2] Rasterization of the 750m pixels in a new column in the above table, where
--     the same row covers the same extent but in two different resolutions.
-- 3] Merging the output of the above two operations by performing a logical OR, 
--     storing the results into a third column.
--
-- "schema"."tbl"           : the geometry table to rasterize.
-- "gt_schema"."rast_table" : the raster table to which the result should be aligned
-- "gt_schema"."geom_table" : the table containing "ground truth" by which the 
--                            input geometry is filtered.
-- distance                 : the maximum distance a candidate geometry may be from 
--                          : a geometry in "gt_schema"."geom_table". Pass -1 to turn
--                          : filtering off.
--
CREATE OR REPLACE FUNCTION viirs_rasterize_375(schema text, tbl text,
                           gt_schema text, 
                           rast_table text, 
                           geom_table text,
                           distance float) 
   RETURNS void AS
$BODY$
    DECLARE 
       dist_clause text ; 
       filter_tbl  text ;
    BEGIN

	EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(schema) || '.fire_events_raster' ;
	
	IF distance <> -1 THEN 
	  dist_clause := 'ST_DWithin(a.geom_nlcd, c.geom, $1) AND ' ||
	                 'ST_Intersects(c.geom, b.rast) AND ' ;
          filter_tbl := ', '||quote_ident(gt_schema)||'.'||quote_ident(geom_table)||' c ' ;
	ELSE
	  dist_clause := ' ' ;
	  filter_tbl := ' ' ;
	END IF ; 

        EXECUTE 'CREATE TABLE ' || quote_ident(schema) || '.fire_events_raster AS ' ||
          'SELECT b.rid, ' ||
	     'ST_MapAlgebra(' ||
	        'ST_Union(ST_AsRaster(geom_nlcd, b.rast, ' || quote_literal('8BUI') ||')), '
		'ST_AddBand(ST_MakeEmptyRaster(b.rast), ' || quote_literal('8BUI') || '::text), ' ||
		quote_literal('[rast1]') || ', ' || 
		quote_literal('8BUI') || ', ' || 
		quote_literal('SECOND') || ') rast_375 ' ||
	  'FROM ' || quote_ident(schema)||'.'||quote_ident(tbl)|| ' a, ' || 
	        quote_ident(gt_schema) || '.' || quote_ident(rast_table) || ' b ' || 
	        filter_tbl || 
	  'WHERE ST_Intersects(a.geom_nlcd, b.rast) AND ' ||
	        dist_clause ||
	        'pixel_size = 375 ' ||  
	  'GROUP BY b.rid, b.rast' USING distance;  

	EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' ||
	    'SET rast_375 = ST_SetBandNoDataValue(rast_375, 3.)' ;
	      
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize_375(schema text, tbl text, gt_schema text, 
                          rast_table text, geom_table text, distance float)
  OWNER to postgres ;

CREATE OR REPLACE FUNCTION viirs_rasterize_750(schema text, tbl text,
                           gt_schema text, 
                           rast_table text, 
                           geom_table text, 
                           distance float) 
   RETURNS void AS
$BODY$
    DECLARE 
        dist_clause text ; 
        filter_tbl text ;
    BEGIN

	EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
	        'DROP COLUMN IF EXISTS rast_750'  ; 

	EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
	        'ADD COLUMN rast_750 raster'  ; 

    DISCARD TEMP ;
    CREATE TEMPORARY TABLE newrasters (rid integer, rast_750 raster) ; 
    
    IF distance <> -1 THEN 
        dist_clause := 'ST_DWithin(a.geom_nlcd, c.geom, $1) AND ' ||
                       'ST_Intersects(c.geom, b.rast) AND ' ;
        filter_tbl := quote_ident(gt_schema)||'.'||quote_ident(geom_table)||' c, ' ;
    ELSE
        dist_clause := ' ' ;
        filter_tbl := ' ' ;
    END IF ; 

    EXECUTE  'INSERT INTO newrasters ' || 
      'SELECT b.rid, ST_MapAlgebra(' ||
		    'ST_Union(ST_AsRaster(a.geom_nlcd, empty_rast_750.rast, '|| 
                         quote_literal('8BUI') || ')), empty_rast_750.rast, ' ||
		    quote_literal('[rast1]') || ', ' || 
		    quote_literal('8BUI') || ', ' || 
		    quote_literal('SECOND') || ') as rast_750 ' ||
      'FROM ' || quote_ident(schema)||'.'||quote_ident(tbl)||' a, ' || 
           quote_ident(gt_schema) || '.' || quote_ident(rast_table) || ' b, ' ||
           filter_tbl || 
           '(SELECT rid, ' || 
	           'St_SetSRID(ST_AddBand(ST_MakeEmptyRaster(ST_Width(rast)/2, ' ||
		                                  'ST_Height(rast)/2, ' ||
		                                  'ST_UpperLeftX(rast), ' ||
		                                  'ST_UpperLeftY(rast), 750), ' ||
		             quote_literal('8BUI')||'::text), ST_SRID(rast)) as rast ' ||
            'FROM ' || quote_ident(gt_schema)||'.'||quote_ident(rast_table)||') empty_rast_750 ' ||
      'WHERE ST_Intersects(a.geom_nlcd, b.rast) AND ' ||
	            dist_clause ||
	            'b.rid = empty_rast_750.rid AND ' ||
	            'pixel_size = 750  ' ||
      'GROUP BY b.rid, empty_rast_750.rast ' USING distance;
    
    EXECUTE 'LOCK TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
          'IN EXCLUSIVE MODE' ; 
    
    EXECUTE 'UPDATE  ' || quote_ident(schema) || '.fire_events_raster me '
      'SET rast_750 = newrasters.rast_750 ' ||
      'FROM newrasters ' ||
      'WHERE newrasters.rid = me.rid' ; 

    EXECUTE 'INSERT INTO ' || quote_ident(schema) || '.fire_events_raster ' ||
       '(rid, rast_750) ' || 
       'SELECT newrasters.rid, newrasters.rast_750 ' || 
       'FROM newrasters ' || 
       'LEFT OUTER JOIN ' || quote_ident(schema) || '.fire_events_raster me ' ||
          'ON (newrasters.rid = me.rid) ' || 
       'WHERE me.rid IS NULL' ;

    EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' ||
	    'SET rast_750 = ST_SetBandNoDataValue(rast_750, 3.) ' ||
            'WHERE rast_750 IS NOT NULL' ;

    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize_750(schema text, tbl text, 
               gt_schema text, rast_table text, geom_table text, distance float)
  OWNER to postgres ;

CREATE OR REPLACE FUNCTION viirs_rasterize_merge(schema text, col text) 
   RETURNS void AS
$BODY$
   BEGIN
   
   EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
          'DROP COLUMN IF EXISTS ' || quote_ident(col) ;

   EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' || 
          'ADD COLUMN ' || quote_ident(col) || ' raster' ;

   EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster '||
          'SET ' || quote_ident(col) || '=rast_375 ' ||
          'WHERE rast_375 IS NOT NULL and rast_750 IS NULL'; 

   EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' || 
          'SET ' || quote_ident(col) || '=ST_Rescale(rast_750, 375., -375) '  ||
          'WHERE rast_375 IS NULL and rast_750 IS NOT NULL' ; 

   EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' || 
          'SET ' || quote_ident(col) || '=ST_SetBandNoDataValue(' ||
             'ST_MapAlgebra(rast_375, ST_Rescale(rast_750,375.,-375.), ' ||
                     quote_literal('(([rast1]=1) OR ([rast2]=1))::int') ||', '|| 
                     quote_literal('8BUI') ||','||
                     quote_literal('FIRST') || '), 3.) ' || 
           'WHERE rast_375 IS NOT NULL and rast_750 IS NOT NULL' ;     
   END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize_merge(schema text, col text)
  OWNER to postgres ;

--
-- viirs_rasterize_filter masks the merged raster. The mask to apply to
-- the raster is specified by the mask_schema and mask_tbl parameters.
-- The data to mask is specified by rast_schema and rast_col (the table 
-- name "fire_events_raster" is assumed.)
--
-- Masked data is put in the "rast" column.
-- 

CREATE OR REPLACE FUNCTION viirs_rasterize_filter(
                                rast_schema text, rast_col text,
                                mask_schema text, mask_tbl text) 
   RETURNS void AS
$BODY$
   BEGIN
   
   EXECUTE 'ALTER TABLE ' || quote_ident(rast_schema) || '.fire_events_raster ' ||
          'DROP COLUMN IF EXISTS rast'  ;

   EXECUTE 'ALTER TABLE ' || quote_ident(rast_schema) || '.fire_events_raster ' || 
          'ADD COLUMN rast raster' ;
          
   EXECUTE 'WITH mask AS (' || 
       'SELECT a.rid, ST_Union(' ||
           'ST_MapAlgebra(a.' || quote_ident(rast_col) || ',b.rast,' || 
              quote_literal('([rast1]=1 AND [rast2]=1)::int') || '),' || 
              quote_literal('MAX') || ') as rast ' || 
        'FROM ' || quote_ident(rast_schema) || 
                  '.fire_events_raster a, ' || 
                  quote_ident(mask_schema) || '.' || 
                  quote_ident(mask_tbl) || ' b ' || 
        'WHERE ST_Contains(a.' || quote_ident(rast_col)|| ',b.rast) ' || 
        'GROUP BY a.rid) ' || 
     'UPDATE ' || quote_ident(rast_schema)||'.fire_events_raster a ' ||
     'SET rast = mask.rast ' || 
     'FROM mask ' || 
     'WHERE a.rid=mask.rid' ;

   END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize_filter(schema text, col text, 
                                   gt_schema text, mask_tbl text)
OWNER to postgres ;



CREATE OR REPLACE FUNCTION viirs_rasterize_375_mindoy(schema text, tbl text,
                           gt_schema text, 
                           rast_table text, 
                           geom_table text,
                           distance float) 
   RETURNS void AS
$BODY$
    DECLARE 
       dist_clause text ; 
       filter_tbl  text ;
    BEGIN

	EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(schema) || '.fire_events_raster' ;
	
	IF distance <> -1 THEN 
	  dist_clause := 'ST_DWithin(a.geom_nlcd, c.geom, $1) AND ' ||
	                 'ST_Intersects(c.geom, b.rast) AND ' ;
          filter_tbl := ', '||quote_ident(gt_schema)||'.'||quote_ident(geom_table)||' c ' ;
	ELSE
	  dist_clause := ' ' ;
	  filter_tbl := ' ' ;
	END IF ; 

        EXECUTE 'CREATE TABLE ' || quote_ident(schema) || '.fire_events_raster AS ' ||
          'SELECT b.rid, ' ||
	     'ST_MapAlgebra(' ||
	        'ST_Union(ST_AsRaster(geom_nlcd, b.rast, ' || quote_literal('16BUI') || ', ' ||
				'EXTRACT(DOY FROM a.collection_date), 367), ' ||
		                quote_literal('MIN') || '), ' ||
		'ST_AddBand(ST_MakeEmptyRaster(b.rast), ' || quote_literal('16BUI') || '::text), ' ||
		quote_literal('[rast1]') || ', ' || 
		quote_literal('16BUI') || ', ' || 
		quote_literal('SECOND') || ', ' ||
                quote_literal('367')    || ', ' || 
                quote_literal('[rast1]')    || ', ' || 
                quote_literal('367')    || ') rast_375_doy ' ||
	  'FROM ' || quote_ident(schema)||'.'||quote_ident(tbl)|| ' a, ' || 
	        quote_ident(gt_schema) || '.' || quote_ident(rast_table) || ' b ' || 
	        filter_tbl || 
	  'WHERE ST_Intersects(a.geom_nlcd, b.rast) AND ' ||
	        dist_clause ||
	        'pixel_size = 375 ' ||  
	  'GROUP BY b.rid, b.rast' USING distance;  

	EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' ||
	   'SET rast_375_doy = ST_SetBandNoDataValue(rast_375_doy, 367.)' ;
	      
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize_375_mindoy(schema text, tbl text, gt_schema text, 
                          rast_table text, geom_table text, distance float)
  OWNER to postgres ;

CREATE OR REPLACE FUNCTION viirs_rasterize_750_mindoy(schema text, tbl text,
                           gt_schema text, 
                           rast_table text, 
                           geom_table text, 
                           distance float) 
   RETURNS void AS
$BODY$
    DECLARE 
        dist_clause text ; 
        filter_tbl text ;
    BEGIN

	EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
	        'DROP COLUMN IF EXISTS rast_750_doy'  ; 

	EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
	        'ADD COLUMN rast_750_doy raster'  ; 

    DISCARD TEMP ;
    CREATE TEMPORARY TABLE newrasters (rid integer, rast_750_doy raster) ; 
    
    IF distance <> -1 THEN 
        dist_clause := 'ST_DWithin(a.geom_nlcd, c.geom, $1) AND ' ||
                       'ST_Intersects(c.geom, b.rast) AND ' ;
        filter_tbl := quote_ident(gt_schema)||'.'||quote_ident(geom_table)||' c, ' ;
    ELSE
        dist_clause := ' ' ;
        filter_tbl := ' ' ;
    END IF ; 

    EXECUTE  'INSERT INTO newrasters ' || 
      'SELECT b.rid, ST_MapAlgebra(' ||
		    'ST_Union(ST_AsRaster(a.geom_nlcd, empty_rast_750.rast, '|| 
                         quote_literal('16BUI') || ', ' ||
                         'EXTRACT(DOY FROM a.collection_date), 367), ' ||
		         quote_literal('MIN') || '), empty_rast_750.rast, ' ||
		    quote_literal('[rast1]') || ', ' || 
		    quote_literal('16BUI') || ', ' || 
		    quote_literal('SECOND') || ', ' ||
                    quote_literal('367')    || ', ' || 
                    quote_literal('[rast1]')    || ', ' || 
                    quote_literal('367')    || ') rast_750_doy ' ||
      'FROM ' || quote_ident(schema)||'.'||quote_ident(tbl)||' a, ' || 
           quote_ident(gt_schema) || '.' || quote_ident(rast_table) || ' b, ' ||
           filter_tbl || 
           '(SELECT rid, ' || 
	           'St_SetSRID(ST_AddBand(ST_MakeEmptyRaster(ST_Width(rast)/2, ' ||
		                                  'ST_Height(rast)/2, ' ||
		                                  'ST_UpperLeftX(rast), ' ||
		                                  'ST_UpperLeftY(rast), 750), ' ||
		             quote_literal('8BUI')||'::text), ST_SRID(rast)) as rast ' ||
            'FROM ' || quote_ident(gt_schema)||'.'||quote_ident(rast_table)||') empty_rast_750 ' ||
      'WHERE ST_Intersects(a.geom_nlcd, b.rast) AND ' ||
	            dist_clause ||
	            'b.rid = empty_rast_750.rid AND ' ||
	            'pixel_size = 750  ' ||
      'GROUP BY b.rid, empty_rast_750.rast ' USING distance;
    
    EXECUTE 'LOCK TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
          'IN EXCLUSIVE MODE' ; 
    
    EXECUTE 'UPDATE  ' || quote_ident(schema) || '.fire_events_raster me '
      'SET rast_750_doy = newrasters.rast_750_doy ' ||
      'FROM newrasters ' ||
      'WHERE newrasters.rid = me.rid' ; 

    EXECUTE 'INSERT INTO ' || quote_ident(schema) || '.fire_events_raster ' ||
       '(rid, rast_750_doy) ' || 
       'SELECT newrasters.rid, newrasters.rast_750_doy ' || 
       'FROM newrasters ' || 
       'LEFT OUTER JOIN ' || quote_ident(schema) || '.fire_events_raster me ' ||
          'ON (newrasters.rid = me.rid) ' || 
       'WHERE me.rid IS NULL' ;

    EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' ||
	    'SET rast_750_doy = ST_SetBandNoDataValue(rast_750_doy, 367.) ' ||
            'WHERE rast_750_doy IS NOT NULL' ;

    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize_750_mindoy(schema text, tbl text, 
               gt_schema text, rast_table text, geom_table text, distance float)
  OWNER to postgres ;

CREATE OR REPLACE FUNCTION viirs_rasterize_merge_doy(schema text, col text) 
   RETURNS void AS
$BODY$
   BEGIN
   
   EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' ||
          'DROP COLUMN IF EXISTS ' || quote_ident(col) ;

   EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events_raster ' || 
          'ADD COLUMN ' || quote_ident(col) || ' raster' ;

   EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster '||
          'SET ' || quote_ident(col) || '=rast_375_doy ' ||
          'WHERE rast_375_doy IS NOT NULL and rast_750_doy IS NULL'; 

   EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' || 
          'SET ' || quote_ident(col) || '=ST_Rescale(rast_750_doy, 375., -375) '  ||
          'WHERE rast_375_doy IS NULL and rast_750_doy IS NOT NULL' ; 

   EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events_raster ' || 
          'SET ' || quote_ident(col) || '=ST_SetBandNoDataValue(' ||
             'ST_MapAlgebra(rast_375_doy, ST_Rescale(rast_750_doy,375.,-375.), ' ||
                     quote_literal('least([rast1],[rast2])') ||', '|| 
                     quote_literal('16BUI') ||','||
                     quote_literal('FIRST') || ', ' || 
                     quote_literal('[rast2]') || ', ' ||
                     quote_literal('[rast1]') || ', ' ||
                     quote_literal('367') || '), 367.0) ' || 
           'WHERE rast_375_doy IS NOT NULL and rast_750_doy IS NOT NULL' ;     
   END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_rasterize_merge_doy(schema text, col text)
  OWNER to postgres ;

-- Function: public.viirs_simple_confirm_burns(timestamp without time zone, interval, integer)

-- DROP FUNCTION public.viirs_simple_confirm_burns(timestamp without time zone, interval, integer);

CREATE OR REPLACE FUNCTION public.viirs_simple_confirm_burns(
    varchar(200),
    timestamp without time zone,
    interval,
    integer)
  RETURNS void AS
$BODY$
  DECLARE
    schema varchar(200) := $1;
    collection TIMESTAMP := $2;
    recent_interval INTERVAL := $3;
    distance INTEGER := $4;
    query text ; 
BEGIN
    -- subquery returns every row from threshold burned which meets the 
    -- temporal criteria, whether it meets the spatial criteria or not.
    -- every row returned by the subquery has its confirmed flag set 
    -- to true
    -- this has been identified as a bottleneck. One potential reason is 
    -- that the active fire and threshold_burned tables become very large and
    -- there is no index on the collection_date field on either one.
    query := 'UPDATE ' || quote_ident(schema) || '.threshold_burned ' ||
        'SET confirmed_burn = TRUE ' || 
        'FROM(' ||
            'SELECT a.* FROM ' || quote_ident(schema) || '.threshold_burned a ' ||
            'LEFT JOIN ' || quote_ident(schema) || '.active_fire b ' || 
            'ON ST_DWithin(ST_Transform(a.geom, 102008), ST_Transform(b.geom, 102008), $1)' ||
        'WHERE a.collection_date = $2 ' || 
            'AND b.collection_date >= $2 - $3 ' || 
            'AND b.collection_date <= $2) AS subquery ' || 
        'WHERE threshold_burned.fid = subquery.fid';
    EXECUTE query USING distance, collection, recent_interval ;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION public.viirs_simple_confirm_burns(varchar(200), timestamp without time zone, interval, integer)
  OWNER TO postgres;
-- Function: viirs_threshold_2_fireevents(timestamp without time zone, interval, integer)

-- DROP FUNCTION viirs_threshold_2_fireevents(timestamp without time zone, interval, integer);

CREATE OR REPLACE FUNCTION viirs_threshold_2_fireevents(
    varchar(200),
    timestamp without time zone,
    interval,
    integer,
    text DEFAULT NULL,
    text DEFAULT NULL)
  RETURNS void AS
$BODY$
DECLARE 
  schema varchar(200) := $1 ;
  collection timestamp without time zone := $2; 
  recent interval := $3;
  distance integer := $4; 
  lm_schema text := $5 ; 
  lm_table text := $6 ;
  added record ; 
  confirm_query text ; 
  confirm_point text ; 
  insert_confirmed text ; 
  update_collection text;
BEGIN

  RAISE NOTICE 'Interval = %', recent ;
  
  -- This will return one row for each confirmed "threshold_burned" point in the 
  -- specified collection, paired with exactly one fire collection via exactly one 
  -- fire event with a source of "ActiveFire" meeting the spatiotemporal criteria. 
  confirm_query := 'SELECT t_fid, fe_fid, fc.fid as fc_fid ' || 
    'FROM ' || quote_ident(schema) || '.fire_collections fc, ' ||
               quote_ident(schema) || '.fire_events fe, ' || 
        '(SELECT t.fid as t_fid, MAX(fe.fid) AS fe_fid ' || 
         'FROM ' || quote_ident(schema) || '.fire_events fe, ' || 
             quote_ident(schema) || '.fire_collections fc, ' ||
             quote_ident(schema) || '.threshold_burned t ' ||
         'WHERE ' ||
             -- glue and seed criteria
             'fe.collection_id = fc.fid AND ' || 
             'fe.source = ' || quote_literal('ActiveFire') || ' AND ' || 
             't.collection_date = $1 AND ' || 

             -- temporal criterion
             'fc.last_update >= $1 - $2 AND ' ||
             'fc.last_update <= $1 AND ' || 

             -- spatial criterion
             'ST_DWithin(ST_Transform(t.geom, 102008), fe.geom, $3) AND ' || 
             
             -- mask out nonburnable
             '(NOT masked) ' ||

        'GROUP BY t.fid) confirmed ' ||
     'WHERE fe.fid = fe_fid AND ' ||
        'fe.collection_id = fc.fid' ;



    
  insert_confirmed := 'INSERT INTO ' || quote_ident(schema) || '.fire_events ' ||
      '(latitude, longitude, geom, source, collection_id, ' ||
       'collection_date, pixel_size, band_i_m) ' ||
      'SELECT latitude, longitude, ST_Transform(geom, 102008), ' || 
        quote_literal('Threshold') || ', ' || 
        'fc_fid, collection_date, pixel_size, band_i_m ' ||
      'FROM confirmed_pts cp, ' || 
            quote_ident(schema) || '.threshold_burned t ' ||
      'WHERE t.fid = cp.t_fid'  ;

  confirm_point := 'UPDATE ' || quote_ident(schema) || '.threshold_burned t ' || 
      'SET confirmed_burn = TRUE ' || 
      'FROM confirmed_pts cp ' || 
      'WHERE t.fid = cp.t_fid' ; 

  -- mask threshold points by burn mask
  IF lm_schema IS NOT NULL THEN 
    PERFORM viirs_collection_mask_points(schema, 'threshold_burned', lm_schema,
               lm_table, 'geom', collection) ; 
  END IF ;

  EXECUTE 'CREATE TEMPORARY TABLE confirmed_pts AS ' || confirm_query
      USING collection, recent, distance ; 
      
  EXECUTE 'SELECT count(*) as c FROM confirmed_pts' INTO added ; 

  RAISE NOTICE 'adding % points.', added.c ; 
  EXECUTE insert_confirmed ; 
  EXECUTE confirm_point ;     
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_threshold_2_fireevents(varchar(200), timestamp without time zone, interval, integer, text, text)
  OWNER TO postgres;
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
