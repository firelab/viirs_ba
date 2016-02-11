-- Function: viirs_threshold_2_fireevents(timestamp without time zone, interval, integer)

-- DROP FUNCTION viirs_threshold_2_fireevents(timestamp without time zone, interval, integer);

CREATE OR REPLACE FUNCTION viirs_threshold_2_fireevents(
    timestamp without time zone,
    interval,
    integer)
  RETURNS void AS
$BODY$
DECLARE 
  collection timestamp without time zone := $1; 
  recent interval := $2;
  distance integer := $3; 
--   a_row active_fire_alb%rowtype;
  a_row RECORD;
  dumrec RECORD;
BEGIN
  FOR a_row IN SELECT a.* FROM threshold_burned a  
    WHERE collection_date = collection
      AND confirmed_burn = TRUE
  LOOP
  SELECT * from (SELECT fe.fid as fe_fid,
           fe.geom, 
           fc.fid as fc_fid
      FROM fire_events fe, fire_collections fc
      WHERE fe.collection_id = fc.fid 
        AND fc.last_update >= collection - recent 
        AND fc.last_update <= collection
        AND fc.active = TRUE) as tf WHERE ST_DWithin(ST_Transform(a_row.geom, 102008), tf.geom, distance) LIMIT 1 INTO dumrec;
  IF EXISTS (SELECT * from (SELECT fe.fid as fe_fid,
           fe.geom, 
           fc.fid as fc_fid
      FROM fire_events fe, fire_collections fc
      WHERE fe.collection_id = fc.fid 
        AND fc.last_update >= collection - recent 
        AND fc.last_update <= collection
        AND fc.active = TRUE) as tf WHERE ST_DWithin(ST_Transform(a_row.geom, 102008), tf.geom, distance) LIMIT 1) THEN 
    RAISE NOTICE 'found a match' ;
    INSERT INTO fire_events(latitude, longitude, geom, source, collection_id, collection_date, pixel_size, band_i_m)
      VALUES(a_row.latitude, a_row.longitude, ST_Multi(ST_Transform(a_row.geom, 102008)), 'Threshold', dumrec.fc_fid, a_row.collection_date, a_row.pixel_size, a_row.band_i_m);
    UPDATE fire_collections SET last_update = a_row.collection_date
      WHERE dumrec.fc_fid = fire_collections.fid;
  END IF;
  END LOOP;
return;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION viirs_threshold_2_fireevents(timestamp without time zone, interval, integer)
  OWNER TO postgres;
