--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.0
-- Dumped by pg_dump version 9.5.0

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';


SET search_path = public, pg_catalog;

--
-- Name: copy_activefire_2_fireevents(timestamp without time zone); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION copy_activefire_2_fireevents(timestamp without time zone) RETURNS void
    LANGUAGE plpgsql
    AS $_$
  DECLARE
    collection TIMESTAMP :=$1;
 BEGIN
  INSERT INTO fire_events (latitude, 
                           longitude, 
                           initial_date,
                           geom, 
                           source)
  SELECT latitude,longitude, collection_date, ST_Multi(ST_Transform(geom, 102008)), 'ActiveFire'
  FROM active_fire
  WHERE collection_date = collection;
 END;
$_$;


ALTER FUNCTION public.copy_activefire_2_fireevents(timestamp without time zone) OWNER TO postgres;

--
-- Name: copy_burns_2_confirmed(timestamp without time zone); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION copy_burns_2_confirmed(timestamp without time zone) RETURNS void
    LANGUAGE plpgsql
    AS $_$
  DECLARE
    collection TIMESTAMP :=$1;
 BEGIN
  INSERT INTO confirmed_burned_area (latitude, 
                                     longitude, 
                                     collection_date,
                                     geom)
  SELECT latitude,longitude, collection_date, ST_Transform(geom, 102008)
  FROM preliminary_burned
  WHERE collection_date = collection AND confirmed_burn = TRUE;
 END;
$_$;


ALTER FUNCTION public.copy_burns_2_confirmed(timestamp without time zone) OWNER TO postgres;

--
-- Name: copy_preliminary_burned_2_fireevents(timestamp without time zone); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION copy_preliminary_burned_2_fireevents(timestamp without time zone) RETURNS void
    LANGUAGE plpgsql
    AS $_$
  DECLARE
    collection TIMESTAMP :=$1;
 BEGIN
  INSERT INTO fire_events (latitude, 
                           longitude, 
                           initial_date,
                           geom, 
                           source)
  SELECT latitude,longitude, collection_date, ST_Multi(ST_Transform(geom, 102008)), 'Threshold'
  FROM preliminary_burned
  WHERE collection_date = collection AND confirmed_burn = TRUE;
 END;
$_$;


ALTER FUNCTION public.copy_preliminary_burned_2_fireevents(timestamp without time zone) OWNER TO postgres;

--
-- Name: viirs_activefire_2_fireevents(timestamp without time zone, interval, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION viirs_activefire_2_fireevents(timestamp without time zone, interval, integer) RETURNS void
    LANGUAGE plpgsql
    AS $_$
DECLARE 
  collection timestamp without time zone := $1; 
  recent interval := $2;
  distance integer := $3; 
--   a_row active_fire%rowtype;
  a_row RECORD;
  ret RECORD;
  dumrec RECORD;
  dumint integer;
  query_str text;
  tempFID integer;
BEGIN
  FOR a_row IN SELECT a.* FROM active_fire a  
    WHERE collection_date = collection
  LOOP
  SELECT * from (SELECT fe.fid as fe_fid,
                        fe.geom, 
                        fc.fid as fc_fid
                   FROM fire_events fe, fire_collections fc
                   WHERE fe.collection_id = fc.fid 
                     AND fc.last_update >= collection - recent 
                     AND fc.last_update <= collection
                     AND fc.active = TRUE) as tf 
    WHERE ST_DWithin(ST_Transform(a_row.geom, 102008), tf.geom, distance) LIMIT 1 INTO dumrec;
  IF EXISTS (SELECT * from (SELECT fe.fid as fe_fid,
                        fe.geom, 
                        fc.fid as fc_fid
                   FROM fire_events fe, fire_collections fc
                   WHERE fe.collection_id = fc.fid 
                     AND fc.last_update >= collection - recent 
                     AND fc.last_update <= collection
                     AND fc.active = TRUE) as tf 
    WHERE ST_DWithin(ST_Transform(a_row.geom, 102008), tf.geom, distance) LIMIT 1) THEN 
    RAISE NOTICE 'found a match' ;
    INSERT INTO fire_events(latitude, longitude, geom, source, collection_id, collection_date, pixel_size, band_i_m)
      VALUES(a_row.latitude, a_row.longitude, ST_Multi(ST_Transform(a_row.geom, 102008)), 'ActiveFire', dumrec.fc_fid, a_row.collection_date, a_row.pixel_size, a_row.band_i_m);
    UPDATE fire_collections SET last_update = a_row.collection_date
      WHERE dumrec.fc_fid = fire_collections.fid;
  ELSE
    INSERT INTO fire_collections(initial_date, last_update, active, initial_fid)
      VALUES(a_row.collection_date, a_row.collection_date, TRUE, a_row.fid)
      RETURNING fid as collectionFid INTO tempFID;
    INSERT INTO fire_events(latitude, longitude, geom, source, collection_id, collection_date, pixel_size, band_i_m) 
      VALUES(a_row.latitude, a_row.longitude, ST_Multi(ST_Transform(a_row.geom, 102008)), 'ActiveFire', tempFID, a_row.collection_date, a_row.pixel_size, a_row.band_i_m);    
  END IF;
  END LOOP;
return;
END
$_$;


ALTER FUNCTION public.viirs_activefire_2_fireevents(timestamp without time zone, interval, integer) OWNER TO postgres;

--
-- Name: viirs_check_4_activity(timestamp without time zone, interval); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION viirs_check_4_activity(timestamp without time zone, interval) RETURNS void
    LANGUAGE plpgsql
    AS $_$
DECLARE
  collection timestamp without time zone := $1;
  recent interval := $2;
BEGIN
UPDATE fire_collections SET active = FALSE where  age(collection, last_update) > recent;
END
$_$;


ALTER FUNCTION public.viirs_check_4_activity(timestamp without time zone, interval) OWNER TO postgres;

--
-- Name: viirs_simple_confirm_burns(timestamp without time zone, interval, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION viirs_simple_confirm_burns(timestamp without time zone, interval, integer) RETURNS void
    LANGUAGE plpgsql
    AS $_$
  DECLARE
    collection TIMESTAMP := $1;
    recent_interval INTERVAL := $2;
    distance INTEGER := $3;
BEGIN
UPDATE threshold_burned
SET confirmed_burn = TRUE
FROM(
SELECT a.* FROM public.threshold_burned a 
LEFT JOIN public.active_fire b 
ON ST_DWithin(ST_Transform(a.geom, 102008), ST_Transform(b.geom, 102008), distance)
WHERE a.collection_date = collection 
        AND
        b.collection_date >= collection - recent_interval 
        AND
        b.collection_date <= collection) AS subquery
WHERE threshold_burned.fid = subquery.fid;
END
$_$;


ALTER FUNCTION public.viirs_simple_confirm_burns(timestamp without time zone, interval, integer) OWNER TO postgres;

--
-- Name: viirs_threshold_2_fireevents(timestamp without time zone, interval, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION viirs_threshold_2_fireevents(timestamp without time zone, interval, integer) RETURNS void
    LANGUAGE plpgsql
    AS $_$
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
$_$;


ALTER FUNCTION public.viirs_threshold_2_fireevents(timestamp without time zone, interval, integer) OWNER TO postgres;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: active_fire; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE active_fire (
    fid bigint NOT NULL,
    latitude real,
    longitude real,
    collection_date timestamp without time zone,
    geom geometry(Point,4326),
    event_fid integer,
    pixel_size integer NOT NULL,
    band_i_m character(1) NOT NULL
);


ALTER TABLE active_fire OWNER TO postgres;

--
-- Name: active_fire_fid_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE active_fire_fid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE active_fire_fid_seq OWNER TO postgres;

--
-- Name: active_fire_fid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE active_fire_fid_seq OWNED BY active_fire.fid;


--
-- Name: fire_collections; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE fire_collections (
    fid bigint NOT NULL,
    active boolean,
    initial_fid bigint,
    last_update timestamp without time zone,
    initial_date timestamp without time zone
);


ALTER TABLE fire_collections OWNER TO postgres;

--
-- Name: fire_collections_fid_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE fire_collections_fid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE fire_collections_fid_seq OWNER TO postgres;

--
-- Name: fire_collections_fid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE fire_collections_fid_seq OWNED BY fire_collections.fid;


--
-- Name: fire_events; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE fire_events (
    fid bigint NOT NULL,
    latitude real,
    longitude real,
    geom geometry(MultiPoint,102008),
    source character(10),
    collection_id bigint,
    collection_date timestamp without time zone,
    pixel_size integer NOT NULL,
    band_i_m character(1) NOT NULL
);


ALTER TABLE fire_events OWNER TO postgres;

--
-- Name: fire_events_fid_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE fire_events_fid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE fire_events_fid_seq OWNER TO postgres;

--
-- Name: fire_events_fid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE fire_events_fid_seq OWNED BY fire_events.fid;


--
-- Name: threshold_burned; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE threshold_burned (
    fid bigint NOT NULL,
    latitude real,
    longitude real,
    collection_date timestamp without time zone,
    geom geometry(Point,4326),
    confirmed_burn boolean DEFAULT false,
    pixel_size integer NOT NULL,
    band_i_m character(1) NOT NULL
);


ALTER TABLE threshold_burned OWNER TO postgres;

--
-- Name: threshold_burned_fid_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE threshold_burned_fid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE threshold_burned_fid_seq OWNER TO postgres;

--
-- Name: threshold_burned_fid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE threshold_burned_fid_seq OWNED BY threshold_burned.fid;


--
-- Name: fid; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY active_fire ALTER COLUMN fid SET DEFAULT nextval('active_fire_fid_seq'::regclass);


--
-- Name: fid; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY fire_collections ALTER COLUMN fid SET DEFAULT nextval('fire_collections_fid_seq'::regclass);


--
-- Name: fid; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY fire_events ALTER COLUMN fid SET DEFAULT nextval('fire_events_fid_seq'::regclass);


--
-- Name: fid; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY threshold_burned ALTER COLUMN fid SET DEFAULT nextval('threshold_burned_fid_seq'::regclass);


--
-- Name: active_fire_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY active_fire
    ADD CONSTRAINT active_fire_pkey PRIMARY KEY (fid);


--
-- Name: fire_collections_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY fire_collections
    ADD CONSTRAINT fire_collections_pkey PRIMARY KEY (fid);


--
-- Name: fire_events_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY fire_events
    ADD CONSTRAINT fire_events_pkey PRIMARY KEY (fid);


--
-- Name: threshold_burned_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY threshold_burned
    ADD CONSTRAINT threshold_burned_pkey PRIMARY KEY (fid);


--
-- Name: idx_active_fire_geom; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_active_fire_geom ON active_fire USING gist (geom);


--
-- Name: idx_fire_events_geom; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fire_events_geom ON fire_events USING gist (geom);


--
-- Name: idx_threshold_burned_geom; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_threshold_burned_geom ON threshold_burned USING gist (geom);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

