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
        'band_i_m character(1) NOT NULL)';
    
    
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
        'band_i_m character(1) NOT NULL)';
    
    
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

