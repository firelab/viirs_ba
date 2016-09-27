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
