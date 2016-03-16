CREATE OR REPLACE FUNCTION viirs_nlcd_geom(schema text, tbl text, srid int) 
   RETURNS void AS
$BODY$
    BEGIN

    EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.' ||
              quote_ident(tbl) || 
              ' DROP COLUMN IF EXISTS geom_nlcd' ;
    EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.' || 
              quote_ident(tbl) || 
              ' ADD COLUMN geom_nlcd geometry' ;
              
    EXECUTE 'UPDATE ' || quote_ident(schema) || '.' ||
             quote_ident(tbl) || 
             ' SET geom_nlcd = ST_Transform(geom, $1)' USING srid ;
    
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_nlcd_geom(schema text, tbl text, srid int)
  OWNER to postgres ;

