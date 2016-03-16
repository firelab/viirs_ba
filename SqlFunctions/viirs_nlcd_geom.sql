CREATE OR REPLACE FUNCTION viirs_nlcd_geom(schema text, tbl text) 
   RETURNS void AS
$BODY$
    BEGIN

    EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.' || 
              quote_ident(tbl) || 
              ' DROP COLUMN IF EXISTS geom_nlcd' ;
    EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.' || 
              quote_ident(tbl) || 
              ' ADD COLUMN geom_nlcd geometry'; 
              
    EXECUTE 'UPDATE ' || quote_ident(schema) || '.' ||
             quote_ident(tbl) || 
             ' SET geom_nlcd = ST_Transform(geom, 96630)';
    
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_nlcd_geom(schema text, tbl text)
  OWNER to postgres ;

