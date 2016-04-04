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

