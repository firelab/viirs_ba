CREATE OR REPLACE FUNCTION viirs_nlcd_fire_events(schema text, srid int) 
   RETURNS void AS
$BODY$
    DECLARE
        sridtext text := srid::text ;
    BEGIN

    EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events ' ||
              'DROP COLUMN IF EXISTS geom_nlcd' ;
    EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events ' || 
              'ADD COLUMN geom_nlcd geometry(Point, ' || sridtext || ')' ; 
              
    EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events ' ||
             'SET geom_nlcd = ST_Transform(geom, $1)' USING srid ;
    
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_nlcd_fire_events(schema text, srid int)
  OWNER to postgres ;

