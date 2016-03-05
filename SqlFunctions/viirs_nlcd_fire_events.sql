CREATE OR REPLACE FUNCTION viirs_nlcd_fire_events(schema text) 
   RETURNS void AS
$BODY$
    BEGIN

    EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events ' ||
              'DROP COLUMN IF EXISTS geom_nlcd' ;
    EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.fire_events ' || 
              'ADD COLUMN geom_nlcd geometry(Multipoint, 96630)'; 
              
    EXECUTE 'UPDATE ' || quote_ident(schema) || '.fire_events ' ||
             'SET geom_nlcd = ST_Transform(geom, 96630)';
    
    END
$BODY$ 
  LANGUAGE plpgsql VOLATILE
  COST 100 ; 
ALTER FUNCTION viirs_nlcd_fire_events(schema text)
  OWNER to postgres ;

