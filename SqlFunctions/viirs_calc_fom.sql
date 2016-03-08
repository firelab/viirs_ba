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
