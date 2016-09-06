#
# Script to run all of the figure of merit code for a single run back to back.
#
import viirs_fom as vf
import sys


def all_fom(database_name, workers=12) : 
    # I over U FOM
    vf.calc_all_ioveru_fom('{}_schema_info.csv'.format(database_name),
                           'gt', 'burnmask13', workers=workers)
                           
    # Zones
    vf.do_all_zonetbl_runs('.','gt','burnmask13',
                           zone_tbl='fixed_zone_counts', 
                           workers=workers,
                           mask_tbl='bobafet13')

    # 2013 events
    vf.do_all_zonetbl_runs('.','gt','burnmask13',
                           zonedef_tbl='calevents_2013',
                           zone_tbl='fixed_events_2013_counts',
                           zone_col='fireid',
                           year=2013,
                           workers=workers,
                           spatial_filter=True,
                           mask_tbl='bobafet13')
    # 2014 events
    vf.do_all_zonetbl_runs('.','gt','burnmask14',
                           zonedef_tbl='calevents_2014',
                           zone_tbl='fixed_events_2014_counts',
                           zone_col='fireid',
                           year=2014,
                           workers=workers,
                           spatial_filter=True,
                           mask_tbl='bobafet14')
#
if __name__ == "__main__" :
    if len(sys.argv) != 2 : 
        print "Usage: {0} database_name".format(sys.argv[0])
        print "Run this from the base directory of a batch-of-runs, and"
        print "provide the database name associated with the entire batch."
        sys.exit()
        
    all_fom(sys.argv[1])
