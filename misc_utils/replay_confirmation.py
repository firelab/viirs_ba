import VIIRS_threshold_reflCor_Bulk as vt
import viirs_config as vc 
import datetime
import glob
import sys
import multiprocessing as mp

def delete_confirmed(config) : 
    """wipe out fire events and collections"""
    vt.execute_query(config, 'DELETE FROM "{0}".fire_events'.format(config.DBschema))
    vt.execute_query(config, 'DELETE FROM "{0}".fire_collections'.format(config.DBschema))

def mask_points(config) : 
    """apply landcover mask to active_fire and threshold burned"""
    vt.execute_query(config, "SELECT viirs_mask_points('{0}','active_fire','landcover','noburn','geom')")
    vt.execute_query(config, "SELECT viirs_mask_points('{0}','threshold_burned','landcover','noburn','geom')")
    
# Convert to a datetime object
def image_date_time(imagedate):
    # pattern: 
    # d20140715_t1921193,
    components = imagedate.split("_")
    dt = components[0][1:] + components[1][1:-1]
    dt = datetime.datetime.strptime(dt, '%Y%m%d%H%M%S')
    return dt

def db_date_string(dt) : 
    date_4db = datetime.datetime.strftime(dt, "%Y-%m-%d %H:%M:%S")
    return date_4db

def confirm_date(config, datestring) : 
    """copy points (active fire and confirmed burn points to fire_events table"""
    db_date = db_date_string(image_date_time(datestring))
    print db_date

    vt.execute_active_fire_2_events(config, db_date)
    vt.execute_threshold_2_events(config, db_date)


def reconfirm_run(config) : 
    """re-computes fire_events and fire_collections table for entire run"""
    delete_confirmed(config)
    mask_points(config)
    for d in config.SortedImageDates : 
        confirm_date(config, d)


def reconfirm_batch(base_dir, workers=1) : 
    """re-computes fire_events and fire_collections for every run in this batch"""
    config_list = vc.VIIRSConfig.load_batch(base_dir)
    
    if workers > 1 : 
        mypool = mp.Pool(processes=workers)
        mypool.map(reconfirm_run, config_list)
    else : 
        for c in config_list : 
            reconfirm_run(c)
    

if __name__ == '__main__' : 
    if len(sys.argv) not in [2,3] : 
        print "Usage: {0} base_directory [workers]".format(sys.argv[0])
        sys.exit() 

    if len(sys.argv) == 3 : 
        reconfirm_batch(sys.argv[1],int(sys.argv[2]))
    else : 
        reconfirm_batch(sys.argv[1])
