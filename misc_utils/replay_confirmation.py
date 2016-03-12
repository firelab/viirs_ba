import VIIRS_threshold_reflCor_Bulk as vt
import viirs_config as vc 
import datetime

def delete_confirmed(config) : 
    """wipe out fire events and collections"""
    vt.execute_query(config, 'DELETE FROM "{0}".fire_events'.format(config.DBschema))
    vt.execute_query(config, 'DELETE FROM "{0}".fire_collections'.format(config.DBschema))

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

