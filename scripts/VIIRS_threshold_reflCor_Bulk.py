#-------------------------------------------------------------------------------
# Name:         VIIRS_threshold.py
# Purpose:      Ingests VIIRS level 2 hdf5 data sets, thresholds for burned 
#               area.
#
# Inputs:       The script is set up to read an ini file. The .ini file
#               is provided as a command line argument. For example:
#               "c:\VIIRS_threshold_reflCor_Bulk.py VIIRS_threshold_bulk.ini"     
#               The ini file contains the base path to input data, threshold
#               values, temporal and spatial proximity thresholds, and output 
#               flags.
#               The script expects that all imagery is in one directory, (e.g.,
#               e:\VIIRS_calibration_data). A list of of the datasets to be 
#               processed is read from the ini file from the entry "ImageDates"
#               where each dataset is identified by its date/time in the format
#               "dYYYYMMDD_tHHMMSSS" (e.g., d20130503_t2104190). The script will
#               loop through this list using each entry to identify the and 
#               process the data files listed below.
#
#               Required files and sub datasets are:
#                  - SVM07_*.h5, 'All_Data/VIIRS-M7-SDR_All/Reflectance'
#                  - SVM08_*.h5, 'All_Data/VIIRS-M8-SDR_All/Reflectance'
#                  - SVM10_*.h5, 'All_Data/VIIRS-M10-SDR_All/Reflectance'
#                  - SVM11_*.h5, 'All_Data/VIIRS-M11-SDR_All/Reflectance'
#                  - GMTCO_*.h5, 'All_Data/VIIRS-MOD-GEO-TC_All/Latitude'
#                  - GITCO_*.H5, 'All_Data/VIIRS-IMG-GEO-TC_All/Latitude'
#                  - GMTCO_*.h5, 'All_Data/VIIRS-MOD-GEO-TC_All/Latitude'
#                  - AVAFO_*.h5, 'All_Data/VIIRS-AF-EDR_All/fireMask'
#                  - VF375_*.hdf, 'fire mask' !!Note this is an hdf4!!
#   
# Outputs:      - text file of burned area pixel locations
#               - text file of VIIRS active fire pixel locations
#               - pushes both datasets to PostGIS database using psycopg
#                   -the database parameters are read from the ini
#
# Comments:     The h5 (and hdf4) data is read in as a numpy array for each band. A 
#               conditional array "con" of zeros is established. For each pixel 
#               in the  band arrays that meets the thresholds the con array is 
#               set to 1. The lat and lon arrays are multiplied by the con 
#               array to get the lat and lon for each pixel that meets the 
#               thresholds. 
#                
#               The thresholding is only applied to pixels that are classified 
#               as "5" or non-fire in the AVAFO product.
#
#               Night pixels are suppressed using the MaxSolZen paramater in the ini file.
#               Night and day avtive fire detections are included.    
#
#
#
# Notable dependencies: numpy, h5py, pyhdf, six, psycog2
#
# Author:      Carl Albury
#
# Created:     20150615
# Modified:    20160210
#-------------------------------------------------------------------------------


import os
import sys
import shutil
import datetime
import glob
import pipes
import numpy as np
import h5py
import psycopg2
import time
import gc
import subprocess
import viirs_config as vc
from pyhdf.SD import SD, SDC



# Prints the contents of an h5.
def print_name(name):
    print name


# Returns the memory block address of an array.
def id(x):
    return x.__array_interface__['data'][0]


# Write the coordinate list to text file.
def write_coordinates2text(config, coordsList, fileName, date):
    if not os.path.exists(os.path.join(config.BaseDir,"TextOut")):
        os.makedirs(os.path.join(config.BaseDir,"TextOut"))
    outfile = os.path.join(config.BaseDir, "TextOut", fileName + ".txt")
    if os.path.exists(outfile):
        os.remove(outfile)
    
    format = '%Y-%m-%d %H:%M:%S'
    if os.path.exists(outfile):
        os.remove(outfile)
    print "\nWriting:", fileName
    with open(outfile, "w") as w:
        w.write("Lat, Lon, DateTime\n")
        for i in coordsList:
            w.write("%s,%s,%s\n" %(i[0], i[1], datetime.datetime.strftime(date, format)))


# This array2list is the old way I did it first. Hanging on to this code just in case.
#def array2list(array):
#    outlist = []
#    for latLon in array:
#        if latLon[0] != 0 and latLon[1] != 0:
#            outlist.append(list(latLon))
#    return outlist


# Write all the non-zero values to a list
def array2list(array):
    outlist = []
    tup = np.nonzero(array)
    for i in tup[0][0::2]: #only need every second value in the tuple[0]
#        print list(array[i])
        outlist.append(list(array[i]))
    return outlist


# Get coordinates of the pixels that satisfy the thresholding
def get_coords_from_Con_array(Con, Lat, Lon):
    # Multiply location arrays by conditional to get points
    LatCon = Lat * Con
    LonCon = Lon * Con
    # Clean up arrays
    Con = None
    Lat = None
    Lon = None
    del Con
    del Lat
    del Lon
    # Flatten with ravel
    LatCon = LatCon.ravel()
    LonCon = LonCon.ravel()
    LatLons = np.array([LatCon, LonCon])
    # Clean up arrays
    LatCon = None
    LonCon = None
    del LatCon
    del LonCon
    LatLons = np.rot90(LatLons)
    return LatLons


# Extract the date/time of the acquisition
def h5_date_time(f):
    dt = f.split("_")[2][1:9] + f.split("_")[3][1:7]
    dt = datetime.datetime.strptime(dt, '%Y%m%d%H%M%S')
    print datetime.datetime.strftime(dt, '%Y%m%d%H%M%S')
    print "\n"
    return dt

def postgis_conn_params(config) : 
    if config.DBhost is None : 
        ConnParam = "dbname={0} user={1} password={2}".format(
           config.DBname, config.DBuser, config.pwd)
    else : 
        ConnParam = "host={3} dbname={0} user={1} password={2}".format(
           config.DBname, config.DBuser, config.pwd, config.DBhost)
    return ConnParam


# Push the coordinates and date/time of the thresholded pixels to PostGIS
def push_list_to_postgis(config, list, date, table, pSize, band):
    print "\nPushing data to {0} DB table: {1}.{2}".format(config.DBname, config.DBschema,table)
    format = '%Y-%m-%d %H:%M:%S'
    # Connect to VIIRS database
    ConnParam = postgis_conn_params(config)
    conn = psycopg2.connect(ConnParam)
    # Open a cursor to perform database operations
    cur = conn.cursor()
    #Loop through list
    for i in list:
        # Execute a command to insert new records into table
        cur.execute("INSERT INTO \"%s\".%s (latitude, longitude, collection_date, geom, pixel_size, band_i_m) VALUES ('%s','%s','%s', ST_GeomFromText('POINT(%s %s)',4326),'%s','%s');"%(config.DBschema, table, i[0], i[1], datetime.datetime.strftime(date, format), i[1], i[0],pSize,band))
        # Make the changes to the database persistent
        conn.commit()
    old_isolation_level = conn.isolation_level
    conn.set_isolation_level(0)
    cur.execute("VACUUM ANALYZE \"%s\".%s;"%(config.DBschema, table)) 
    conn.set_isolation_level(old_isolation_level)
    conn.commit()
    # Close communication with the database
    cur.close()
    conn.close()
def execute_query(config, queryText):
    print "Start", queryText, get_time()
    ConnParam = postgis_conn_params(config)
    conn = psycopg2.connect(ConnParam)
    # Open a cursor to perform database operations
    cur = conn.cursor()
    cur.execute(queryText)
    conn.commit()
    # Close communication with the database
    cur.close()
    conn.close()
    print "End", queryText, get_time()
 
def execute_check_4_activity(config, collectionDate):
    query_text = "SELECT viirs_check_4_activity('{0}', '{1}', '{2}');".format(config.DBschema, collectionDate, config.TemporalProximity)
    execute_query(config,query_text)
 
def execute_active_fire_2_events(config, collectionDate):
    print "Start active_fire to fire_events", get_time()
    query_text = "SELECT VIIRS_activefire_2_fireevents('{0}', '{1}', '{2}', {3});".format(config.DBschema, collectionDate, config.TemporalProximity, config.SpatialProximity)
    execute_query(config,query_text)

def execute_threshold_2_events(config, collectionDate):
    print "Start simple confirm burn", get_time()
    query_text = "SELECT VIIRS_threshold_2_fireevents('{0}', '{1}', '{2}', {3});".format(config.DBschema, collectionDate, config.TemporalProximity, config.SpatialProximity)
    execute_query(config,query_text)

def execute_simple_confirm_burns(config, collectionDate):
    print "Start threshold_burned to fire_events", get_time()
    query_text = "SELECT VIIRS_simple_confirm_burns('{0}', '{1}', '{2}', {3});".format(config.DBschema, collectionDate, config.TemporalProximity, config.SpatialProximity)
    execute_query(config,query_text)

 
# def execute_copy_threshold_burned_2_fire_events(collectionDate):
    # print "Copying confimed burned to fire events for:", collectionDate
    # query_text = "SELECT copy_threshold_burned_2_fireevents(\'{0}\');".format(collectionDate)
    # execute_query(config, query_text)
    
# def execute_copy_active_fire_2_fire_events(collectionDate):
    # print "Copying active fire to fire events for:", collectionDate
    # query_text = "SELECT copy_activefire_2_fireevents(\'{0}\');".format(collectionDate)
    # execute_query(config,query_text)
    
def vacuum_analyze(config, table):
    print "Start Vacuum {0}.{1}".format(config.DBschema, table), get_time()
    query_text = "VACUUM ANALYZE \"{0}\".{1}".format(config.DBschema, table) 
    # Connect to VIIRS database
    ConnParam = postgis_conn_params(config)
    conn = psycopg2.connect(ConnParam)
    old_isolation_level = conn.isolation_level
    conn.set_isolation_level(0)
    # Open a cursor to perform database operations
    cur = conn.cursor()
    cur.execute(query_text)
    conn.commit()
    conn.set_isolation_level(old_isolation_level)
    # Close communication with the database
    cur.close()
    conn.close()
    print "End Vacuum {0}".format(table), get_time(), "\n"

def initialize_schema_for_postgis(config) : 
    """create the schema to hold outputs, populate with empty tables"""
    query_text = "SELECT init_schema('{0}')".format(config.DBschema)
    execute_query(config,query_text)
    

def get_time():
    ts = time.time()
    dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return dt
    
    

def run(config):
    
    if config.DatabaseOut == "y":
        initialize_schema_for_postgis(config)

    #Loop through BaseDir, look for h5s and load arrays
    count = 0
    start_group = datetime.datetime.now()
    for ImageDate in config.ImageDates:
        start_indiviudal = datetime.datetime.now()
        count  = count + 1
        print "Processing number:", count, "of:", len(config.ImageDates)
        print ImageDate + '\n'
        #Read band 7
        h5 = glob.glob(os.path.join(config.BaseDir, "SVM07_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading band 07:", os.path.basename(h5)
        
        M07Hdf = h5py.File(h5, "r")
        M07ReflArray = M07Hdf['All_Data/VIIRS-M7-SDR_All/Reflectance'][:]
        M07ReflFact = M07Hdf['All_Data/VIIRS-M7-SDR_All/ReflectanceFactors'][:]

        # Read band 8
        h5 = glob.glob(os.path.join(config.BaseDir, "SVM08_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading band 08:", os.path.basename(h5)
        M08Hdf = h5py.File(h5, "r")
        M08ReflArray = M08Hdf['All_Data/VIIRS-M8-SDR_All/Reflectance'][:]
        M08ReflFact = M08Hdf['All_Data/VIIRS-M8-SDR_All/ReflectanceFactors'][:]

        # Read band 10
        h5 = glob.glob(os.path.join(config.BaseDir, "SVM10_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading band 10:", os.path.basename(h5)
        M10Hdf = h5py.File(h5, "r")
        M10ReflArray = M10Hdf['All_Data/VIIRS-M10-SDR_All/Reflectance'][:]
        M10ReflFact = M10Hdf['All_Data/VIIRS-M10-SDR_All/ReflectanceFactors'][:]

        # Read band 11
        h5 = glob.glob(os.path.join(config.BaseDir, "SVM11_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading band 11:", os.path.basename(h5)
        M11Hdf = h5py.File(h5, "r")
        M11ReflArray = M11Hdf['All_Data/VIIRS-M11-SDR_All/Reflectance'][:]
        M11ReflFact = M11Hdf['All_Data/VIIRS-M11-SDR_All/ReflectanceFactors'][:]

        # Read GMTCO
        h5 = glob.glob(os.path.join(config.BaseDir, "GMTCO_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading GMTCO Latitude and Longitude:", os.path.basename(h5)
        GeoHdf = h5py.File(h5,"r")
        LatArray = GeoHdf['All_Data/VIIRS-MOD-GEO-TC_All/Latitude'][:]
        LonArray = GeoHdf['All_Data/VIIRS-MOD-GEO-TC_All/Longitude'][:]
        print "Reading GMTCO Solar Zenith:", os.path.basename(h5)
        SolZen750 = GeoHdf['All_Data/VIIRS-MOD-GEO-TC_All/SolarZenithAngle'][:]
        
        if config.use375af.lower() == "y":
            # Read GITCO
            h5 = glob.glob(os.path.join(config.BaseDir, "GITCO_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
            print "Reading GITCO Latitude and Longitude:", os.path.basename(h5)
            GeoHdf = h5py.File(h5,"r")
            Lat375Array = GeoHdf['All_Data/VIIRS-IMG-GEO-TC_All/Latitude'][:]
            Lon375Array = GeoHdf['All_Data/VIIRS-IMG-GEO-TC_All/Longitude'][:]
            print "Reading GITCO Solar Zenith:", os.path.basename(h5)
            #SolZen375 = GeoHdf['All_Data/VIIRS-IMG-GEO-TC_All/SolarZenithAngle'][:]
            
            # Read VF375
            h4 = glob.glob(os.path.join(config.BaseDir, "VF375_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.hdf"))[0]
            print "Reading VF375:", os.path.basename(h5)
            AF375hdf = SD(h4, SDC.READ)
            AF375_fm = AF375hdf.select('fire mask')
            Af375Array = AF375_fm[:]
            Af375DateTime = h5_date_time(os.path.basename(h4))
        
        # Read AVAFO
        h5 = glob.glob(os.path.join(config.BaseDir, "AVAFO_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading AFAFO:", os.path.basename(h5)
        AfHdf = h5py.File(h5, "r")
        AfArray = AfHdf['All_Data/VIIRS-AF-EDR_All/fireMask'][:]
        AfDateTime = h5_date_time(os.path.basename(h5))
                # AfArray codes (these apply to I and M bands )):
                # 0 = missing input data
                # 1 = not processed (obsolete)
                # 2 = not processed (obsolete)
                # 3 = water
                # 4 = cloud
                # 5 = non-fire
                # 6 = unknown
                # 7 = fire (low confidence)
                # 8 = fire (nominal confidence)
                # 9 = fire (high confidence)
                # FILL VALUES: NA_UINT8_FILL = 255
                # MISS_UINT8_FILL = 254
                # ONBOARD_PT_UINT8_FILL = 253
                # ONGROUND_PT_UINT8_FILL = 252
                # ERR_UINT8_FILL = 251
                # ELLIPSOID_UINT8_FILL = 250
                # VDNE_UINT8_FILL = 249
                # SOUB_UINT8_FILL = 248
    
    
        H5Date = h5_date_time(os.path.basename(h5))
        H5DateStr = datetime.datetime.strftime(H5Date, "%Y%m%d_%H%M%S")
        # Print h5 contents
        ##GeoHdf.visit(print_name)
        ##M08Hdf.visit(print_name)
        
        # Correct reflectance values by applying scale factors
        M07ReflArray = M07ReflArray*M07ReflFact[0] + M07ReflFact[1]
        M08ReflArray = M08ReflArray*M08ReflFact[0] + M08ReflFact[1]
        M10ReflArray = M10ReflArray*M10ReflFact[0] + M10ReflFact[1]
        M11ReflArray = M11ReflArray*M11ReflFact[0] + M11ReflFact[1]
        
        # Set up Burned Area Conditional array: BaCon
        print "Thresholding"
        BaCon = np.zeros_like(M07ReflArray)
        
        # Threshold bands
        # Cast any pixels as one that meet the thresholds.
        # I'm getting a divide by zero warning here that I am catching with
        # the "np.where" but I'm still getting it, however I'm pretty confident that
        # the catch is working properly.
        BaCon[
            (M07ReflArray < config.M07UB) &
            (M08ReflArray > config.M08LB) &
            (M08ReflArray < config.M08UB) &
            (M10ReflArray > config.M10LB) &
            (M10ReflArray < config.M10UB) &
            (M11ReflArray > config.M11LB) &
            (np.where(M11ReflArray != 0,((M08ReflArray-config.RthSub)/M11ReflArray),config.RthLB-1) >= config.RthLB) &
            (np.where(M11ReflArray != 0,((M08ReflArray-config.RthSub)/M11ReflArray),config.Rth+1) < config.Rth) &
            (AfArray == 5) &
            (SolZen750 < config.MaxSolZen) &   #this should supress nigth pixels
            ((M07ReflArray - M07ReflFact[1])/M07ReflFact[0] < 65528) &
            ((M08ReflArray - M08ReflFact[1])/M08ReflFact[0] < 65528) &
            ((M10ReflArray - M10ReflFact[1])/M10ReflFact[0] < 65528) &
            ((M11ReflArray - M11ReflFact[1])/M11ReflFact[0] < 65528)
            ] = 1

        # Apply geographic window, if specified
        if config.has_window() : 
            BaCon[
                (LatArray > config.north) | 
                (LatArray < config.south) | 
                (LonArray < config.west) | 
                (LonArray > config.east)
            ] = 0
                
    
        # Clean up arrays
        M07ReflArray = None
        M08ReflArray = None
        M10ReflArray = None
        M11ReflArray = None
        M07ReflFact = None
        M08ReflFact = None
        M10ReflFact = None
        M11ReflFact = None
        SolZen750 = None
        del M07ReflArray
        del M08ReflArray
        del M10ReflArray
        del M11ReflArray
        del M07ReflFact
        del M08ReflFact
        del M10ReflFact
        del M11ReflFact
        del SolZen750
            
        
        # Get Burned area coordinates as an array
        BaLatLons = get_coords_from_Con_array(BaCon, LatArray, LonArray)
        # Clean up arrays
        BaCon = None
        del BaCon
        
        # Convert Burned area coordinates array to a list
        BaOut_list = array2list(BaLatLons)
        # Clean up arrays
        BaLatLons = None
        del BaLatLons
        
        # Burned area output to text
        if config.TextOut == "y":
            write_coordinates2text(config, BaOut_list, "BaOut_" + H5DateStr, H5Date)

        # Burned area output to PostGIS 
        if config.DatabaseOut == "y":
            push_list_to_postgis(config, BaOut_list, H5Date, "threshold_burned", "750", "m")
            #vacuum_analyze(config,"threshold_burned")
    
            # Clean up arrays
        BaOut_list = None
        del BaOut_list
        # #######################################################################
        # Finish burned area thresholding
        # #######################################################################
        
        # #######################################################################
        # Begin Active Fire
        # #######################################################################
        
        if config.use750af == "y":
            # #######################################################################
            # Begin 750-m Active Fire
            # #######################################################################
            
            #Set up Active Fire Conditional array: AfCon
            AfCon = np.zeros_like(AfArray)
            
            # Get the coordinates of any active fire pixels using a the same method as 
            # the burned area. 
            # Cast any pixels as one that are active fire
            AfCon[
                (AfArray >= 7) &
                (AfArray <= 9)
                ] = 1
                
            # Apply geographic window, if specified
            if config.has_window() : 
                AfCon[
                    (LatArray > config.north) | 
                    (LatArray < config.south) | 
                    (LonArray < config.west) | 
                    (LonArray > config.east)
                ] = 0
                
            # Get coordinates of all active fire pixels    
            AfLatLons = get_coords_from_Con_array(AfCon, LatArray, LonArray)
            # Clean up arrays
            AfCon = None
            del AfCon
            # Convert coordinate array to list
            AfOut_list = array2list(AfLatLons)
            # Clean up arrays
            AfLatLons = None
            del AfLatLons
        
        
        if config.use375af == "y":
            # #######################################################################
            # Begin 375-m Active Fire
            # #######################################################################
            
            #Set up Active Fire Conditional array: AfCon
            AfCon = np.zeros_like(Af375Array)
            # Get the coordinates of any active fire pixels using a the same method as 
            # the burned area. 
            # Cast any pixels as one that are active fire
            AfCon[
                (Af375Array >= 7) &
                (Af375Array <= 9)
                ] = 1
            
            # Apply geographic window, if specified
            if config.has_window() : 
                AfCon[
                    (Lat375Array > config.north) | 
                    (Lat375Array < config.south) | 
                    (Lon375Array < config.west) | 
                    (Lon375Array > config.east)
                ] = 0
                
            # Get coordinates of all active fire pixels    
            Af375LatLons = get_coords_from_Con_array(AfCon, Lat375Array, Lon375Array)
            # Clean up arrays
            AfCon = None
            del AfCon
            
            # Convert coordinate array to list
            Af375Out_list = array2list(Af375LatLons)
            # Clean up arrays
            Af375LatLons = None
            del Af375LatLons
        if config.TextOut == "y":
            if config.use750af == "y":  
                # Write active fire 750 coordinates to text
                write_coordinates2text(config, AfOut_list, "AfOut_" + H5DateStr, H5Date)
            if config.use375af == "y":    
                # Write active fire 375 coordinates to text
                write_coordinates2text(config, Af375Out_list, "AfOut_" + H5DateStr, H5Date)

        # Push active fire coordinates to PostGIS
        if config.DatabaseOut == "y":
            if config.use375af == "y":
                # write 375 active fire to DB
                push_list_to_postgis(config,Af375Out_list, H5Date, "active_fire", "375", "i")
                #vacuum_analyze(config,"active_fire")
            
            if config.use750af == "y":
                # write 750 active fire to DB
                push_list_to_postgis(config,AfOut_list, H5Date, "active_fire", "750", "m")
                #vacuum_analyze(config,"active_fire")
        
            # check if fires are still active
            print "\nChecking if fires are still active"
            date_4db = datetime.datetime.strftime(H5Date, "%Y-%m-%d %H:%M:%S")
            execute_check_4_activity(config, date_4db)
            
            # active fire to fires events 
            print "\nCopy active fire to fire events and create collections"
            date_4db = datetime.datetime.strftime(H5Date, "%Y-%m-%d %H:%M:%S")
            execute_active_fire_2_events(config, date_4db)
            #vacuum_analyze(config,"active_fire")
    
            # simple confirm threshold burns 
            print "\nPerform simple confirm burned area"
            date_4db = datetime.datetime.strftime(H5Date, "%Y-%m-%d %H:%M:%S")
            execute_simple_confirm_burns(config, date_4db)
            #vacuum_analyze(config,"threshold_burned")
    
            # threshold to fires events 
            print "\nEvaluate and copy thresholded burned area to fire events"
            date_4db = datetime.datetime.strftime(H5Date, "%Y-%m-%d %H:%M:%S")
            execute_threshold_2_events(config, date_4db)

            vacuum_analyze(config,"active_fire")
            vacuum_analyze(config,"threshold_burned")
            #vacuum_analyze(config, '')
        
        # Clean up arrays
        AfOut_list = None
        del AfOut_list
        AfArray = None
        del AfArray
        Af375Out_list = None
        del Af375Out_list
        Af375Array = None
        del Af375Array
        
        gc.collect()
        print "Done Processing:", ImageDate,  
        print "Number:", count, "of:", len(config.ImageDates)
        end_indiviudal = datetime.datetime.now()
        print end_indiviudal.strftime("%Y%m%d %H:%M:%S")
        print "Elapsed time for individual:", (end_indiviudal - start_indiviudal).total_seconds(), "seconds"
        print "*"*50 + "\n"

    # Output shapefile
    if config.ShapeOut == "y":
        print "Exporting to point shapefile:"
        if not os.path.exists(config.ShapePath):
            os.makedirs(config.ShapePath)
        shp = config.ShapePath + '/' + 'fire_collection_point_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')    
        Pgsql2shpExe = os.path.join(config.PostBin, "pgsql2shp")
        query = 'SELECT a.*, b.fid as col_id, b.active FROM "{0}".fire_events a, "{0}".fire_collections b WHERE a.collection_id = b.fid;'.format(config.DBschema)
        if config.DBhost is None : 
            command =  '{0} -f {1} -h localhost -u {2} -P {3} {4} {5}'.format(pipes.quote(Pgsql2shpExe), shp, config.DBuser, config.pwd, config.DBname, pipes.quote(query))
        else : 
            command =  '{0} -f {1} -h {6} -u {2} -P {3} {4} {5}'.format(pipes.quote(Pgsql2shpExe), shp, config.DBuser, config.pwd, config.DBname, pipes.quote(query), config.DBhost)
            
        print command
        subprocess.call(command, shell = True)

        print "Exporting to polygon shapefile:"
        if not os.path.exists(config.ShapePath):
            os.makedirs(config.ShapePath)
        shp = config.ShapePath + '/' + 'fire_collection_poly_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')    
        Pgsql2shpExe = os.path.join(config.PostBin, "pgsql2shp")
        query = 'SELECT ST_Multi(ST_Union(ST_Expand(geom, 375))) as geom, collection_id FROM "{0}".fire_events GROUP BY collection_id;'.format(config.DBschema)
        if config.DBhost is None : 
            command =  '{0} -f {1} -h localhost -u {2} -P {3} {4} {5}'.format(pipes.quote(Pgsql2shpExe), shp, config.DBuser, config.pwd, config.DBname, pipes.quote(query))
        else : 
            command =  '{0} -f {1} -h {6} -u {2} -P {3} {4} {5}'.format(pipes.quote(Pgsql2shpExe), shp, config.DBuser, config.pwd, config.DBname, pipes.quote(query), config.DBhost)
        print command
        subprocess.call(command, shell = True)

	config.save(os.path.join(config.ShapePath, '{0}_{1}.ini'.format(config.DBname,config.DBschema)))

    end_group = datetime.datetime.now()
    print end_group.strftime("%Y%m%d %H:%M:%S")
    print "Elapsed time for group:", (end_group - start_group).total_seconds(), "seconds"

    print "Done"
    print "Done"
    
################################################################################


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print "\nMissing argrument"
        print "\nEnter the ini file name as an argument when launching this script."
        print "e.g., VIIRS_threshold.py VIIRS_threshold.ini"	
        print "The ini file should be in the current working directory.\n"
        sys.exit()
    IniFileName = sys.argv[1]    
    IniFile = os.path.join(os.getcwd(), IniFileName)
    config = vc.VIIRSConfig.load(IniFile)
	
    run(config)
