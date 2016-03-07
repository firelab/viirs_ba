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
import numpy as np
import h5py
import psycopg2
import ConfigParser
import time
import datetime
import gc
import subprocess
from pyhdf.SD import SD, SDC



# Prints the contents of an h5.
def print_name(name):
    print name


# Returns the memory block address of an array.
def id(x):
    return x.__array_interface__['data'][0]


# Write the coordinate list to text file.
def write_coordinates2text(coordsList, fileName, date):
    if not os.path.exists(os.path.join(BaseDir,"TextOut")):
        os.makedirs(os.path.join(BaseDir,"TextOut"))
    outfile = os.path.join(BaseDir, "TextOut", fileName + ".txt")
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


# Push the coordinates and date/time of the thresholded pixels to PostGIS
def push_list_to_postgis(list, date, table, pSize, band):
    print "\nPushing data to VIIRS_burned_area DB table: public." + table
    format = '%Y-%m-%d %H:%M:%S'
    # Connect to VIIRS database
    ConnParam = "dbname={0} user={1} password={2}".format(DBname, DBuser, pwd)
    conn = psycopg2.connect(ConnParam)
    # Open a cursor to perform database operations
    cur = conn.cursor()
    #Loop through list
    for i in list:
        # Execute a command to insert new records into table
        cur.execute("INSERT INTO public.%s (latitude, longitude, collection_date, geom, pixel_size, band_i_m) VALUES ('%s','%s','%s', ST_GeomFromText('POINT(%s %s)',4326),'%s','%s');"%(table, i[0], i[1], datetime.datetime.strftime(date, format), i[1], i[0],pSize,band))
        # Make the changes to the database persistent
        conn.commit()
    old_isolation_level = conn.isolation_level
    conn.set_isolation_level(0)
    cur.execute("VACUUM ANALYZE %s;"%(table)) 
    conn.set_isolation_level(old_isolation_level)
    conn.commit()
    # Close communication with the database
    cur.close()
    conn.close()
def execute_query(queryText):
    print "Start", queryText, get_time()
    ConnParam = "dbname={0} user={1} password={2}".format(DBname, DBuser, pwd)
    conn = psycopg2.connect(ConnParam)
    # Open a cursor to perform database operations
    cur = conn.cursor()
    cur.execute(queryText)
    conn.commit()
    # Close communication with the database
    cur.close()
    conn.close()
    print "End", queryText, get_time()
 
def execute_check_4_activity(collectionDate, interval):
    query_text = "SELECT viirs_check_4_activity(\'{0}\', \'{1}\');".format(collectionDate, interval)
    execute_query(query_text)
 
def execute_active_fire_2_events(collectionDate, interval, distance):
    print "Start active_fire to fire_events", get_time()
    query_text = "SELECT VIIRS_activefire_2_fireevents(\'{0}\', \'{1}\', {2});".format(collectionDate, interval, distance)
    execute_query(query_text)

def execute_threshold_2_events(collectionDate, interval, distance):
    print "Start VIIRS_threshold_2_fireevents", get_time()
    query_text = "SELECT VIIRS_threshold_2_fireevents(\'{0}\', \'{1}\', {2});".format(collectionDate, interval, distance)
    execute_query(query_text)

def execute_simple_confirm_burns(collectionDate, interval, distance):
    print "Start threshold_burned to fire_events", get_time()
    query_text = "SELECT VIIRS_simple_confirm_burns(\'{0}\', \'{1}\', {2});".format(collectionDate, interval, distance)
    execute_query(query_text)

 
# def execute_copy_threshold_burned_2_fire_events(collectionDate):
    # print "Copying confimed burned to fire events for:", collectionDate
    # query_text = "SELECT copy_threshold_burned_2_fireevents(\'{0}\');".format(collectionDate)
    # execute_query(query_text)
    
# def execute_copy_active_fire_2_fire_events(collectionDate):
    # print "Copying active fire to fire events for:", collectionDate
    # query_text = "SELECT copy_activefire_2_fireevents(\'{0}\');".format(collectionDate)
    # execute_query(query_text)
    
def vacuum_analyze(table):
    print "Start Vacuum {0}".format(table), get_time()
    query_text = "VACUUM ANALYZE {0}".format(table) 
    # Connect to VIIRS database
    ConnParam = "dbname={0} user={1} password={2}".format(DBname, DBuser, pwd)
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

def get_time():
    ts = time.time()
    dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return dt
    
    

def run():
    # Define threshold values.
#    Thresholds = {
#        "M07UB" : 1800,     #Band 07 (0.86 um)upper bound
#        "M08LB" : 500,      #Band 08 (1.24 um)lower bound
#        "M08UB" : 2000,     #Band 08 (1.24 um)upper bound
#        "M10LB" : 1000,     #Band 10 (1.61 um)lower bound
#        "M10UB" : 10000,    #Band 10 (1.61 um)upper bound
#        "M11LB" : 500,      #Band 11 (2.25 um)lower bound
#        "RthSub" : 500,     #RthSub is the factor subtracted from the 1.240 band when comparing to the Rth
#        "Rth" : 8000,       #Rth
#        "RthLB": 0          #RthLB is the factor that the Rth check must be less than or equal to
#        }

    #Loop through BaseDir, look for h5s and load arrays
    count = 0
    start_group = datetime.datetime.now()
    for ImageDate in ImageDates:
        start_indiviudal = datetime.datetime.now()
        count  = count + 1
        print "Processing number:", count, "of:", len(ImageDates)
        print ImageDate + '\n'
        #Read band 7
        h5 = glob.glob(os.path.join(BaseDir, "SVM07_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading band 07:", os.path.basename(h5)
        
        M07Hdf = h5py.File(h5, "r")
        M07ReflArray = M07Hdf['All_Data/VIIRS-M7-SDR_All/Reflectance'][:]
        M07ReflFact = M07Hdf['All_Data/VIIRS-M7-SDR_All/ReflectanceFactors'][:]

        # Read band 8
        h5 = glob.glob(os.path.join(BaseDir, "SVM08_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading band 08:", os.path.basename(h5)
        M08Hdf = h5py.File(h5, "r")
        M08ReflArray = M08Hdf['All_Data/VIIRS-M8-SDR_All/Reflectance'][:]
        M08ReflFact = M08Hdf['All_Data/VIIRS-M8-SDR_All/ReflectanceFactors'][:]

        # Read band 10
        h5 = glob.glob(os.path.join(BaseDir, "SVM10_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading band 10:", os.path.basename(h5)
        M10Hdf = h5py.File(h5, "r")
        M10ReflArray = M10Hdf['All_Data/VIIRS-M10-SDR_All/Reflectance'][:]
        M10ReflFact = M10Hdf['All_Data/VIIRS-M10-SDR_All/ReflectanceFactors'][:]

        # Read band 11
        h5 = glob.glob(os.path.join(BaseDir, "SVM11_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading band 11:", os.path.basename(h5)
        M11Hdf = h5py.File(h5, "r")
        M11ReflArray = M11Hdf['All_Data/VIIRS-M11-SDR_All/Reflectance'][:]
        M11ReflFact = M11Hdf['All_Data/VIIRS-M11-SDR_All/ReflectanceFactors'][:]

        # Read GMTCO
        h5 = glob.glob(os.path.join(BaseDir, "GMTCO_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading GMTCO Latitude and Longitude:", os.path.basename(h5)
        GeoHdf = h5py.File(h5,"r")
        LatArray = GeoHdf['All_Data/VIIRS-MOD-GEO-TC_All/Latitude'][:]
        LonArray = GeoHdf['All_Data/VIIRS-MOD-GEO-TC_All/Longitude'][:]
        print "Reading GMTCO Solar Zenith:", os.path.basename(h5)
        SolZen750 = GeoHdf['All_Data/VIIRS-MOD-GEO-TC_All/SolarZenithAngle'][:]
        
        if use375af.lower() == "y":
            # Read GITCO
            h5 = glob.glob(os.path.join(BaseDir, "GITCO_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
            print "Reading GITCO Latitude and Longitude:", os.path.basename(h5)
            GeoHdf = h5py.File(h5,"r")
            Lat375Array = GeoHdf['All_Data/VIIRS-IMG-GEO-TC_All/Latitude'][:]
            Lon375Array = GeoHdf['All_Data/VIIRS-IMG-GEO-TC_All/Longitude'][:]
            print "Reading GITCO Solar Zenith:", os.path.basename(h5)
            #SolZen375 = GeoHdf['All_Data/VIIRS-IMG-GEO-TC_All/SolarZenithAngle'][:]
            
            # Read VF375
            h4 = glob.glob(os.path.join(BaseDir, "VF375_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.hdf"))[0]
            print "Reading VF375:", os.path.basename(h5)
            AF375hdf = SD(h4, SDC.READ)
            AF375_fm = AF375hdf.select('fire mask')
            Af375Array = AF375_fm[:]
            Af375DateTime = h5_date_time(os.path.basename(h4))
        
        # Read AVAFO
        h5 = glob.glob(os.path.join(BaseDir, "AVAFO_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
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
            (M07ReflArray < M07UB) &
            (M08ReflArray > M08LB) &
            (M08ReflArray < M08UB) &
            (M10ReflArray > M10LB) &
            (M10ReflArray < M10UB) &
            (M11ReflArray > M11LB) &
            (np.where(M11ReflArray != 0,((M08ReflArray-RthSub)/M11ReflArray),RthLB-1) >= RthLB) &
            (np.where(M11ReflArray != 0,((M08ReflArray-RthSub)/M11ReflArray),Rth+1) < Rth) &
            (AfArray == 5) &
            (SolZen750 < MaxSolZen) &   #this should supress nigth pixels
            ((M07ReflArray - M07ReflFact[1])/M07ReflFact[0] < 65528) &
            ((M08ReflArray - M08ReflFact[1])/M08ReflFact[0] < 65528) &
            ((M10ReflArray - M10ReflFact[1])/M10ReflFact[0] < 65528) &
            ((M11ReflArray - M11ReflFact[1])/M11ReflFact[0] < 65528)
            ] = 1
    
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
        if TextOut == "y":
            write_coordinates2text(BaOut_list, "BaOut_" + H5DateStr, H5Date)

        # Burned area output to PostGIS 
        if DatabaseOut == "y":
            push_list_to_postgis(BaOut_list, H5Date, "threshold_burned", "750", "m")
            vacuum_analyze("threshold_burned")
    
            # Clean up arrays
        BaOut_list = None
        del BaOut_list
        # #######################################################################
        # Finish burned area thresholding
        # #######################################################################
        
        # #######################################################################
        # Begin Active Fire
        # #######################################################################
        
        if use750af.lower() == "y":
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
        
        
        if use375af.lower() == "y":
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
        if TextOut == "y":
            if use750af.lower() == "y":  
                # Write active fire 750 coordinates to text
                write_coordinates2text(AfOut_list, "AfOut_" + H5DateStr, H5Date)
            if use375af.lower() == "y":    
                # Write active fire 375 coordinates to text
                write_coordinates2text(Af375Out_list, "AfOut_" + H5DateStr, H5Date)

        # Push active fire coordinates to PostGIS
        if DatabaseOut == "y":
            if use375af.lower() == "y":
                # write 375 active fire to DB
                push_list_to_postgis(Af375Out_list, H5Date, "active_fire", "375", "i")
                vacuum_analyze("active_fire")
            
            if use750af.lower() == "y":
                # write 750 active fire to DB
                push_list_to_postgis(AfOut_list, H5Date, "active_fire", "750", "m")
                vacuum_analyze("active_fire")
        
            # check if fires are still active
            print "\nChecking if fires are still active"
            date_4db = datetime.datetime.strftime(H5Date, "%Y-%m-%d %H:%M:%S")
            execute_check_4_activity(date_4db, TemporalProximity)
            
            # active fire to fires events 
            print "\nCopy active fire to fire events and create collections"
            date_4db = datetime.datetime.strftime(H5Date, "%Y-%m-%d %H:%M:%S")
            execute_active_fire_2_events(date_4db, TemporalProximity, SpatialProximity)
            #vacuum_analyze("active_fire")
    
            # simple confirm threshold burns 
#            print "\nPerform simple confirm burned area"
#            date_4db = datetime.datetime.strftime(H5Date, "%Y-%m-%d %H:%M:%S")
#            execute_simple_confirm_burns(date_4db, TemporalProximity, SpatialProximity)
#            #vacuum_analyze("threhold_burned")
    
            # threshold to fires events 
            print "\nEvaluate and copy thresholded burned area to fire events"
            date_4db = datetime.datetime.strftime(H5Date, "%Y-%m-%d %H:%M:%S")
            execute_threshold_2_events(date_4db, TemporalProximity, SpatialProximity)
            #vacuum_analyze("threhold_burned")
            vacuum_analyze('')
        
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
        print "Number:", count, "of:", len(ImageDates)
        end_indiviudal = datetime.datetime.now()
        print end_indiviudal.strftime("%Y%m%d %H:%M:%S")
        print "Elapsed time for individual:", (end_indiviudal - start_indiviudal).total_seconds(), "seconds"
        print "*"*50 + "\n"


    end_group = datetime.datetime.now()
    print end_group.strftime("%Y%m%d %H:%M:%S")
    print "Elapsed time for group:", (end_group - start_group).total_seconds(), "seconds"

    print "Done"
    print "Done"
    
################################################################################

if len(sys.argv) == 1:
    print "\nMissing argrument"
    print "\nEnter the ini file name as an argument when launching this script."
    print "e.g., VIIRS_threshold.py VIIRS_threshold.ini"	
    print "The ini file should be in the current working directory.\n"
    sys.exit()
IniFileName = sys.argv[1]    
IniFile = os.path.join(os.getcwd(), IniFileName)
	
	
ini = ConfigParser.ConfigParser()
ini.read(IniFile)
BaseDir = ini.get("InDirectory", "BaseDirectory")     #Directory with h5 data files  
use375af = ini.get("ActiveFire", "use375af")              # Flag to use M-band 750 m active fire data, AVAFO (y or n)  
use750af = ini.get("ActiveFire", "use750af")              # Flag to use I-band 375 m active fire data, VF375 (y or n)
M07UB = float(ini.get("Thresholds", "M07UB"))         #Band 07 (0.86 um)upper bound
M08LB = float(ini.get("Thresholds", "M08LB"))         #Band 08 (1.24 um)lower bound
M08UB = float(ini.get("Thresholds", "M08UB"))         #Band 08 (1.24 um)upper bound
M10LB = float(ini.get("Thresholds", "M10LB"))         #Band 10 (1.61 um)lower bound
M10UB = float(ini.get("Thresholds", "M10UB"))         #Band 10 (1.61 um)upper bound
M11LB = float(ini.get("Thresholds", "M11LB"))         #Band 11 (2.25 um)lower bound
RthSub = float(ini.get("Thresholds", "RthSub"))       #RthSub is the factor subtracted from the 1.240 band when comparing to the Rth
Rth = float(ini.get("Thresholds", "Rth"))             #Rth
RthLB = float(ini.get("Thresholds", "RthLB"))         #RthLB is the factor that the Rth check must be greater than or equal to
MaxSolZen = float(ini.get("Thresholds", "MaxSolZen")) #Maximum solar zenith angle, used to filter out night pixels from burned area thresholding 

TemporalProximity = ini.get("ConfirmBurnParamaters", "TemporalProximity")
SpatialProximity = int(ini.get("ConfirmBurnParamaters", "SpatialProximity"))	#Proximity of a thresholded point to active fire to be confirmed.

TextOut = ini.get("OutputFlags", "TextFile").lower()
DatabaseOut = ini.get("OutputFlags", "PostGIS").lower()
PostBin = ini.get("OutputFlags", "PostgresqlBin").lower()

DBname = ini.get("DataBaseInfo", "DataBaseName")
DBuser = ini.get("DataBaseInfo", "UserName")
pwd = ini.get("DataBaseInfo", "password")
#Read list of image date/times
ImageDates = ini.get("ImageDates", "ImageDates").split(",")

if __name__ == "__main__":
    run()
