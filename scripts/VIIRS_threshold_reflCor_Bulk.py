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
from collections import namedtuple

# Friendly names for relevant projections: 
srids = { 
  "WGS84" : 4326,
  "Albers" : 102008,
  "NLCD"   : 96630
}

Point = namedtuple('Point', ['latitude','longitude'])

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

def execute_sql_file(config, filename):
    print "Start", get_time()

    command = 'psql -h {0} -U {1} -d {2} -f {3}'.format(
        config.DBhost,
        config.DBuser,
        config.DBname,
        filename)
    env = os.environ.copy()
    env['PGPASSWORD'] = config.pwd
    print command
    subprocess.call(command, shell=True, env=env)

    print "End", get_time()

  
def execute_check_4_activity(config, collectionDate):
    query_text = "SELECT viirs_check_4_activity('{0}', '{1}', '{2}');".format(config.DBschema, collectionDate, config.get_sql_interval())
    execute_query(config,query_text)
 
def execute_active_fire_2_events(config, collectionDate):
    print "Start active_fire to fire_events", get_time()
    query_text = "SELECT VIIRS_activefire_2_fireevents('{0}', '{1}', '{2}', {3});".format(config.DBschema, collectionDate, config.get_sql_interval(), config.SpatialProximity)
    execute_query(config,query_text)

def execute_threshold_2_events(config, collectionDate):
    print "Start VIIRS_threshold_2_fireevents", get_time()
    query_text = "SELECT VIIRS_threshold_2_fireevents('{0}', '{1}', '{2}', {3});".format(config.DBschema, collectionDate, config.get_sql_interval(), config.SpatialProximity)
    execute_query(config,query_text)

def execute_simple_confirm_burns(config, collectionDate):
    print "Start threshold_burned to fire_events", get_time()
    query_text = "SELECT VIIRS_simple_confirm_burns('{0}', '{1}', '{2}', {3});".format(config.DBschema, collectionDate, config.get_sql_interval(), config.SpatialProximity)
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

def output_shape_files(config) : 
    """produces the shapefile products in the specified output folder"""

    if not os.path.exists(config.ShapePath):
        os.makedirs(config.ShapePath)

    print "Exporting to point shapefile:"
    Pgsql2shpExe = os.path.join(config.PostBin, "pgsql2shp")
    shp = config.ShapePath + '/' + 'fire_collection_point_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')    

    query = 'SELECT a.*, b.fid as col_id, b.active FROM "{0}".fire_events a, "{0}".fire_collections b WHERE a.collection_id = b.fid;'.format(config.DBschema)
    if config.DBhost is None : 
        command =  '{0} -f {1} -h localhost -u {2} -P {3} -g geom {4} {5}'.format(pipes.quote(Pgsql2shpExe), shp, config.DBuser, config.pwd, config.DBname, pipes.quote(query))
    else : 
        command =  '{0} -f {1} -h {6} -u {2} -P {3} -g geom {4} {5}'.format(pipes.quote(Pgsql2shpExe), shp, config.DBuser, config.pwd, config.DBname, pipes.quote(query), config.DBhost)
            
    print command
    subprocess.call(command, shell = True)

    print "Exporting to polygon shapefile:"
    shp = config.ShapePath + '/' + 'fire_collection_poly_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')    
    query = 'SELECT ST_Multi(ST_Union(ST_Expand(geom, 375))) as geom, collection_id FROM "{0}".fire_events GROUP BY collection_id;'.format(config.DBschema)

    if config.DBhost is None : 
        command =  '{0} -f {1} -h localhost -u {2} -P {3} {4} {5}'.format(pipes.quote(Pgsql2shpExe), shp, config.DBuser, config.pwd, config.DBname, pipes.quote(query))
    else : 
        command =  '{0} -f {1} -h {6} -u {2} -P {3} {4} {5}'.format(pipes.quote(Pgsql2shpExe), shp, config.DBuser, config.pwd, config.DBname, pipes.quote(query), config.DBhost)
    print command
    subprocess.call(command, shell = True)


class FileSet (object) : 
    """Represents a set of files containing satellite data from the same scene.
    Collects filenames only, and associates them with a common date.
    """
        
    @classmethod
    def from_imagedate(cls, imagedate_str) : 
        target=cls()
        target.ImageDate = imagedate_str
        return target

    @classmethod
    def from_date(cls, dt) : 
        """creates a new fileset object from a datetime object"""
        target = cls() 
        target._dt = dt
        return target
                
    @classmethod
    def parse_filename(cls, filename) : 
        """given a filename that matches the expected pattern, compute and 
        return a FileSet object."""
        dt = filename.split("_")[2][1:9] + f.split("_")[3][1:7]
        dt = datetime.datetime.strptime(dt, '%Y%m%d%H%M%S')
        return cls.from_date(dt)
        
        
    def get_datetime(self) :
        """compute and return the datetime object associated with this file set.""" 
        # pattern:
        # d20140715_t1921193,
        if not hasattr(self, '_dt') : 
            components = self.ImageDate.split("_")
            dt = components[0][1:] + components[1][1:-1]
            self._dt = datetime.datetime.strptime(dt, '%Y%m%d%H%M%S')
        return self._dt
        
    def get_out_date(self) : 
        """a formatted date string to include in output file names"""
        if not hasattr(self, "_out_date")  :
            self._out_date = datetime.datetime.strftime(self.get_datetime(), 
                                                       "%Y%m%d_%H%M%S")
                                                       
        return self._out_date

        
    def get_sql_date(self) : 
        """compute and return the SQL formatted date string associated with 
        this file set."""
        if not hasattr(self,"_date_4db") : 
            self._date_4db = datetime.datetime.strftime(self.get_datetime(), 
                     "%Y-%m-%d %H:%M:%S")
        return self._date_4db
        
    def get_imagedate(self) : 
        if not hasattr(self,"ImageDate") : 
            self.ImageDate = "d{0:%Y%m%d}_t{0:%H%M%S}".format(self._dt)
        return self.ImageDate
        
    def find_hdf_file(self, basedir, prefix,extension="h5") : 
        """locates an hdf4/5 file in the basedir having a specific prefix,
        returns the full pathname to the file"""
        h5 = glob.glob(os.path.join(basedir, 
           "{0}_{1}_e???????_b00001_c????????????????????_all-_dev.{2}".format(
              prefix,self.get_imagedate(), extension)))[0]
        return h5
        
    def get_file_names(self, basedir) : 
        """attempt to locate all files in this set, return a dictionary of filenames"""
        if not hasattr(self, "_filenames") : 
            self._filenames = {} 
            fileset = [ ("SVM07_npp", 'h5') , 
                        ("SVM08_npp", 'h5') , 
                        ("SVM10_npp", 'h5') , 
                        ("SVM11_npp", 'h5') , 
                        ("GMTCO_npp", 'h5') , 
                        ("GITCO_npp", 'h5') ,
                        ("VF375_npp", 'hdf'),
                        ("AVAFO_npp", 'h5') ]
                        
            for pre, ext in fileset :
                try : 
                    self._filenames[pre] = self.find_hdf_file(basedir, pre, ext)
                except : 
                    pass
            
        return self._filenames
        
        


class GeoFile(object) :
    """A geofile can apply a geographic window and produce a list of lat/lon
    values associated with confirmed values"""
    
    def apply_window(self, config, confirmed) : 
        """applies a geographic window specified in the config to the confirmed array
        Note that the confirmed array must be the same shape as this object's 
        latitude and longitude arrays."""
        if config.has_window() : 
            confirmed[
                (self.LatArray > config.north) | 
                (self.LatArray < config.south) | 
                (self.LonArray < config.west) | 
                (self.LonArray > config.east)
            ] = 0
            
    def make_list(self, confirmed) : 
        """produces a list of tuples containing the lat/lon coordinates
        corresponding to true values in the confirmed mask.
        This will only be successful if the confirmed array and the Lat/LonArrays
        are related."""
        idx = np.where(confirmed == 1) 
        return zip(self.LatArray[idx],self.LonArray[idx])
        
    def day_pixels(self, zenith) : 
        """given a specified solar zenith angle defining "sundown", returns 
        where the day pixels are"""
        return self.SolZen < zenith
        


class GeoFile750(GeoFile) : 
    def __init__(self) :
        self.pixel_size = 750
        self.band_i_m   = 'm'
        
    @classmethod
    def load(cls,filename) : 
        target = cls()
        GeoHdf = h5py.File(filename,"r")
        target.LatArray = GeoHdf['All_Data/VIIRS-MOD-GEO-TC_All/Latitude'][:]
        target.LonArray = GeoHdf['All_Data/VIIRS-MOD-GEO-TC_All/Longitude'][:]
        target.SolZen = GeoHdf['All_Data/VIIRS-MOD-GEO-TC_All/SolarZenithAngle'][:]
        return target 
        
class GeoFile375(GeoFile) : 
    def __init__(self) : 
        self.pixel_size = 375
        self.band_i_m   = 'i'
    
    @classmethod
    def load(cls, filename) :
        target = cls()
        GeoHdf = h5py.File(h5,"r")
        target.LatArray = GeoHdf['All_Data/VIIRS-IMG-GEO-TC_All/Latitude'][:]
        target.LonArray = GeoHdf['All_Data/VIIRS-IMG-GEO-TC_All/Longitude'][:]
        return target

        
    
class ActiveFire(object) :
    """Encapsulates ActiveFire data, regardless of resolution"""
    
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

    def get_conditional(self) : 
        """computes if necessary and returns the "conditional" array for this object""" 
        if not hasattr(self, 'conditional') : 
            self.conditional = np.zeros_like(self.AfArray)

            self.conditional[
                (self.AfArray >= 7) &
                (self.AfArray <= 9)
                ] = 1
        return self.conditional
        
    def get_non_fire(self) : 
        return (self.AfArray == 5)

    
    
class ActiveFire750 (ActiveFire) : 
    def __init__(self) : 
        self.pixel_size = 750
        self.band_i_m = 'm'

    @classmethod
    def load(cls, filename) :
        """Reads in a 750m VIIRS HDF 5 file from disk."""
        target = cls()  
        target._AfHdf = h5py.File(filename, "r")
        target.AfArray = target._AfHdf['All_Data/VIIRS-AF-EDR_All/fireMask'][:]
        target.AfDateTime = h5_date_time(os.path.basename(filename))
        return target
        
    
class ActiveFire375 (ActiveFire) : 
    def __init__(self) : 
        self.pixel_size = 375
        self.band_i_m = 'i'
        
    @classmethod
    def load(cls, filename) :
        """Reads in a 375m VIIRS HDF4 file from disk."""
        target = cls() 
        target._AF375hdf = SD(filename, SDC.READ)
        target._AF375_fm = target._AF375hdf.select('fire mask')
        target.AfArray = target._AF375_fm[:]
        target.AfDateTime = h5_date_time(os.path.basename(filename))
        return target

def run(config):
    
    if config.DatabaseOut == "y":
        initialize_schema_for_postgis(config)

    #Loop through BaseDir, look for h5s and load arrays
    count = 0
    start_group = datetime.datetime.now()
    for ImageDate in config.SortedImageDates:
        start_indiviudal = datetime.datetime.now()
        count  = count + 1
        print "Processing number:", count, "of:", len(config.SortedImageDates)
        print ImageDate + '\n'
        
        fileset = FileSet.from_imagedate(ImageDate)
        files = fileset.get_file_names()

        #Read band 7
        #h5 = glob.glob(os.path.join(config.BaseDir, "SVM07_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading band 07: ", os.path.basename(files['SVM07_npp'])        
        M07Hdf = h5py.File(files['SVM07_npp'], "r")
        M07ReflArray = M07Hdf['All_Data/VIIRS-M7-SDR_All/Reflectance'][:]
        M07ReflFact = M07Hdf['All_Data/VIIRS-M7-SDR_All/ReflectanceFactors'][:]

        # Read band 8
        #h5 = glob.glob(os.path.join(config.BaseDir, "SVM08_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading band 08:", os.path.basename(files['SVM08_npp'])
        M08Hdf = h5py.File(files['SVM08_npp'], "r")
        M08ReflArray = M08Hdf['All_Data/VIIRS-M8-SDR_All/Reflectance'][:]
        M08ReflFact = M08Hdf['All_Data/VIIRS-M8-SDR_All/ReflectanceFactors'][:]

        # Read band 10
        #h5 = glob.glob(os.path.join(config.BaseDir, "SVM10_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading band 10:", os.path.basename(files['SVM10_npp'])
        M10Hdf = h5py.File(files['SVM10_npp'], "r")
        M10ReflArray = M10Hdf['All_Data/VIIRS-M10-SDR_All/Reflectance'][:]
        M10ReflFact = M10Hdf['All_Data/VIIRS-M10-SDR_All/ReflectanceFactors'][:]

        # Read band 11
        #h5 = glob.glob(os.path.join(config.BaseDir, "SVM11_npp_" + ImageDate + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
        print "Reading band 11:", os.path.basename(files['SVM11_npp'])
        M11Hdf = h5py.File(files['SVM11_npp'], "r")
        M11ReflArray = M11Hdf['All_Data/VIIRS-M11-SDR_All/Reflectance'][:]
        M11ReflFact = M11Hdf['All_Data/VIIRS-M11-SDR_All/ReflectanceFactors'][:]

        # Read GMTCO
        geo_750 = GeoFile750.load(files['GMTCO_npp'])
        
        if config.use375af.lower() == "y":
            # Read GITCO
            geo_375 = GeoFile375.load(files['GITCO_npp'])
            
            # Read VF375
            af_375 = ActiveFire375.load(files['VF375_npp'])
        
        # Read AVAFO
        af_750 = ActiveFire750.load(files['AVAFO_npp'])
            
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
            (af_750.get_non_fire()) &
            (geo_750.day_pixels(config.MaxSolZen)) &   #this should supress night pixels
            ((M07ReflArray - M07ReflFact[1])/M07ReflFact[0] < 65528) &
            ((M08ReflArray - M08ReflFact[1])/M08ReflFact[0] < 65528) &
            ((M10ReflArray - M10ReflFact[1])/M10ReflFact[0] < 65528) &
            ((M11ReflArray - M11ReflFact[1])/M11ReflFact[0] < 65528)
            ] = 1

        # Apply geographic window, if specified
        geo_750.apply_window(config,BaCon)
                
    
        # Clean up arrays
        M07ReflArray = None
        M08ReflArray = None
        M10ReflArray = None
        M11ReflArray = None
        M07ReflFact = None
        M08ReflFact = None
        M10ReflFact = None
        M11ReflFact = None
        del M07ReflArray
        del M08ReflArray
        del M10ReflArray
        del M11ReflArray
        del M07ReflFact
        del M08ReflFact
        del M10ReflFact
        del M11ReflFact
            
        
        # Get Burned area coordinates as an array
        BaOut_list = geo_750.make_list(BaCon)
        # Clean up arrays
        BaCon = None
        del BaCon
        
        # Burned area output to text
        if config.TextOut == "y":
            write_coordinates2text(config, BaOut_list, 
                 "BaOut_" + fileset.get_out_date(), fileset.get_datetime())

        # Burned area output to PostGIS 
        if config.DatabaseOut == "y":
            push_list_to_postgis(config, BaOut_list, 
                 fileset.get_datetime(), "threshold_burned", "750", "m")
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
            AfCon = af_750.get_conditional()
                            
            # Apply geographic window, if specified
            geo_750.apply_window(config, AfCon)
                
            # Get coordinates of all active fire pixels 
            AfLatLons = geo_750.make_list(AfCon)  
             
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
            AfCon = af_375.get_conditional()
            
            # Apply geographic window, if specified
            geo_375.apply_window(AfCon)
                
            # Get coordinates of all active fire pixels 
            Af375Out_list = geo_375.make_list(AfCon) 
              
            # Clean up arrays
            AfCon = None
            del AfCon
            
        if config.TextOut == "y":
            if config.use750af == "y":  
                # Write active fire 750 coordinates to text
                write_coordinates2text(config, AfOut_list, 
                       "AfOut_" + fileset.get_out_date(), 
                       fileset.get_datetime() )
            if config.use375af == "y":    
                # Write active fire 375 coordinates to text
                write_coordinates2text(config, Af375Out_list, 
                       "AfOut_" + fileset.get_out_date(), 
                       fileset.get_datetime())

        # Push active fire coordinates to PostGIS
        if config.DatabaseOut == "y":
            if config.use375af == "y":
                # write 375 active fire to DB
                push_list_to_postgis(config,Af375Out_list, 
                     fileset.get_datetime(), "active_fire", "375", "i")
                #vacuum_analyze(config,"active_fire")
            
            if config.use750af == "y":
                # write 750 active fire to DB
                push_list_to_postgis(config,AfOut_list, 
                     fileset.get_datetime(), "active_fire", "750", "m")
                #vacuum_analyze(config,"active_fire")
        
            # check if fires are still active
            #print "\nChecking if fires are still active"
            #date_4db = datetime.datetime.strftime(H5Date, "%Y-%m-%d %H:%M:%S")
            #execute_check_4_activity(config, date_4db)
            
            # active fire to fires events 
            print "\nCopy active fire to fire events and create collections"
            execute_active_fire_2_events(config, fileset.get_sql_date())
            #vacuum_analyze(config,"active_fire")
    
            # simple confirm threshold burns 
#            print "\nPerform simple confirm burned area"
#            date_4db = datetime.datetime.strftime(H5Date, "%Y-%m-%d %H:%M:%S")
#            execute_simple_confirm_burns(config, date_4db)
#            #vacuum_analyze(config,"threshold_burned")
   
            # threshold to fires events 
            print "\nEvaluate and copy thresholded burned area to fire events"
            execute_threshold_2_events(config, fileset.get_sql_date())

            vacuum_analyze(config,"active_fire")
            vacuum_analyze(config,"threshold_burned")
            #vacuum_analyze(config, '')
        
        # Clean up arrays
        AfOut_list = None
        del AfOut_list
        Af375Out_list = None
        del Af375Out_list
        
        gc.collect()
        print "Done Processing:", ImageDate,  
        print "Number:", count, "of:", len(config.SortedImageDates)
        end_indiviudal = datetime.datetime.now()
        print end_indiviudal.strftime("%Y%m%d %H:%M:%S")
        print "Elapsed time for individual:", (end_indiviudal - start_indiviudal).total_seconds(), "seconds"
        print "*"*50 + "\n"

    # Output shapefile
    if config.ShapeOut == "y":
        output_shape_files(config)

    config.save(os.path.join(config.ShapePath, '{0}_{1}.ini'.format(config.DBname,config.DBschema)))

    end_group = datetime.datetime.now()
    print end_group.strftime("%Y%m%d %H:%M:%S")
    print "Elapsed time for group:", (end_group - start_group).total_seconds(), "seconds"

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
