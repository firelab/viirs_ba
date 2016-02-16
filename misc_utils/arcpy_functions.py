# This file contains the arcpy funcitons that export rasters and shape files
# These were removed from the production script because they are not used.
# I'm saving them here just in case..



# The function array2raster uses arcpy to output a raster from the VIIRS array.
# This function DOES NOT handle the pixel size properly. The output is NOT
# properly aligned in space. These rasters are for testing only.
def array2raster(array, lat, lon, OutRaster):
    array  = np.fliplr(np.flipud(array))
    lat = np.fliplr(np.flipud(lat))
    lon = np.fliplr(np.flipud(lon))
    OutRaster = OutRaster + ".tif"
    if os.path.exists(os.path.join(BaseDir, "tiffs", OutRaster)):
        os.remove(os.path.join(BaseDir,  "tiffs",OutRaster))
    cellSize = 1
    LLlat = float(lat[lat.shape[0]-1, 0])
    LLlon = float(lon[lon.shape[0]-1, 0])
    print "LLlat:", LLlat
    print "LLlon:", LLlon

    tempRaster = arcpy.NumPyArrayToRaster(array, arcpy.Point(LLlon, LLlat),cellSize, cellSize)
    tempRaster.save(os.path.join(BaseDir,  "tiffs",OutRaster))
    del tempRaster
    array = None
    lat = None
    lon = None
    del array
    del lat
    del lon


# Output to shapefile    
def out_to_shapefile(list, fileName, date):
    shp_file = fileName +'.shp'
    # Check for pre-existing shape, delete if necessary.
    if os.path.exists(os.path.join(BaseDir, shp_file)):
        arcpy.Delete_management(os.path.join(BaseDir, shp_file))
    # Set up parameters and delete create shapefile.    
    geometry_type = "POINT"
    spatial = """GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]"""
    arcpy.CreateFeatureclass_management(BaseDir, shp_file, geometry_type, "", "Disabled", "Disabled", spatial)
    # Add attributes
    shp_file = os.path.join(BaseDir, shp_file)
    arcpy.AddField_management(shp_file, "Lat", "FLOAT")
    arcpy.AddField_management(shp_file, "Lon", "FLOAT")
    arcpy.AddField_management(shp_file, "Date", "DATE")
    # Set up cursor and loop through list adding rows.
    cursor = arcpy.da.InsertCursor(shp_file, ["Lat", "Lon", "Date", "SHAPE@XY"])
    for coord in list:
        row = [coord[0], coord[1], date, (coord[1], coord[0])]
        cursor.insertRow(row)
    del cursor
    
    
# Output rasters from arrays 
# The following should be uncommented if rasters are needed for testing.
##array2raster(M07ReflArray, LatArray, LonArray, "M07Refl")
##array2raster(M08ReflArray, LatArray, LonArray, "M08Refl")
##array2raster(M10ReflArray, LatArray, LonArray, "M10Refl")
##array2raster(M11ReflArray, LatArray, LonArray, "M11Refl")
##array2raster(AfArray, LatArray, LonArray, "ActiveFire")

    # Output shapefile
    if ShapeOut == "y":
        print "Exporting to point shapefile:"
        if not os.path.exists(ShapePath):
            os.makedirs(ShapePath)
        shp = ShapePath + '/' + 'fire_collection_point_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')    
        Pgsql2shpExe = os.path.join(PostBin, "pgsql2shp")
        query = '\"SELECT a.*, b.fid as col_id, b.active FROM fire_events a, fire_collections b WHERE a.collection_id = b.fid;\"'
        command =  '\"{0}\" -f {1} -h localhost -u {2} -P {3} {4} {5}'.format(Pgsql2shpExe, shp, DBuser, pwd, DBname, query).replace('\\', '/')     
        print command
        subprocess.call(command, shell = True)
        shutil.copy2(IniFile, os.path.join(ShapePath, os.path.basename(IniFile + '_'+ datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))))     

        print "Exporting to polygon shapefile:"
        if not os.path.exists(ShapePath):
            os.makedirs(ShapePath)
        shp = ShapePath + '/' + 'fire_collection_poly_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')    
        Pgsql2shpExe = os.path.join(PostBin, "pgsql2shp")
        query = '\"SELECT ST_Multi(ST_Union(ST_Expand(geom, 375))) as geom, collection_id FROM fire_events GROUP BY collection_id;\"'
        command =  '\"{0}\" -f {1} -h localhost -u {2} -P {3} {4} {5}'.format(Pgsql2shpExe, shp, DBuser, pwd, DBname, query).replace('\\', '/')     
        print command
        subprocess.call(command, shell = True)
        shutil.copy2(IniFile, os.path.join(ShapePath, os.path.basename(IniFile + '_'+ datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))))     

        
ShapeOut = ini.get("OutputFlags", "ShapeFile").lower()
ShapePath = ini.get("OutputFlags", "OutShapeDir").lower()

# lines from ini file
#ShapeFile = n       ; Flag to output to shapefile using pgsql2shp
#
#OutShapeDir = c:\fiddle\VIIRS_Data\ShapeOut                    ; Shapefile output directory

    