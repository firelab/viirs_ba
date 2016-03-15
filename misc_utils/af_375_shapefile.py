"""
Utility to dump out a shapefile of 375m Active Fire pixels.
"""

import viirs_config as vc
import VIIRS_threshold_reflCor_Bulk as vt
import osgeo.ogr as ogr
import osgeo.osr as osr
import os.path
import sys

def define_layer(data_source, layername, srs) : 
    layer = data_source.CreateLayer(layername, srs, ogr.wkbPoint)
    
    field_timestamp = ogr.FieldDefn("Timestamp", ogr.OFTString)
    field_timestamp.SetWidth(19)
    layer.CreateField(field_timestamp)
    layer.CreateField(ogr.FieldDefn("Code", ogr.OFTInteger))
    
    return layer

class FireShape (object) : 
    def __init__(self, fire, geo) : 
        self.fire = fire
        self.geo  = geo
        self.driver = ogr.GetDriverByName("ESRI Shapefile")
        self.wgs84  = osr.SpatialReference()
        self.wgs84.ImportFromEPSG(4326) #WGS84
        
    def save_to_layer(self, layer) :
        """given a pre-existing layer, load it up with points from this 
        object's collection.""" 
        con = self.fire.get_conditional()
        geo_points = self.geo.make_list(con)
        vals = self.fire.AfArray[con]
        
        for i in range(len(vals)) :
            feature = ogr.Feature(layer.GetLayerDefn())
            feature.SetField("Code", vals[i])
            
            wkt = "POINT ({0} {1})".format(geo_points[i][1], geo_points[i][0])
            point = ogr.CreateGeometryFromWkt(wkt)
            feature.SetGeometry(point)
            
            layer.CreateFeature(feature)
            
            feature.Destroy()

    def create_output_shapefile(self, filename): 
        data_source = self.driver.CreateDataSource(filename)
        layer_name = os.path.basename(filename).rsplit('.', 1)[0]
        layer = define_layer(data_source, layer_name, self.wgs84)
        
        return data_source, layer
                
        
    def save(self, filename) : 
        
        data_source, layer = self.create_output_shapefile(filename)        
        self.save_to_layer(layer)            
        data_source.Destroy() 

if __name__ == "__main__" :
    if len(sys.argv) != 3 : 
        print "Usage: {0} ini_file shapefile".format(sys.argv[0])
        print "The ini_file is only used for the ImageDates and BaseDir parameters."
        sys.exit()
    config = vc.VIIRSConfig.load(sys.argv[1])  
    
    if len(config.SortedImageDates) > 0 : 
        shapefile = None
        for ImageDate in config.SortedImageDates : 
            # get the file names
            fileset = vt.FileSet.from_imagedate(ImageDate)
            file_names = fileset.get_file_names(config.BaseDir)
            
            # open/load the VIIRS files
            af = vt.ActiveFire375.load(file_names['VF375_npp'])
            geo = vt.GeoFile375.load(file_names['GITCO_npp'])
            
            # combine fire data & geo data
            fire_shape = FireShape(af, geo)
            
            # create shapefile first time around
            if shapefile is None : 
                shapefile, layer=fire_shape.create_output_shapefile(sys.argv[2])
            
            # save this ImageDate's data to the file
            fire_shape.save_to_layer(layer)
            
        #close shapefile
        shapefile.Destroy()
                
            
