import unittest
import os.path
import ConfigParser
import StringIO
import viirs_config as vc
import numpy as np

def copy_config_parser(ini) :
    ini_string = StringIO.StringIO()
    ini.write(ini_string)
    
    ini_string.seek(0)
    new_config = ConfigParser.ConfigParser()
    new_config.readfp(ini_string)
    return new_config

class TestVIIRSConfig (unittest.TestCase) : 
    attrs_to_copy = ['BaseDir','use375af','use750af','M07UB','M08UB','M08LB',
                     'M10LB','M10UB','M11LB','RthSub','Rth','RthLB','MaxSolZen',
                     'TemporalProximity', 'SpatialProximity','TextOut','ShapeOut',
                     'DatabaseOut','ShapePath','PostBin','ImageDates','DBname',
                     'DBuser','pwd', 'DBhost', 'DBschema']
    def setUp(self) :
        # a clearly fake configuration which has unique values for each parameter 
        self.BaseDir = "TESTING" 
        self.use375af = 'y'
        self.use750af = 'n'
        self.M07UB = 1.6
        self.M08UB =1.8
        self.M08LB = 1.5
        self.M10LB = 44.232
        self.M10UB = 734.2
        self.M11LB = 22.2
        self.RthSub = 0.223311
        self.Rth = 55.66345
        self.RthLB = 994322.3222
        self.MaxSolZen=180.
        
        self.TemporalProximity = 2
        self.SpatialProximity=6000
        
        self.TextOut= 'x'
        self.ShapeOut='z'
        self.DatabaseOut='a'
        self.ShapePath='totally fake'
        self.PostBin='not on this machine, you dont'
        
        self.ImageDates = ['boo','hoo','too']
        self.DBname = 'no data here'
        self.DBuser = 'happy gilmore'
        self.DBhost = 'all your data are belong to me'
        self.DBschema = 'data go here. now.'
        self.pwd = 'mine all mine'
        
         
        ini = ConfigParser.ConfigParser()
        ini.add_section("InDirectory")
        ini.set("InDirectory", "BaseDirectory", self.BaseDir)

        ini.add_section("ActiveFire")
        ini.set("ActiveFire", "use375af",self.use375af.lower())              # Flag to use M-band 750 m active fire data, AVAFO (y or n)  
        ini.set("ActiveFire", "use750af",self.use750af.lower())              # Flag to use I-band 375 m active fire data, VF375 (y or n)

        ini.add_section("Thresholds")
        fltfmt = '{:4.2f}'
        ini.set("Thresholds", "M07UB", fltfmt.format(self.M07UB))     #Band 07 (0.86 um)upper bound
        ini.set("Thresholds", "M08LB", fltfmt.format(self.M08LB))     #Band 08 (1.24 um)lower bound
        ini.set("Thresholds", "M08UB", fltfmt.format(self.M08UB))     #Band 08 (1.24 um)upper bound
        ini.set("Thresholds", "M10LB", fltfmt.format(self.M10LB))     #Band 10 (1.61 um)lower bound
        ini.set("Thresholds", "M10UB", fltfmt.format(self.M10UB))     #Band 10 (1.61 um)upper bound
        ini.set("Thresholds", "M11LB", fltfmt.format(self.M11LB))     #Band 11 (2.25 um)lower bound
        ini.set("Thresholds", "RthSub", fltfmt.format(self.RthSub))   #RthSub is the factor subtracted from the 1.240 band when comparing to the Rth
        ini.set("Thresholds", "Rth", fltfmt.format(self.Rth))         #Rth
        ini.set("Thresholds", "RthLB", fltfmt.format(self.RthLB))     #RthLB is the factor that the Rth check must be greater than or equal to
        ini.set("Thresholds", "MaxSolZen",fltfmt.format(self.MaxSolZen)) #Maximum solar zenith angle, used to filter out night pixels from burned area thresholding 

        ini.add_section("ConfirmBurnParameters")
        ini.set("ConfirmBurnParameters", "TemporalProximity", 
                   '{:d}'.format(self.TemporalProximity))
        ini.set("ConfirmBurnParameters", "SpatialProximity", 
                   '{:d}'.format(self.SpatialProximity))

        ini.add_section("OutputFlags")
        ini.set("OutputFlags", "TextFile", self.TextOut.lower())
        ini.set("OutputFlags", "ShapeFile", self.ShapeOut.lower())
        ini.set("OutputFlags", "PostGIS", self.DatabaseOut.lower())
        ini.set("OutputFlags", "OutShapeDir", self.ShapePath.lower())
        ini.set("OutputFlags", "PostgresqlBin",self.PostBin.lower())
        
        ini.add_section("ImageDates")
        ini.set("ImageDates", "ImageDates", ','.join(self.ImageDates))
        
        ini.add_section("DataBaseInfo")
        ini.set("DataBaseInfo", "DataBaseName", self.DBname)
        ini.set("DataBaseInfo", "UserName", self.DBuser)
        ini.set("DataBaseInfo", "password", self.pwd)
        ini.set("DataBaseInfo", "Schema", self.DBschema)
        
        self.no_host = copy_config_parser(ini)
        
        ini.set("DataBaseInfo","Host", self.DBhost)
        
        self.ini = ini

        config = vc.VIIRSConfig() 
        for n in self.attrs_to_copy : 
            setattr(config,n,getattr(self,n))
        config.run_id = vc.VIIRSConfig.create_run_id(config)
        self.config = config
            
        config = vc.VIIRSConfig() 
        for n in self.attrs_to_copy : 
            setattr(config,n,getattr(self,n))
        config.DBhost = None
        config.run_id = vc.VIIRSConfig.create_run_id(config)
        self.no_host_config = config
        
    def test_get_vector(self) : 
        """ensures that the returned vector contains the values we'd expect"""
        vec = self.config.get_vector()
        
        for attr in vc.float_vector_params : 
            self.assertEqual(float(getattr(self, attr)), getattr(vec, attr))
            
        for attr in vc.int_vector_params : 
            self.assertEqual(int(getattr(self, attr)), getattr(vec, attr))
            
    def test_hash_run_id(self) : 
        """exercises the run_id maker of a VIIRSConfig object"""
        run_id = vc.VIIRSConfig.create_run_id(self.config)
        self.assertEqual(run_id, hash(self.config))
        
    def test_sequence_run_id(self) : 
        """exercises the run_id maker of a SequentialVIIRSConfig object"""
        bias = None
        for i in range(5) : 
            run_id = vc.SequentialVIIRSConfig.create_run_id(self.config)
            if bias is None : 
                bias = run_id
            else : 
                self.assertEqual(run_id, bias+i)
            
    def test_get_ini_obj(self) : 
        """exercises the ini object view of the configuration"""
        test_ini = self.config.get_ini_obj()
        
        # check that we have the same sections
        self.assertEqual(test_ini.sections(), self.ini.sections())
        
        # check that each section has the same items
        sections = test_ini.sections() 
        for s in sections : 
            test_items = [ i for i,v in test_ini.items(s) ] 
            ref_items  = [ i for i,v in self.ini.items(s) ] 
            for ti in test_items : 
                self.assertTrue(ti in ref_items)
            
        # check that each item in each section has the same value
        for s in sections : 
            items = test_ini.items(s)
            for i,v in items : 
                self.assertEqual(test_ini.get(s,i), self.ini.get(s,i))
            
    def test_get_ini_obj_no_host(self) : 
        """exercises the ini object view of the configuration"""
        test_ini = self.no_host_config.get_ini_obj()
        
        # check that we have the same sections
        self.assertEqual(test_ini.sections(), self.no_host.sections())
        
        # check that each section has the same items
        sections = test_ini.sections() 
        for s in sections : 
            test_items = [ i for i,v in test_ini.items(s) ] 
            ref_items  = [ i for i,v in self.no_host.items(s) ] 
            for ti in test_items : 
                self.assertTrue(ti in ref_items)
            
        # check that each item in each section has the same value
        for s in sections : 
            items = test_ini.items(s)
            for i,v in items : 
                self.assertEqual(test_ini.get(s,i), self.no_host.get(s,i))
        
    def test_merge(self) : 
        """exercises the merge of a config object with a new vector of parameters"""
        test_vec = vc.ConfigVector(*(np.array(self.config.get_vector()) + 1))
        m = vc.VIIRSConfig.merge_into_template(test_vec, self.config)
        
        # check all the non-vector items were copied
        non_vector = list(self.attrs_to_copy)
        for vec_item in vc.vector_param_names : 
            non_vector.remove(vec_item)
        non_vector.remove('ShapePath') # expect that the shapefile path is different
        non_vector.remove('DBschema') # expect that the schema is different
            
        for nonvec_item in non_vector : 
            self.assertEqual(getattr(self.config, nonvec_item), getattr(m,nonvec_item))
            
        # check that the vector parameters came from the supplied vector
        for vec_item in vc.vector_param_names : 
            self.assertEqual(getattr(test_vec, vec_item), getattr(m,vec_item))

    def test_merge_no_host(self) : 
        """exercises the merge of a config object with a new vector of parameters"""
        test_vec = vc.ConfigVector(*(np.array(self.no_host_config.get_vector()) + 1))
        m = vc.VIIRSConfig.merge_into_template(test_vec, self.no_host_config)
        
        # check all the non-vector items were copied
        non_vector = list(self.attrs_to_copy)
        for vec_item in vc.vector_param_names : 
            non_vector.remove(vec_item)
        non_vector.remove('ShapePath') # expect that the shapefile path is different
        non_vector.remove('DBschema') # expect that the schema is different
            
        for nonvec_item in non_vector : 
            self.assertEqual(getattr(self.no_host_config, nonvec_item), getattr(m,nonvec_item))
            
        # check that the vector parameters came from the supplied vector
        for vec_item in vc.vector_param_names : 
            self.assertEqual(getattr(test_vec, vec_item), getattr(m,vec_item))
            
            
    def test_merge_subclass(self) : 
        """verifies that subclass can override the type of object returned"""
        test_vec = vc.ConfigVector(*(np.array(self.config.get_vector()) + 1))
        m = vc.SequentialVIIRSConfig.merge_into_template(test_vec, self.config)
        
        self.assertEqual(type(m), vc.SequentialVIIRSConfig)
        self.assertTrue(m.run_id < 10) # note this accumulates over all the tests,
                                    # we're just trying to make sure its not a hash
        
        # check all the non-vector items were copied
        non_vector = list(self.attrs_to_copy)
        for vec_item in vc.vector_param_names : 
            non_vector.remove(vec_item)
        non_vector.remove('ShapePath') # expect that the shapefile path is different
        non_vector.remove('DBschema') # expect that the schema is different
            
        for nonvec_item in non_vector : 
            self.assertEqual(getattr(self.config, nonvec_item), getattr(m,nonvec_item))
            
        # check that the vector parameters came from the supplied vector
        for vec_item in vc.vector_param_names : 
            self.assertEqual(getattr(test_vec, vec_item), getattr(m,vec_item))
            
    def test_perturbed_dir(self) : 
        base_dir = '/hey/there/Ima/path'
        new_dir  = self.config.perturb_dir(base_dir)
        self.assertEqual(os.path.join('/hey/there/Ima','Run_{0}'.format(self.config.run_id)),new_dir)
                