import viirs_config as vc
import VIIRS_threshold_reflCor_Bulk as vt
import sys

def refresh(base_dir) : 
    config_list = vc.VIIRSConfig.load_batch(base_dir)

    for c in config_list : 
        vt.output_shape_files(c)



if __name__ == '__main__' : 

    if len(sys.argv) != 2 : 
        print "Usage: {0} base_directory" 
        print "Assuming that base_directory contains many subdirectories"
        print "with runs, new shapefiles are placed into each subdirectory"
        print "representing the current state of the fire_events table."
        sys.exit() 

    refresh(sys.argv[1])
