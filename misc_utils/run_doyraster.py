#
# Script to run all of the figure of merit code for a single run back to back.
#
import viirs_config as vc
import viirs_doy as vdoy
import sys
import multiprocessing as mp

def rasterize_batch(base_dir, workers=1) : 
    """re-computes fire_events and fire_collections for every run in this batch"""
    config_list = vc.VIIRSConfig.load_batch(base_dir)
    
    if workers > 1 : 
        mypool = mp.Pool(processes=workers)
        mypool.map(vdoy.rasterize, config_list)
    else : 
        for c in config_list : 
            vdoy.rasterize(c)
    

if __name__ == '__main__' : 
    if len(sys.argv) not in [2,3] : 
        print "Usage: {0} base_directory [workers]".format(sys.argv[0])
        sys.exit() 

    if len(sys.argv) == 3 : 
        rasterize_batch(sys.argv[1],int(sys.argv[2]))
    else : 
        rasterize_batch(sys.argv[1])

