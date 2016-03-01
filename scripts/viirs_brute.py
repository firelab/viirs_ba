import viirs_config as vc
import VIIRS_threshold_reflCor_Bulk as vt
import sys
import numpy as np
import multiprocessing as mp
import pandas as pd

def create_params(template, cls=vc.SequentialVIIRSConfig) : 
    """creates a list of configuration objects to be used while iterating 
    through the parameter space.
    
    By default, the resultant configuration objects are of the "SequentialVIIRSConfig"
    type, but a different class may be specified via the cls parameter."""
    # for the moment,we're only varying two parameters.
    RthSub_range = np.arange(0.02, 0.11, 0.02) # 0.02 - 0.10 by 0.02: 5 steps
    Rth_range    = np.arange(0.7, 0.91, 0.05) # 0.7-0.9 by 0.05: 5 steps
    
    template_vector = template.get_vector() 
    params = [] 
    for RthSub in RthSub_range : 
        for Rth in Rth_range : 
            i_vec = template_vector._replace(RthSub=RthSub, Rth=Rth)
            params.append( cls.merge_into_template(i_vec, template) )
            
    return params

def make_run_info_table(params) : 
    """creates a table with the run_id and the various parameter settings"""
    table = {} 
    for name in vc.vector_param_names : 
        table[name] = [ getattr(p, name) for p in params ]
    
    table["run_id"] = [ p.run_id for p in params ] 
            
    return pd.DataFrame(table) 
    
    

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print "\nMissing argrument"
        print "\nEnter the template ini file name as an argument when launching this script."
        print "The ini file should be in the current working directory.\n"
        sys.exit()

    # load in the template and create the plan of work
    template_ini = vc.VIIRSConfig.load(sys.argv[1])
    p = create_params(template_ini)    
    
    # save out the plan of work
    run_info_file = "{0}_schema_info.csv".format(template_ini.DBname)
    print "Saving planned run information to {0}.".format(run_info_file) 
    data_table = make_run_info_table(p)
    data_table.to_csv(run_info_file)
    
    # do the work
    workers=12
    print "Running {0} iterations, using {1} parallel workers.".format(len(p), workers)
    mypool = mp.Pool(processes=workers)
    mypool.map(vt.run, p)
        
