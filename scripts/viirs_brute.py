import viirs_config as vc
import VIIRS_threshold_reflCor_Bulk as vt
import sys
import numpy as np
import multiprocessing as mp
import pandas as pd


def inc_idx( idx, maxval) :
    """increments an index vector, where each element can have values 0..maxval-1
    and the entire vector is treated like a counter"""
    i=0
    done = False
    while not done : 
        if i != 0 : 
            idx[i-1]=0
        idx[i] += 1
        done = (idx[i]<maxval) or i==(len(idx)-1)
        i += 1
    if i == len(idx) and not idx[-1] < maxval : 
        idx[-1]=0
        

def csv_vectors(template, table, params=vc.vector_param_names,cls=vc.VIIRSConfig) : 
    """converts the template+vecors in csv format to an array of 
    configuration objects."""

    
    # setup
    ref_vector = template.get_vector()
    nrows = table.shape[0]
    config_list = [ ]

    # loop over all the rows in the table
    for i_row in range(nrows) : 
        row = table.iloc[i_row, :]
        newvals = {} 
        for p in params : 
            if p in vc.int_vector_params : 
                newvals[p] = int(row[p])
            else : 
                newvals[p] = row[p]
        i_vec = ref_vector._replace(**newvals)
        i_cfg = cls.merge_into_template(i_vec, template, runid=int(row["run_id"]))
        config_list.append(i_cfg)

    return config_list

def reflectance_deltas(template, delta, cls=vc.SequentialVIIRSConfig) :
    """ 
    given a reference configuration ("template") and a delta value (reflectance
    units), this function generates all combinations of candidate configurations
    separated from the reference by +/- the delta. 
    
    Only the raw reflectance thresholds are affected: 
        * 'M07UB'
        * 'M08LB'
        * 'M08UB'
        * 'M10LB'
        * 'M11LB'
        
    There are five affected parameters. We are not going to alter M10UB since it
    is set to 1, and therefore was essentially removed from play.
    
    We have a choice as to whether we allow any of the parameters to remain the 
    same as the reference parameter set. 
        * Can be the same as reference: (3**5) - 1 = 242 trials
        * Cannot be the same as reference: (2**5) = 32 trials
        
    """
    raw_refl_params = vc.float_vector_params[:6]
    del raw_refl_params[raw_refl_params.index('M10UB')]
    
    ref_vector = template.get_vector()
    ref_params = np.array([getattr(ref_vector,a) for a in raw_refl_params])
    delta_mult = [-1, 0, 1] # add 0 if you want to let parameters have orig. value.
    config_list = [] 
    dm_idx = np.zeros( (len(raw_refl_params),), dtype=np.int)
    done = False
    while not done : 
        #calculate new value
        p = ref_params + (np.array([delta_mult[i] for i in dm_idx])*delta)
        
        # set params on template
        cur_params = {}
        for i in range(len(raw_refl_params)) : 
            cur_params[raw_refl_params[i]] = p[i]
        i_vec = ref_vector._replace(**cur_params)
            
        # make a new config object
        config_list.append( cls.merge_into_template(i_vec, template) )
           
        #increment index, check for done 
        inc_idx(dm_idx, len(delta_mult))
        done = np.all(dm_idx==0)
    
    return config_list
    
    

def create_params(template, cls=vc.SequentialVIIRSConfig) : 
    """creates a list of configuration objects to be used while iterating 
    through the parameter space.
    
    By default, the resultant configuration objects are of the "SequentialVIIRSConfig"
    type, but a different class may be specified via the cls parameter."""
    # for the moment,we're only varying two parameters.
    RthSub_range = np.arange(0.03, 0.06, 0.01) # 0.03 - 0.05 by 0.01: 3 steps
    Rth_range    = np.arange(0.76, 0.81, 0.01) # 0.76-0.80 by 0.01: 5 steps
    
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
    #table = pd.read_csv(sys.argv[2])
    #p = csv_vectors(template_ini, table)
    p = reflectance_deltas(template_ini, 0.02)    
    
    # save out the plan of work
    run_info_file = "{0}_schema_info.csv".format(template_ini.DBname)
    print "Saving planned run information to {0}.".format(run_info_file) 
    data_table = make_run_info_table(p)
    #print data_table
    data_table.to_csv(run_info_file)
    
    # do the work
    workers=12
    print "Running {0} iterations, using {1} parallel workers.".format(len(p), workers)
    mypool = mp.Pool(processes=workers)
    mypool.map(vt.run, p)
        
