import replay_confirmation as rc
import run_fom as rf 

def both(database) : 
    rc.reconfirm_batch('.', 12) ; 
    rf.all_fom(database, 12)
