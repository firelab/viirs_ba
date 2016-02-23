import glob
import os
import shutil


BaseDir = 'G:/VIIRS_calibration_data'
# deleteDir = 'G:/store'
# for SVM07 in glob.glob(os.path.join(BaseDir, "SVM07_npp_d20130917_t2138003_e2149450_b00001_c20150911174317383000_all-_dev.h5")):
# prefix = ["AVAFO", "GMTCO", "SVM07", "SVM08", "SVM10", "SVM11"]
prefix = ["VF375", "AVAFO", "GITCO", "GMTCO", "SVM10", "SVM11", "SVM08"]
count = 0
for SVM07 in glob.glob(os.path.join(BaseDir, "SVM07_npp_d????????_t???????_e???????_b00001_c????????????????????_all-_dev.h5")):
    # print SVM07
    count = count + 1
    collectionDateTime = os.path.basename(SVM07)[10:28]
    for pre in prefix:
        files = glob.glob(os.path.join(BaseDir, pre + "_npp_" + collectionDateTime + "_e???????_b00001_c????????????????????_all-_dev.*"))
        # print files
        if len(files) == 0:
            print "Not Found", pre, collectionDateTime
            
    #print collectionDateTime
    # s8 = "SMV08_npp_" + collectionDateTime + "_e???????_b00001_c????????????????????_all-_dev.h5"
    # print s8
    #print "*"*50
    # for svm08 in glob.glob(os.path.join(BaseDir, "SVM08_npp_" + collectionDateTime + "_e???????_b00001_c????????????????????_all-_dev.h5")):
        # print "here"
        # print svm08
        # print os.path.exists(svm08)
    # svm08 = glob.glob(os.path.join(BaseDir, "SVM08_npp_" + collectionDateTime + "_e???????_b00001_c????????????????????_all-_dev.h5"))[0]
    # print "here"
    # print svm08
    # print os.path.exists(svm08)
# print "found", count, "SVM07 files"    
