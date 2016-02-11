import glob
import os
import shutil


BaseDir = 'G:/VIIRS_calibration_data'
deleteDir = 'G:/store'
# for SVM07 in glob.glob(os.path.join(BaseDir, "SVM07_npp_d20130917_t2138003_e2149450_b00001_c20150911174317383000_all-_dev.h5")):
prefix = ["AVAFO", "GMTCO", "SVM07", "SVM08", "SVM10", "SVM11"]
count = 0
for pre in prefix:
 for SVM07 in glob.glob(os.path.join(BaseDir, pre + "_npp_d????????_t???????_e???????_b00001_c????????????????????_all-_dev.h5")):
     count = count + 1
     collectionDateTime = os.path.basename(SVM07)[10:28]
     
     files = glob.glob(os.path.join(BaseDir, pre + "_npp_" + collectionDateTime + "_e???????_b00001_c????????????????????_all-_dev.h5"))
     if len(files) > 1:
         if len(files) > 2:
            print '!'*30, '\n',files
            os.system('pause')
         print files[0]
         print files[1]
         print os.path.join(deleteDir, os.path.basename(files[1]))
         print '*'*30
         shutil.move(files[1], os.path.join(deleteDir, os.path.basename(files[1])))
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