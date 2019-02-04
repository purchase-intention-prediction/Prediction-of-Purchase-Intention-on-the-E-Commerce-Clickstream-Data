#"SessionID,Session_Duration,Bounce_Visit,PageView,Distinct_PageView,Distinct_Category,Promo_SpecOffer,Class\n"
from random import randint
import os
import shutil

with open('allAttributes.csv','r',encoding="utf-8") as fp:
    fp.readline()
    count = 0
    for line in fp:
        count = count + 1
print("num of records", count)
percent = int(count / 100)
print("1 percent", percent)



with open('allAttributes.csv','r',encoding="utf-8") as fp:
    with open('remainAttributes.csv','w',encoding="utf-8") as fp1:
        with open('randomsampled.csv','w',encoding="utf-8") as fp2:
            fp1.write(fp.readline())
            for i in range(0,percent):
                random = randint(0,99)
                rest = 99 - random
                for j in range(0,random):
                    fp1.write(fp.readline())
                
                temp = fp.readline()
                record = temp.replace("\n","").split(",")
                anomaly = 0 # NO
                minPerPage = float(record[1]) / int(record[3])
                #if minute per page is less from bounce visit 
                if minPerPage < 0.33:
                    anomaly = 1
                #if has bounce visit and has bought
                if record[2] == "1":
                    anomaly = 1
                #if min per session 
                if float(record[1]) > 100.0:
                    anomaly = 1
                if anomaly == 0:
                    fp2.write(temp)

                for j in range(0,rest):
                    fp1.write(fp.readline())
    shutil.copy2(os.getcwd()+'/randomsampled.csv', os.getcwd()+'/output')
    
                




