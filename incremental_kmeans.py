from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark import SparkContext
import numpy as np
import math
from pyspark.sql import Row
from os.path import isfile, join
from time import sleep
import os

def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])

def fitsIntoAnyCluster(point, centerPoints, cRadius):
    bestIndex = 0
    closest = float("+inf")
    if len(centerPoints) == 0: #print("No center")
        return -1
    for i in range(len(centerPoints)):
        tempDist = math.sqrt(np.sum((point - centerPoints[i]) ** 2))
        if tempDist < closest and tempDist != 0:
            closest = tempDist
            bestIndex = i
    sim = cRadius[bestIndex] / closest
    if sim > 0.90:
        return [bestIndex,closest]
    else:
        return -1

def fitsInto(point, centerPoints, cRadius):
    bestIndex = 0
    closest = float("+inf")
    if len(centerPoints) == 0: #print("No center")
        return -1
    for i in range(len(centerPoints)):
        tempDist = math.sqrt(np.sum((point - centerPoints[i]) ** 2))
        if tempDist < closest and tempDist != 0:
            closest = tempDist
            bestIndex = i
    sim = cRadius[bestIndex] / closest
    if sim > 0.66:
        return [bestIndex,closest,sim]
    else:
        return -1

def findTwoClosestCentroid(centerPoints):
    minDistance = float("+inf")
    for i in range(len(centerPoints)):
        for j in range(1, len(centerPoints)):
            if i != j:
                tempDist = math.sqrt(np.sum((centerPoints[i] - centerPoints[j]) ** 2))
                if tempDist < minDistance:
                    minDistance = tempDist
                    closestTwo = [i, j]
    return closestTwo

def calculateRadius(centerPoints, table, clusterOfPoints):
    maxRadius = 0
    index = len(centerPoints)-1
    for i in range(len(clusterOfPoints)):
        if clusterOfPoints[i] == len(centerPoints)-1:
            tempDist = math.sqrt(np.sum((table[i] - centerPoints[index]) ** 2))
            if tempDist > maxRadius:
                maxRadius = tempDist
    return maxRadius

INDEX=0
DISTANCE = 1

if __name__ == "__main__":
    #print(len(sys.argv))
    #print(type(sys.argv[1]),sys.argv[1])
    if len(sys.argv) != 3:
        print("Usage: spark-submit <file.py> <maxK> <initialRadiusLength>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.master("local[*]").appName("trainigModule").getOrCreate()
    sqlContext = SQLContext(spark)
    
    struct = StructType(
        [StructField("Session_Duration", FloatType(), True),
        StructField("Bounce_Visit", FloatType(), True),
        StructField("PageView", FloatType(), True),
        StructField("Distinct_PageView", FloatType(), True),
        StructField("Distinct_Category", FloatType(), True),
        StructField("Promo_SpecOffer", FloatType(), True),
        StructField("Class", DoubleType(), True),
        StructField("Label", StringType(), True)]
    )
    
    KMEANS_FEATURES = ['Session_Duration','Bounce_Visit','PageView','Distinct_PageView','Distinct_Category','Promo_SpecOffer','Class']
    LR_FEATURES = ['Session_Duration','Bounce_Visit','PageView','Distinct_PageView','Distinct_Category','Promo_SpecOffer']
    
    fileSET = set(f for f in os.listdir() if isfile(join(os.getcwd(), f)) and "randomsampled.csv" in f)
    while len(fileSET) == 0:
        print("Please put first randomsampled.csv file into $SPARK_HOME")
        sleep(5)
        fileSET = set(f for f in os.listdir() if isfile(join(os.getcwd(), f)) and "randomsampled.csv" in f)
    print("File:",fileSET)
    lines = spark.read.text(os.getcwd()+"/randomsampled.csv").rdd.map(lambda r: r[0])
    lines = lines.map(lambda l: np.array([float(x) for x in l.split(',')[1:] ]))
    lines = lines.collect()

    MAXK = int(sys.argv[1])
    INITRADIUS = int(sys.argv[2])
    print("maxK =",MAXK,"initial radius length of each cluster =",INITRADIUS)
    
    totalCluster = 0
    clusterOfPoints = [] #Noktaların ait olduğu kümelerin indexlerini gösterir.
    cRadius = [] # Kümelerin yarıcaplarını gösterir.
    centerPoints = [] # We have zero cluster center at start!
    index= 0
    print("Initial clusters are being created from benign data")
    for point in lines:
        result = fitsIntoAnyCluster(point, centerPoints, cRadius)
        if result != -1:
            if cRadius[result[INDEX]] < result[DISTANCE]: # Update Cluster Radius
                cRadius[result[INDEX]] = result[DISTANCE]
            clusterOfPoints.append(result[INDEX]) # UpdateCluster()
        else:
            centerPoints.append(point)
            cRadius.append(INITRADIUS)
            clusterOfPoints.append(len(centerPoints)-1)
            totalCluster = totalCluster + 1
            if totalCluster > MAXK:
                #Find two closest centroid and return index [i,j] to closestTwo variable
                closestTwo = findTwoClosestCentroid(centerPoints)
                """print(closestTwo)
                print(clusterOfPoints)
                input()"""
                newCenter = ( centerPoints[closestTwo[0]]+centerPoints[closestTwo[1]] ) / 2
                for k in range(len(clusterOfPoints)):
                    if clusterOfPoints[k] == closestTwo[0] or clusterOfPoints[k] == closestTwo[1]:
                        clusterOfPoints[k] = len(centerPoints)-2
                    else:    
                        if clusterOfPoints[k] > closestTwo[0] and clusterOfPoints[k] > closestTwo[1]:
                            clusterOfPoints[k] -= 2
                        else:
                            if clusterOfPoints[k] > closestTwo[0] and clusterOfPoints[k] < closestTwo[1]:
                                clusterOfPoints[k] -= 1
                centerPoints.pop(closestTwo[0])
                centerPoints.pop(closestTwo[1]-1)
                centerPoints.append(newCenter)
                cRadius.pop(closestTwo[0])
                cRadius.pop(closestTwo[1]-1)
                cRadius.append(calculateRadius(centerPoints,lines, clusterOfPoints))
                totalCluster = totalCluster-1
        index = index + 1
        """print(centerPoints,cRadius)
        print(clusterOfPoints)
        input()"""
    print("Number of initial clusters ->", len(centerPoints))
    #print("clusterofPoints",len(clusterOfPoints))
    initalLines = [x for x in lines]
    os.chdir("output")
    processed = set()
    tmpSET = set()
    fileSET = set()
    while True: ####Traini part yap
        fileSET = set(f for f in os.listdir() if isfile(join(os.getcwd(), f)) and "csv.crc" not in f and "part" in f)
        if fileSET != tmpSET:
            for file in fileSET:
                if file not in processed:
                    with open('full.csv', 'a', encoding="utf-8") as train:
                        print("File:",file)
                        with open(file, 'r', encoding="utf-8") as newFile:
                            for line in newFile:
                                record = line.replace("\n","").replace('"','').split(",")#Optimization
                                train.write("%s,%s,%s,%s,%s,%s,%s,%s\n" % (int(record[0]),float(record[1]),int(record[2]),int(record[3]),int(record[4]),int(record[5]),int(record[6]),int(record[7]) ) )
                    processed.add(file)
            #df = spark.read.format("csv").option("header","false").schema(struct).load("output/full.csv")
            lines = spark.read.text(os.getcwd()+"/full.csv").rdd.map(lambda r: r[0])
            if lines.count() > 0:
                print("in incremental clustering...")
                lines = lines.map(lambda l: np.array([float(x) for x in l.split(',')[1:] ]))
                lines = lines.collect()
                lines = initalLines + lines
                with open("labeledA.csv","a",encoding="utf-8") as fp:
                    with open("labeledN.csv","a",encoding="utf-8") as fp1:
                        for i in range(index,len(lines)): # for each data point n do:
                            #print(len(lines), len(clusterOfPoints))
                            result = fitsInto(lines[i], centerPoints, cRadius)
                            t = lines[i]
                            if result != -1:
                                if result[2] > 0.90:
                                    if cRadius[result[INDEX]] < result[DISTANCE]: # Update Cluster Radius
                                        cRadius[result[INDEX]] = result[DISTANCE]
                                    clusterOfPoints.append(result[INDEX]) # UpdateCluster()
                                else:
                                    centerPoints.append(lines[i])
                                    cRadius.append(INITRADIUS)
                                    clusterOfPoints.append(len(centerPoints)-1)
                                    totalCluster = totalCluster + 1
                                    if totalCluster > MAXK:
                                        closestTwo = findTwoClosestCentroid(centerPoints)
                                        newCenter = (centerPoints[closestTwo[0]] + centerPoints[closestTwo[1]]) / 2
                                        for k in range(len(clusterOfPoints)):
                                            if clusterOfPoints[k] == closestTwo[0] or clusterOfPoints[k] == closestTwo[1]:
                                                clusterOfPoints[k] = len(centerPoints)-2
                                            else:
                                                if clusterOfPoints[k] > closestTwo[0] and clusterOfPoints[k] > closestTwo[1]:
                                                    clusterOfPoints[k] -= 2
                                                else:
                                                    if clusterOfPoints[k] > closestTwo[0] and clusterOfPoints[k] < closestTwo[1]:
                                                        clusterOfPoints[k] -= 1
                                        centerPoints.pop(closestTwo[0])
                                        centerPoints.pop(closestTwo[1]-1)
                                        centerPoints.append(newCenter)
                                        cRadius.pop(closestTwo[0])
                                        cRadius.pop(closestTwo[1]-1)
                                        maxRadius = 0
                                        lastIndex = len(centerPoints)-1
                                        for j in range(len(clusterOfPoints)):
                                            if clusterOfPoints[j] == lastIndex:
                                                tempDist = math.sqrt(np.sum((lines[j] - centerPoints[lastIndex]) ** 2))
                                                if tempDist > maxRadius:
                                                    maxRadius = tempDist
                                        cRadius.append(maxRadius)
                                        totalCluster = totalCluster-1
                                label="N"
                                fp1.write("%s,%s,%s,%s,%s,%s,%s,%s\n" % (t[0],t[1],t[2],t[3],t[4],t[5],t[6],label) )
                            else:
                                label="A"
                                fp.write("%s,%s,%s,%s,%s,%s,%s,%s\n" % (t[0],t[1],t[2],t[3],t[4],t[5],t[6],label) )
                            index = index + 1
                        """end for loop -> for point in table: """
                
                print("points labeled as 'A' and 'N' ")
                #spark.stop()
                print("in logistic regression...")
                #Logistic Regression
                df = spark.read.format("csv").option("header","false").schema(struct).load("output/labeledN.csv")
                ###df.registerTempTable("census")
                ###tmp = sqlContext.sql("SELECT * from census WHERE Label = 'N'")
                #tmp = tmp.select(LR_FEATURES)
                vecAssembler = VectorAssembler(inputCols=LR_FEATURES, outputCol="features")
                lr_df = vecAssembler.transform(df).select('Class','features')
                lr = LogisticRegression(featuresCol='features', labelCol='Class', maxIter=10)
                lrModel = lr.fit(lr_df)
                lr_path = os.getcwd() + "/lr"
                lrModel_path = os.getcwd() + "/lr_model"
                lr.write().overwrite().save(lr_path)
                lrModel.write().overwrite().save(lrModel_path)
            print("pipelined training process completed")
        else: # if fileSET == tmpSET, there is no change in stream files.
            print("No incoming stream...")
        tmpSET = set(fileSET)
        print("Number of current clusters->",len(centerPoints))
        sleep(10)

    spark.stop()


