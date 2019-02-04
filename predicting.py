"""
spark-submit predicting.py
"""
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext
from pyspark.sql.types import StructField, StructType
from pyspark.sql import Row
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from time import sleep
from pyspark.sql.types import *
import numpy as np
import time
import os
import shutil

PERIOD = 120

spark = SparkSession.builder.master("local[*]").appName("predictingModule").getOrCreate()
sqlContext = SQLContext(spark)

LR_FEATURES = ['Session_Duration','Bounce_Visit','PageView','Distinct_PageView','Distinct_Category','Promo_SpecOffer']

struct = StructType(
    [StructField("Session_Duration", FloatType(), True),
    StructField("Bounce_Visit", FloatType(), True),
    StructField("PageView", FloatType(), True),
    StructField("Distinct_PageView", FloatType(), True),
    StructField("Distinct_Category", FloatType(), True),
    StructField("Promo_SpecOffer", FloatType(), True)]
)

modelFiles = set()
while ("lr" not in modelFiles) or ("lr_model" not in modelFiles):
    if os.path.exists("output"):
        tmp = os.getcwd()
        os.chdir("output")
        for i in os.listdir():
            if os.path.isdir(i):
                modelFiles.add(i)
        os.chdir(tmp)
    if ("lr" not in modelFiles) or ("lr_model" not in modelFiles):
        print("Could'nt find Model, system is waiting...")
    sleep(5)

lr_path = os.getcwd() + "/output/lr"
lrModel_path = os.getcwd() + "/output/lr_model"
lr = LogisticRegression.load(lr_path)
lrModel = LogisticRegressionModel.load(lrModel_path)
#shutil.copy2(os.getcwd()+'/randomsampled.csv', os.getcwd()+'/output')
lr_path = os.getcwd() + "/output/_spark_metadata/lr"
lrModel_path = os.getcwd() + "/output/_spark_metadata/lr_model"
lr.write().overwrite().save(lr_path)
lrModel.write().overwrite().save(lrModel_path)
lr_path = os.getcwd() + "/output/lr"
lrModel_path = os.getcwd() + "/output/lr_model"
lr = LogisticRegression.load(lr_path)
lrModel = LogisticRegressionModel.load(lrModel_path)

startTime = time.time()
with open('test.csv', 'r', encoding="utf-8") as predictCSV:
    for line in predictCSV:
        record = predictCSV.readline().replace("\n","").split(",")
        record = np.array(record).astype(float)
        correctAnswer = record[-1]
        data = record[1:-1].tolist()
        test = sqlContext.createDataFrame([(data[0],data[1],data[2],data[3],data[4],data[5],)], struct)
        vecAssembler = VectorAssembler(inputCols=LR_FEATURES, outputCol="features")
        test = vecAssembler.transform(test).select('features')
        result = lrModel.transform(test).head()
        if result.prediction == 0.0:
            print(result.probability,"prediction->",result.prediction,"=NO, Actual Class->",correctAnswer)
        else:
            print(result.probability,"prediction->",result.prediction,"=YES, Actual Class->",correctAnswer)
        with open("results.txt", "a", encoding="utf-8") as fp:
            fp.write("%s,%s,%s\n" % (correctAnswer, result.prediction, result.probability ))

        sleep(1)
        deltaTime = time.time()-startTime
        if deltaTime > PERIOD:
            print("Model is being updated")
            lr_path = os.getcwd() + "/output/lr"
            lrModel_path = os.getcwd() + "/output/lr_model"
            lr = LogisticRegression.load(lr_path)
            lrModel = LogisticRegressionModel.load(lrModel_path)
            lr_path = os.getcwd() + "/output/_spark_metadata/lr"
            lrModel_path = os.getcwd() + "/output/_spark_metadata/lr_model"
            lr.write().overwrite().save(lr_path)
            lrModel.write().overwrite().save(lrModel_path)
            lr_path = os.getcwd() + "/output/lr"
            lrModel_path = os.getcwd() + "/output/lr_model"
            lr = LogisticRegression.load(lr_path)
            lrModel = LogisticRegressionModel.load(lrModel_path)
            startTime=time.time()

spark.stop()
