from utils import *
from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import f1_score
import numpy as np

import joblib


spark = SparkSession.builder \
    .appName("MovieLens") \
    .getOrCreate()
    # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \

data = spark.read\
    .option("delimiter", ",")\
    .option("header", True)\
    .csv("diabetes_binary_health_indicators_BRFSS2015.csv", inferSchema=True)

feature_columns = data.columns
data = drop_collumns(data)
data = has_consumed_healthy_food(data)

labels= data.select("Diabetes_binary")
data = data.drop("Diabetes_binary")
feature_columns.remove("Diabetes_binary")
data_list = data.toPandas().values.tolist()
labels_list = labels.toPandas().values.tolist()

labels_list= np.ravel(labels_list)
logisticReg = LogisticRegression(max_iter=300)
mlpClass= MLPClassifier(hidden_layer_sizes=(50,))
decisionTree = DecisionTreeClassifier()
offline, onlineData, offlineLabels, onlineLabels = train_test_split(data_list, labels_list, test_size=0.2, random_state=42)


X_train , X_test, y_train, y_test = train_test_split(offline,offlineLabels,test_size=0.2,random_state=42)
logisticReg.fit(X_train,y_train)
mlpClass.fit(X_train,y_train)
decisionTree.fit(X_train,y_train)



logisticRes= logisticReg.predict(X_test)
mlpRes= mlpClass.predict(X_test)
decisionTreeRes=decisionTree.predict(X_test)

logisticF1=f1_score(y_test,logisticRes)
mlpF1= f1_score(y_test,mlpRes)
decisionTreeF1= f1_score(y_test,decisionTreeRes)


names= ["LogisticReg","MLP","DecisionTree"]
scores=[logisticF1,mlpF1,decisionTreeF1]
models=[logisticReg,mlpClass,decisionTree]

best_index=np.argmax(scores)

print(scores)
print(names[best_index])
print(feature_columns)
joblib.dump({
    'model': models[best_index],
    'feature_columns': feature_columns,
}, "best_model_with_features.joblib")
spark.stop()