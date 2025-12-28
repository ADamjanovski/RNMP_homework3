from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
import joblib
from pyspark import Row
from utils import *
import json


model_info = joblib.load("best_model_with_features.joblib")
model = model_info["model"]
feature_columns=model_info["feature_columns"]
data_sample= '{"HighBP": 1.0, "HighChol": 0.0, "CholCheck": 1.0, "BMI": 30.0, "Smoker": 1.0, "Stroke": 0.0, "HeartDiseaseorAttack": 1.0, "PhysActivity": 1.0, "Fruits": 1.0, "Veggies": 1.0, "HvyAlcoholConsump": 0.0, "AnyHealthcare": 1.0, "NoDocbcCost": 0.0, "GenHlth": 4.0, "MentHlth": 0.0, "PhysHlth": 0.0, "DiffWalk": 1.0, "Sex": 1.0, "Age": 9.0, "Education": 6.0, "Income": 3.0}'

def row_to_json(row: Row) -> str:
    return json.dumps(row.asDict(), default=str)

row_to_json_udf = F.udf(row_to_json)

def process_batch(df, epoch_id):
    
    if df.count() > 0:
        df = df.withColumn(
            "value",
            F.col("value").cast(StringType())
          ).withColumn(
            "value",
            F.from_json(
                F.col("value"),
                F.schema_of_json(data_sample)
            )
        ).select("value.*")

        new_df = df.select(*feature_columns)
        new_df.printSchema()
        new_df = drop_collumns(new_df)
        new_df = has_consumed_healthy_food(new_df)

        model_features = new_df.columns
        pandas_df = new_df.toPandas()
        predictions = model.predict(pandas_df[model_features].values)
        pandas_df['predicted_diabetes'] = predictions
        new_df = spark.createDataFrame(pandas_df)

        result = new_df.withColumn("value", row_to_json_udf(F.struct(*new_df.columns)))
        print(result.show())
        result.select('value') \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("topic", "health_data_predicted") \
                .save()

spark = SparkSession.builder \
    .appName("KafkaConsumerExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "health_data") \
  .option("startingOffsets", "latest") \
  .load()





query = df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()


query.awaitTermination()