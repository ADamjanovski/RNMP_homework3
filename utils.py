import pyspark.sql.functions as F
def drop_collumns(data) :
    data = data.drop("Education")
    data = data.drop("Income")
    data = data.drop("NoDocbcCost")
    data = data.drop("CholCheck")
    return data.drop("AnyHealthcare")


def has_consumed_healthy_food(data):
    data = data.withColumn(
    "Consumed_Healthy_Food",
    F.when(
        (F.col("Fruits") == 1) | (F.col("Veggies") == 1), 1
    ).otherwise(0)    
    )
    data= data.drop("Fruits")
    return data.drop("Veggies")   