from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys
import os
from collections import namedtuple
from pyspark.sql.functions import *
from pyspark.sql.types import *
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\Mohammad\.jdks\ms-17.0.17'
conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
spark.read.format("csv").load("data/test.txt").toDF("Success").show(20,False)

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

text = [
    "hello spark",
    "hello hadoop",
    "hello spark"]

dft = sc.parallelize(text)
print(dft.collect())
words = dft.flatMap(lambda x: x.split(" "))
print(words.collect())
pairs = words.map(lambda x: (x, 1))
print(pairs.collect())
word_count = pairs.reduceByKey(lambda a, b: a + b)

print(word_count.collect())

# data = [
#     (1, "HR", 50000),
#     (2, "HR", 60000),
#     (3, "HR", 55000),
#     (4, "IT", 70000),
#     (5, "IT", 90000),
#     (6, "IT", 80000)
# ]
#
# columns = ["emp_id", "dept", "salary"]
# df = spark.createDataFrame(data, columns)
#
# window_spec = Window.partitionBy(col("dept")).orderBy(col("salary").desc())
#
# df_ranked = df.withColumn("rn", row_number().over(window_spec))
#
# df_ranked.show()
#
# second_highest = df_ranked.filter(col("rn") ==2)
#
# second_highest.show()







#
# from pyspark.sql import SparkSession
# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number, col
#
# # Create Spark session
# spark = SparkSession.builder.appName("SecondHighestSalary").getOrCreate()
#

#
# hf=["A~B","C~D"]
# print(hf)
# print("Raw data")
# dft=sc.parallelize(hf)
# print(dft.collect())
# print("RDD data")
# print()
# fhg=dft.map(lambda x:x.split("~"))
# print(fhg.collect())
# print()
# trh=dft.flatMap(lambda x:x.split("~"))
# print(trh.collect())

#
# # Sample data
# data = [
#     (1, "HR", 50000),
#     (2, "HR", 60000),
#     (3, "HR", 55000),
#     (4, "IT", 70000),
#     (5, "IT", 90000),
#     (6, "IT", 80000),
#     (7, "Finance", 65000),
#     (8, "Finance", 75000),
#     (9, "Finance", 72000)
# ]
#
# # Create DataFrame
# columns = ["emp_id", "dept", "salary"]
# df = spark.createDataFrame(data, columns)
#
# df.show()
#
# from pyspark.sql.window import Window
#
# window_spec = Window.partitionBy(col("dept")).orderBy(col("salary").desc())
# dense_v = df.withColumn("dr",dense_rank().over(window_spec))
# dense_v.show()
# second_highest = dense_v.filter(col("dr") ==2)
# second_highest.show()



#
# # Sample data
# data = [
#     (1, "Alice", None),
#     (2, None, "Bobby"),
#     (3, None, None)
# ]
#
# # Define columns
# columns = ["id", "name", "nickname"]
#
# # Create DataFrame
# df = spark.createDataFrame(data, columns)
#
# # Show input data
# df.show()
#
# # Apply transformation
# # df.select(col("id"),coalesce(col("name"), col("nickname")).alias("final_name")).show()
#
# GH=df.filter(col("name").isNull()).alias("none").show()
#
#
#
# af= [(1, 'Veg Biryani'),
#      (2, 'Veg Fried Rice'),
#      (3, 'Kaju Fried Rice'),
#      (4, 'Chicken Biryani'),
#      (5, 'Chicken Dum Biryani'),
#      (6, 'Prawns Biryani'),
#      (7, 'Fish Birayani')]
#
# Food = spark.createDataFrame(af,["Food_id","food_item"])
# Food.createOrReplaceTempView("Food")
# Food.show()
#
#
# bf= [(1, 5),
#      (2, 3),
#      (3, 4),
#      (4, 4),
#      (5, 5),
#      (6, 4),
#      (7, 4)]
#
# Eat = spark.createDataFrame(bf,["Food_id","rating"])
# Eat.createOrReplaceTempView("Eat")
# Eat.show()
#
# joiundf = spark.sql("Select a.*,b.rating from Food a left join Eat b on a.Food_id = b.Food_id")
# joiundf.show()
#
# hgrate=joiundf.withColumn("starrating",expr("repeat('*',rating)"))
# hgrate.show()
#
# #stark= joiundf.withColumn("stars",expr("repeat('*',rating)"))
#
# #ghugh=hgrate.select("Food_id","food_item","starrating")
# #ghugh.show()
#
# ghugh = hgrate.select("Food_id", "food_item", "starrating")
# ghugh.show()
# iuj=ghugh.limit(5).show()

# ghugh_numeric = ghugh.withColumn("star_number",length(col("starrating")))
# ghugh_numeric.show()
#
# ghugh_digt=ghugh_numeric.withColumn("food_number", length(col("food_item")))
# ghugh_digt.show()
#
# jukj = ghugh.withColumn(
#     "food_item",
#     regexp_replace(col("food_item"), " ", ""))
# jukj.show()
#
# fugh= ghugh.withColumn(
#     "first_word",
#     split(col("food_item"), " ")[4]
# )
# fugh.show()
#
# from pyspark.sql.window import Window
#
#
# datal = [
#     (1111, '2021-01-15', 10),
#     (1111, '2021-01-16', 15),
#     (1111, '2021-01-17', 30),
#     (1112, '2021-01-15', 10),
#     (1112, '2021-01-15', 20),
#     (1112, '2021-01-15', 30)
# ]
#
# datall = spark.createDataFrame(datal, ["sensorid", "timestamp", "values"])
#
# # Define window specification
# windowSpec = Window.partitionBy("sensorid").orderBy("timestamp")
#
# # Calculate difference
# result = datall.withColumn(
#     "prev_value",
#     lag("values").over(windowSpec)
# ).withColumn(
#     "diff",
#     col("values") - col("prev_value")
# ).drop("prev_value").filter(col("diff").isNotNull())
#
# result.show()