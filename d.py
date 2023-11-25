from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import split

sc = SparkContext("local", "FrequentItemSets")
spark = SparkSession(sc)

# Read the input file as a DataFrame
input_file = "d.txt"
df = spark.read.text(input_file)

# Split the space-separated values into an array
df = df.withColumn("items", split(df["value"], " "))

# Set the threshold θ
threshold = 2  

# Create FP-Growth model
fp_growth = FPGrowth(itemsCol="items", minSupport=threshold/df.count(), minConfidence=0.6)
model = fp_growth.fit(df)

# Get frequent itemsets
frequent_itemsets = model.freqItemsets

# Display the result
frequent_itemsets.show(truncate=False)

spark.stop()
