# %%
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, stddev
from pyspark.sql.types import DoubleType 
import pandas

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Record Linkage") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# %% read CSV and show DataFrame
#df = spark.read.csv("data/block*.csv")
#print(df.show(5))

# %% inferSchema
parsed = spark.read.option("header", "true").option("nullValue", "?")\
.option("inferSchema", "true").csv("data/block*.csv")

parsed.printSchema()
print(parsed.show(5))
print(parsed.count())

# %%
parsed.cache()

# %% Aggregation functions
parsed.groupBy("is_match").count().orderBy(col("count").desc()).show()
parsed.agg(avg("cmp_sex"), stddev("cmp_sex")).show()

# using SQL
parsed.createOrReplaceTempView("linkage")
spark.sql(""" SELECT is_match, COUNT(*) cnt FROM linkage 
          GROUP BY is_match ORDER BY cnt DESC """).show()


# %% summary statistics
summary = parsed.describe()
summary.show()
summary.select("summary", "cmp_fname_c1", "cmp_fname_c2").show()

# less than 2% of the records have a non-null value for cmp_fname_c2


# filter sql-style
matches = parsed.where("is_match = true")
match_summary = matches.describe()

#filter Column DataFrame API
misses = parsed.filter(col("is_match") == False)
miss_summary = misses.describe()


# Pivoting and Reshaping DataFrames
summary_p = summary.toPandas()

summary_p.head()
summary_p.shape

# transpose
summary_p = summary_p.set_index('summary').transpose().reset_index()
summary_p = summary_p.rename(columns={'index':'field'})
summary_p = summary_p.rename_axis(None, axis=1)
summary_p.shape

summaryT = spark.createDataFrame(summary_p)
summaryT.show()

# all fields are string type
summaryT.printSchema()

for c in summaryT.columns: 
    if c == 'field': 
        continue  
    summaryT = summaryT.withColumn(c, summaryT[c].cast(DoubleType()))
    
summaryT.printSchema()

#function to transpose and convert fields to Double
def pivot_summary(desc):  
    desc_p = desc.toPandas()
    desc_p = desc_p.set_index('summary').transpose().reset_index()
    desc_p = desc_p.rename(columns={'index':'field'})
    desc_p = desc_p.rename_axis(None, axis=1) 
    descT = spark.createDataFrame(desc_p)
    for c in descT.columns: 
        if c == 'field': 
            continue 
        else:
            descT = descT.withColumn(c, descT[c].cast(DoubleType())) 
    return descT

match_summaryT = pivot_summary(match_summary)
miss_summaryT = pivot_summary(miss_summary)

# Para encerrar a sessão Spark
spark.stop()