#import findspark,json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,ArrayType,LongType
from pyspark.sql.functions import col,udf,from_json,to_timestamp,window
from pyspark.sql.functions import sum as sumUDF
#findspark.init()

spark=SparkSession.builder\
.appName('SparkStream')\
.getOrCreate()

schema = StructType([
    StructField('invoice_no',StringType()),
    StructField('country',StringType()),
    StructField('timestamp',StringType()),
    StructField('type',StringType()),
    StructField(
        'items',ArrayType(
            StructType([
                StructField('SKU',StringType(),False),
                StructField('title',StringType(),False),
                StructField('unit_price',FloatType(),False),
                StructField('quantity',IntegerType(),False),
            ])
        )
    )
])

inputDf=spark.readStream\
.format("kafka")\
.option("kafka.bootstrap.servers","ec2-18-206-39-141.compute-1.amazonaws.com:9092")\
.option("subscribe","real-time-project")\
.option('startingOffset','earliest')\
.load()

@udf(returnType=FloatType())
def totalCost(arr1,arr2):
    result = 0
    for x,y in zip(arr1,arr2):
        result+=x*y
    return result

@udf(returnType=IntegerType())
def totalItems(arr1):
    return sum(arr1)

isOrder = udf(lambda x:1 if x=='ORDER' else 0)
isReturn = udf(lambda x:1 if x=='RETURN' else 0)


commonDf = inputDf\
.select(from_json(col("value").cast("string"),schema).alias('data'))\
.select('data.*')\
.withColumn('total_cost',totalCost(col('items.quantity'),col('items.unit_price')))\
.withColumn('total_items',totalItems(col('items.quantity')))\
.withColumn('is_order',isOrder(col('type')))\
.withColumn('is_return',isReturn(col('type')))\
.withColumn('timestamp',to_timestamp(col("timestamp")))
# commonDf.printSchema()
summerisedInput = commonDf\
.drop('items','type')

# OPM = sum(is_order)
# total_volume_sales = sum(total_cost)
# rate_of_return = sum(is_return)/(sum(is_order)+sum(is_return))
# average_transaction_size = total_volume_sales/(sum(is_order)+sum(is_return))
timeKPI = commonDf\
.withWatermark('timestamp',"1 minutes")\
.groupBy(window('timestamp','1 minutes'))\
.agg(sumUDF('is_order').alias('OPM'),\
    sumUDF('total_cost').alias('total_volume_sales'),\
    (sumUDF('is_return')/(sumUDF('is_return')+sumUDF('is_order'))).alias('rate_of_return'),\
    (sumUDF('total_cost')/(sumUDF('is_return')+sumUDF('is_order'))).alias('average_transaction_size')\
)



summerisedInputQuery = summerisedInput\
.writeStream\
.outputMode("append")\
.format("console")\
.option('truncate','false')\
.trigger(processingTime='1 minutes')\
.start()

timeKPIQuery = timeKPI\
.writeStream\
.format('json')\
.option('checkpointLocation','/user/root/checkpoint/kpi')\
.option('path','/user/root/time_kpi/time_kpi_v1')\
.trigger(processingTime='10 minutes')\
.start()

summerisedInputQuery.awaitTermination()
timeKPIQuery.awaitTermination()
