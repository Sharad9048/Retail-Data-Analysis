import threading,os,time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,ArrayType,LongType,ByteType
from pyspark.sql.functions import col,udf,from_json,to_timestamp,window
from pyspark.streaming.context import StreamingContext
# """
#  I have imported the sum function as sumUDF, 
#  because I want to use the inbuilt sum function of python to create udf
#  """
from pyspark.sql.functions import sum as sumUDF

# """
# This function is for stopping the query gracefully when 'stop' file is found
# """
def stopQueryProcess(query:StreamingContext,stopFile):
  print("Run command 'touch stop' to stop the spark streaming")
  while(1):
    if os.path.exists(stopFile):
      print("Stoping command recieved")
      query.stop(stopGraceFully=True)
    time.sleep(1)

# """
# This function is for starting a thread to stop the query.
# """
def stopQuery(query:StreamingContext):
    stopFile = 'stop'
    stopProcess = threading.Thread(stopQueryProcess,args=(query,stopFile))
    stopProcess.start()


# The bellow commented code is for personal computer
# import findspark
# findspark.init()

# The spark Session is created as with application name as SparkStream
spark=SparkSession.builder\
.appName('SparkStream')\
.getOrCreate()


# """
# {
#   "invoice_no": 154132541653705,
#   "country": "United Kingdom",
#   "timestamp": "2020-09-18 10:55:23",
#   "type": "ORDER",
#   "items": [
#     {
#       "SKU": "21485",
#       "title": "RETROSPOT HEART HOT WATER BOTTLE",
#       "unit_price": 4.95,
#       "quantity": 6
#     },
#     {
#       "SKU": "23499",
#       "title": "SET 12 VINTAGE DOILY CHALK",
#       "unit_price": 0.42,
#       "quantity": 2
#     }
#   ]  
# }
# According to the structure of incomming json, the bellow schema is created.
# For the a dictionary I have used StructType and StructField.
# The 'items' key contains an array of dictionary,
# so I have used ArrayType for the values in 'items' key.
# """
schema = StructType([
    StructField('invoice_no',LongType(),False),
    StructField('country',StringType(),False),
    StructField('timestamp',StringType(),False),
    StructField('type',StringType(),False),
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

# """
# - The code will read data from the kafka topic 'real-time-project, 
#   perform transformation and store in inputDF dataframe
# - The format is for declaring that the code will read from a kafka server.
# - In the bootstrap server option the private IP address and port is passed.
# - In the subcribe option the topic name 'real-time-project' is passed.
# - In the starting offset opthin I want the earlist to the latest data within processing time interval. 
# """
inputDf=spark.readStream\
.format("kafka")\
.option("kafka.bootstrap.servers","18.211.252.152:9092")\
.option("subscribe","real-time-project")\
.option('startingOffset','earliest')\
.load()

# This UDF will be used for creating total_cost column
@udf(returnType=FloatType())    # Declaring UDF with return type as FloatType
def totalCost(arr1,arr2):       # Defining function accepting two arrays
    result = 0                  # It will the summation of the products 
    for x,y in zip(arr1,arr2):  # Creating loop where x will have elements of arr1 and y will have elements of arr2
        result+=x*y             # The x and y are multiplied, added with the result and stored in the result
    return result               # The value in result is returned

# This UDF will be used for creating total_items column
@udf(returnType=IntegerType())  # Declaring UDF with return type as IntegerType
def totalItems(arr1):           # Defining function accepting array
    return sum(arr1)            # The sum of the elements in the array is returned

# This UDF will be used for creating new column is_order
isOrder = udf(
    lambda x:1 if x=='ORDER' else 0,    # It will check if the input="ORDER" then return 1 else 0 
    ByteType()                          # Declaring return type to be ByteType
)

# This UDF will be used for creating new column is_retur
isReturn = udf(
    lambda x:1 if x=='RETURN' else 0,   # It will check if the input="RETURN" then return 1 else 0
    ByteType()                          # Declaring return type to be ByteType
)

# """
# The inputDf is transformend and stored in commomDf dataframe
# - The string data in value column is converted to a dataframe with the help of pre defined schema 'schema'
#   using 'from_json' function.
# - The new columns are created called 'total_cost', 'total_items', 'is_order', 'is_return'.
#   'total_cost' is created by using UDF 'totalCost' on 'item.quantity' and 'item.unit_price'.
#   'total_items' is created by using UDF 'totalItems' on 'items.quantity'.
#   'is_order' is created by using UDF 'isOrder' on 'type'
#   'is_return' is created by using UDF 'isReturn' on 'type'
# - The timestamp string is converted to timestamp datatype using 'to_timestamp' function
# """
commonDf = inputDf\
.select(from_json(col("value").cast("string"),schema).alias('data'))\
.select('data.*')\
.withColumn('total_cost',totalCost(col('items.quantity'),col('items.unit_price')))\
.withColumn('total_items',totalItems(col('items.quantity')))\
.withColumn('is_order',isOrder(col('type')))\
.withColumn('is_return',isReturn(col('type')))\
.withColumn('timestamp',to_timestamp(col("timestamp")))

# """
# - 'summerisedInput' dataframe is given a copy of 'commonDf' dataframe dropping two columns 'items' and 'type'
# """
summerisedInput = commonDf\
.drop('items','type')

# """
# - Watermark is added to 'commonDf' to perform aggrigations
# """
commonDf = commonDf\
.withWatermark('timestamp',"1 minutes")

# """
# - The transformations is stored in 'timeKPI' dataframe
# - Groupby transformation against window of 'timestamp' of 1 minute is added
# - Aggrigation is add creating columns 'OPM', 'total_volume_sales', 'rate_of_return', 'average_transaction_size'.
# """
timeKPI = commonDf\
.groupBy(window('timestamp','1 minutes'))\
.agg(sumUDF('is_order').alias('OPM'),\
    sumUDF('total_cost').alias('total_volume_sales'),\
    (sumUDF('is_return')/(sumUDF('is_return')+sumUDF('is_order'))).alias('rate_of_return'),\
    (sumUDF('total_cost')/(sumUDF('is_return')+sumUDF('is_order'))).alias('average_transaction_size')\
)

# """
# - The transformations is stored in 'timeCountryKPI' dataframe
# - Groupby transformation against window of 'timestamp' of 1 minute and country is added
# - Aggrigation is add creating columns 'OPM', 'total_volume_sales', 'rate_of_return', 'average_transaction_size'.
# """
timeCountryKPI = commonDf\
.groupBy(window('timestamp','1 minutes'),'country')\
.agg(sumUDF('is_order').alias('OPM'),\
    sumUDF('total_cost').alias('total_volume_sales'),\
    (sumUDF('is_return')/(sumUDF('is_return')+sumUDF('is_order'))).alias('rate_of_return'),\
    (sumUDF('total_cost')/(sumUDF('is_return')+sumUDF('is_order'))).alias('average_transaction_size')\
)

# """
# - The summerisedInputQuery is printed on console in append mode
# - truncate option: The truncation is disabled
# - Trigger is added to perform transformations and print to console every 1 minute
# """
summerisedInputQuery = summerisedInput\
.writeStream\
.outputMode("append")\
.format("console")\
.option('truncate','false')\
.trigger(processingTime='1 minutes')\
.start()

# """
# - The timeKPI is saved in a json format. This action is stored in timeKPIQuery
# - Path option: '/user/root/time_kpi/' is the hdfs path where json will be saved.
# - checkpointLocation option: '/user/root/time_kpi/time_kpi_v1' is the hdfs path which is the checkpoint location to avoid any data loss
# - Trigger is added to perform transformations and saves the result in json format every 10 minute
# """
timeKPIQuery = timeKPI\
.writeStream\
.format('json')\
.option('path','/user/root/time_kpi/')\
.option('checkpointLocation','/user/root/time_kpi/time_kpi_v1/')\
.trigger(processingTime='10 minutes')\
.start()

# """
# - The timeCountryKPI is saved in a json format. This action is stored in timeCountryKPIQuery
# - Path option: '/user/root/country_kpi/' is the hdfs path where json will be saved.
# - checkpointLocation option: '/user/root/time_kpi/country_kpi_v1' is the hdfs path which is the checkpoint location to avoid any data loss
# - Trigger is added to perform transformations and saves the result in json format every 10 minute
# """
timeCountryKPIQuery = timeCountryKPI\
.writeStream\
.format('json')\
.option('path','/user/root/country_kpi/')\
.option('checkpointLocation','/user/root/country_kpi/country_kpi_v1/')\
.trigger(processingTime='10 minutes')\
.start()


# The qureies will stop when 'stop' file is found
stopQuery(summerisedInputQuery)
stopQuery(timeKPI)
stopQuery(timeCountryKPI)

# The queries are set to continue transformation until the process is terminated
summerisedInputQuery.awaitTermination()
timeKPIQuery.awaitTermination()
timeCountryKPIQuery.awaitTermination()