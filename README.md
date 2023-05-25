# Retail-Data-Analysis

- [Dataset](https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx)
- The code will extract data from the Kafka borker and perform real-time data analytics and save the data in JSON format in HDFS
The tools and services used in the project are AWS EMR, Spark Streaming, and Hadoop File System.
  - EMR version-5.36.0
  - Hadoop 2.10.1
  - Spark 2.4.8
- [DataProducer.py](https://github.com/Sharad9048/Retail-Data-Analysis/blob/main/DataProducer.py) will load the csv data into the Kafka server. This code will produce the data for your Kafka broker.<br>
Note: The focus of the project is on [spark-stream.py](https://github.com/Sharad9048/Retail-Data-Analysis/blob/main/spark-stream.py) code.

- The sample raw stream data looks like:
```
{
  "invoice_no": 154132541653705,
  "country": "United Kingdom",
  "timestamp": "2020-09-18 10:55:23",
  "type": "ORDER",
  "items": [
    {
      "SKU": "21485",
      "title": "RETROSPOT HEART HOT WATER BOTTLE",
      "unit_price": 4.95,
      "quantity": 6
    },
    {
      "SKU": "23499",
      "title": "SET 12 VINTAGE DOILY CHALK",
      "unit_price": 0.42,
      "quantity": 2
    }
  ]  
}
```
- The code will print the following onto the console
  - The following attributes from the raw Stream data have to be taken into account for the project:
    - invoice_no: Identifier of the invoice
    - country: Country where the order is placed
    - timestamp: Time at which the order is placed
  - In addition to these attributes, the following UDFs have to be calculated and added to the table:
    - total_cost: Total cost of an order arrived at by summing up the cost of all products in that invoice (The return cost is treated as a loss. Thus, for return orders, this value will be negative.)
    - total_items: Total number of items present in an order
    - is_order: This flag denotes whether an order is a new order or not. If this invoice is for a return order, the value should be 0.
    - is_return: This flag denotes whether an order is a return order or not. If this invoice is for a new sales order, the value should be 0.
- Calculating time-based KPIs:
  - Code to calculate time-based KPIs tumbling window of one minute on orders across the globe. These KPIs were discussed in the previous segment.
  - KPIs have to be calculated for a 10-minute interval for evaluation; so, ten 1-minute window batches have to be taken.
  - Time-based KPIs can be structured like below. (These tables do not need to be outputted and are just for reference as to how your KPI tables must be structured when all the files are combined.)
- Calculating time- and country-based KPIs:
  - Code to calculate time- and country-based KPIs tumbling window of one minute on orders on a per-country basis. These KPIs were discussed in the previous segment.
  - KPIs have to be calculated for a 10-minute interval for evaluation; so, ten 1-minute window batches have to be taken.
  - Time- and country-based KPIs can be structured like below. (These tables do not need to be outputted and are just for reference as to how your KPI tables must be structured.)
- Run the following code to run [spark-stream.py](https://github.com/Sharad9048/Retail-Data-Analysis/blob/main/spark-stream.py):
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 spark-stream.py > output
```
- The console data will be stored in 'output' file
- The JSON data of time based KPI will be stored in /user/root/time_kpi folder in hadoop file system.
- The JSON data of country time based KPI will be stored in /user/root/country_kpi folder in hadoop file system.
