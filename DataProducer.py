import pandas as pd
from kafka import KafkaProducer
import json,time,datetime

# The excel data is read from the following link
# Note: Reading the .xlsx file requires openpyxl library
#df = pd.read_excel("https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx")
df = pd.read_excel("D:/Users/MASTO-2975DAS/Documents/upGrad/BigData/BD C7 - Retail Data Analysis/Online Retail.xlsx")

#df['InvoiceNo']=df['InvoiceNo'].astype('int32')

# The following line is for creating a new column called 'type'
# The type column is for differenciating whether the data is of type item oreder or item return
df['type']=df.apply(lambda x:"RETURN" if x['Quantity']<0 else "ORDER",axis=1 )

# The time stamp is converted into string because the json cannot store timestamp datatype.
# Therefore the timestamp in InvoiceDate column is converted into string in new column timestamp.
df['timestamp'] = df.apply(lambda x:str(x['InvoiceDate']),axis=1 )


# The columns are renamed so that the keys that will be created in list of dictionary is satisfies the problem statement.
df.rename(columns={
    'InvoiceNo':'invoice_no',
    'Country':'country',
    'Description':'title',
    'Quantity':'quantity',
    'StockCode':'SKU',
    'UnitPrice':'unit_price',
}, inplace=True)

# The data is sorted in ascending order of InvoiceDate column so that the data is sent in a sequence
df.sort_values(by=['InvoiceDate'],ascending=True,inplace=True)

# New dataframe is create which will help in determining the delays between each sending data
invoiceDate_df = df[['invoice_no','InvoiceDate']].drop_duplicates()

# The following dataframe is for reformatting the dictionary according to the problem statement
item_df = df[['invoice_no','SKU','title','unit_price','quantity']]

# A list of dictionary is created form invoice datarframe
data_l = df[['invoice_no','country','timestamp','type']].drop_duplicates().to_dict('records')

# A connection is establisted with the Kafka server and the topic
producer = KafkaProducer(bootstrap_servers='44.200.201.2:9092')
topic='real-time-project'


# preTimestamp = datetime.datetime.timestamp(invoiceDate_df.iloc[0,1])
for x in data_l:
    x['timestamp'] = str(datetime.datetime.now())
    x['items']=item_df.loc[item_df['invoice_no']==x['invoice_no']].drop(['invoice_no'],axis=1).to_dict('records')
#     currentTimestamp = datetime.datetime.timestamp(
#         invoiceDate_df.loc[invoiceDate_df['invoice_no']==x['invoice_no']].iloc[0,1]
#     )
#     time.sleep(int(currentTimestamp-preTimestamp))
#     preTimestamp = currentTimestamp
    time.sleep(5)
    ack = producer.send(topic,key=b'Data',value=bytes(json.dumps(x).encode('utf-8')))
    meta = ack.get()
    print("Topic:{}".format(meta.topic))
