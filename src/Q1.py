from pyspark.sql import SparkSession
from pyspark.sql.functions import hour,to_timestamp
import pandas as pd 

spark=SparkSession.builder.getOrCreate()



df_1=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_3.csv',header='True').select(" passenger_count",hour(to_timestamp(" pickup_datetime")).alias('hour')," pickup_longitude"," pickup_latitude")
df_2=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_4.csv',header='True').select(" passenger_count",hour(to_timestamp(" pickup_datetime")).alias('hour')," pickup_longitude"," pickup_latitude")
df_3=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_5.csv',header='True').select(" passenger_count",hour(to_timestamp(" pickup_datetime")).alias('hour')," pickup_longitude"," pickup_latitude")

df_4=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_6.csv',header='True').select(" passenger_count",hour(to_timestamp(" pickup_datetime")).alias('hour')," pickup_longitude"," pickup_latitude")
df_5=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_7.csv',header='True').select(" passenger_count",hour(to_timestamp(" pickup_datetime")).alias('hour')," pickup_longitude"," pickup_latitude")
'''
df_6=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_6.csv',header='True').select("passenger_count",hour(to_timestamp("pickup_datetime")).alias('hour'),"pickup_longitude","pickup_latitude")
df_7=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_7.csv',header='True').select("passenger_count",hour(to_timestamp("pickup_datetime")).alias('hour'),"pickup_longitude","pickup_latitude")
df_8=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_8.csv',header='True').select("passenger_count",hour(to_timestamp("pickup_datetime")).alias('hour'),"pickup_longitude","pickup_latitude")
df_9=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_9.csv',header='True').select("passenger_count",hour(to_timestamp("pickup_datetime")).alias('hour'),"pickup_longitude","pickup_latitude")
df_10=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_10.csv',header='True').select("passenger_count",hour(to_timestamp("pickup_datetime")).alias('hour'),"pickup_longitude","pickup_latitude")
df_11=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_11.csv',header='True').select("passenger_count",hour(to_timestamp("pickup_datetime")).alias('hour'),"pickup_longitude","pickup_latitude")
df_12=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_12.csv',header='True').select("passenger_count",hour(to_timestamp("pickup_datetime")).alias('hour'),"pickup_longitude","pickup_latitude")
'''
df=spark.createDataFrame(spark.sparkContext.emptyRDD(), df_1.schema)

df=df_5.unionByName(df_4.unionByName(df_3.unionByName(df_2.unionByName(df_1))))

df_pd_TheBronx=pd.DataFrame(columns=['hour','passenger_count'])
df_pd_Brooklyn=pd.DataFrame(columns=['hour','passenger_count'])
df_pd_Manhattan=pd.DataFrame(columns=['hour','passenger_count'])
df_pd_Queens=pd.DataFrame(columns=['hour','passenger_count'])
df_pd_StatenIsland=pd.DataFrame(columns=['hour','passenger_count'])

for i in range(0,24):
    tmp_fir=df.filter(df["hour"]==i)

    tmp_TB=tmp_fir.filter(tmp_fir[" pickup_latitude"]>=40.78441)
    #pc_TB=tmp_TB.filter(tmp_TB[" pickup_longitude"].between(-73.93868,-73.80333)).select(" passenger_count").summary("count").toPandas()
    pc_TB=tmp_TB.filter(tmp_TB[" pickup_longitude"].between(-73.93868,-73.80333)).agg({' passenger_count':"sum"}).collect()[0]
    tmp_pd_TB=pd.DataFrame([[i,pc_TB["sum( passenger_count)"]]],columns=['hour','passenger_count'])
    df_pd_TheBronx=pd.concat([df_pd_TheBronx,tmp_pd_TB],axis=0)

    tmp_BL=tmp_fir.filter(tmp_fir[" pickup_latitude"].between(40.54169,40.78441))
    pc_BL=tmp_BL.filter(tmp_BL[" pickup_longitude"].between(-74.0435,-73.84426)).agg({' passenger_count':'sum'}).collect()[0]
    tmp_pd_BL=pd.DataFrame([[i,pc_BL["sum( passenger_count)"]]],columns=['hour','passenger_count'])
    df_pd_Brooklyn=pd.concat([df_pd_Brooklyn,tmp_pd_BL],axis=0)

    tmp_MT=tmp_fir.filter(tmp_fir[" pickup_latitude"].between(40.69857,40.85793))
    pc_MT=tmp_MT.filter(tmp_MT[" pickup_longitude"].between(-74.02454,-73.92906)).agg({' passenger_count':'sum'}).collect()[0]
    tmp_pd_MT=pd.DataFrame([[i,pc_MT['sum( passenger_count)']]],columns=['hour','passenger_count'])
    df_pd_Manhattan=pd.concat([df_pd_Manhattan,tmp_pd_MT],axis=0)

    tmp_SI=tmp_fir.filter(tmp_fir[" pickup_latitude"].between(40.5015,40.64128))
    pc_SI=tmp_SI.filter(tmp_SI[" pickup_longitude"].between(-74.25094,-74.05032)).agg({' passenger_count':'sum'}).collect()[0]
    tmp_pd_SI=pd.DataFrame([[i,pc_SI['sum( passenger_count)']]],columns=['hour','passenger_count'])
    df_pd_StatenIsland=pd.concat([df_pd_StatenIsland,tmp_pd_SI],axis=0)

    tmp_QU=tmp_fir.filter(tmp_fir[" pickup_latitude"].between(40.57038,40.95939))
    pc_QU=tmp_BL.filter(tmp_QU[" pickup_longitude"]>=-73.84426).agg({' passenger_count':'sum'}).collect()[0]
    tmp_pd_QU=pd.DataFrame([[i,pc_QU['sum( passenger_count)']]],columns=['hour','passenger_count'])
    df_pd_Queens=pd.concat([df_pd_Queens,tmp_pd_QU],axis=0)

print(df_pd_Brooklyn)
print(df_pd_Manhattan)
print(df_pd_Queens)
print(df_pd_StatenIsland)
print(df_pd_TheBronx)

df_pd_Brooklyn.to_csv('/home/hadoop/Q1_Brooklyn.csv')
df_pd_Manhattan.to_csv('/home/hadoop/Q1_Manhattan.csv')
df_pd_Queens.to_csv('/home/hadoop/Q1_Queens.csv')
df_pd_StatenIsland.to_csv('/home/hadoop/Q1_StatenIsland.csv')
df_pd_TheBronx.to_csv('/home/hadoop/Q1_TheBronx.csv')
