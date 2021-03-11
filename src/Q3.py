from pyspark.sql import SparkSession
from pyspark.sql.functions import hour,to_timestamp
import pandas as pd 

spark=SparkSession.builder.getOrCreate()

df_1=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_3.csv',header='True').select(" passenger_count",hour(to_timestamp(" pickup_datetime")).alias('Phour'),
                                                                                                        hour(to_timestamp(" dropoff_datetime")).alias('Dhour'),
                                                                                                        " pickup_longitude"," pickup_latitude",
                                                                                                        " dropoff_longitude"," dropoff_latitude")
df_2=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_4.csv',header='True').select(" passenger_count",hour(to_timestamp(" pickup_datetime")).alias('Phour'),
                                                                                                        hour(to_timestamp(" dropoff_datetime")).alias('Dhour'),
                                                                                                        " pickup_longitude"," pickup_latitude",
                                                                                                        " dropoff_longitude"," dropoff_latitude")
df_3=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_5.csv',header='True').select(" passenger_count",hour(to_timestamp(" pickup_datetime")).alias('Phour'),
                                                                                                        hour(to_timestamp(" dropoff_datetime")).alias('Dhour'),
                                                                                                        " pickup_longitude"," pickup_latitude",
                                                                                                        " dropoff_longitude"," dropoff_latitude")
df_4=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_6.csv',header='True').select(" passenger_count",hour(to_timestamp(" pickup_datetime")).alias('Phour'),
                                                                                                        hour(to_timestamp(" dropoff_datetime")).alias('Dhour'),
                                                                                                        " pickup_longitude"," pickup_latitude",
                                                                                                        " dropoff_longitude"," dropoff_latitude")
df_5=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_7.csv',header='True').select(" passenger_count",hour(to_timestamp(" pickup_datetime")).alias('Phour'),
                                                                                                        hour(to_timestamp(" dropoff_datetime")).alias('Dhour'),
                                                                                                        " pickup_longitude"," pickup_latitude",
                                                                                                        " dropoff_longitude"," dropoff_latitude")

df=spark.createDataFrame(spark.sparkContext.emptyRDD(), df_1.schema)

df=df_5.unionByName(df_4.unionByName(df_3.unionByName(df_2.unionByName(df_1))))

df_MH_DP=df.filter(df[" dropoff_longitude"].between(-74.02454,-73.92906)).filter(df[" dropoff_latitude"].between(40.69857,40.85793)).filter(df["Dhour"].between(6,9))
df_MH_PK=df.filter(df[" pickup_longitude"].between(-74.02454,-73.92906)).filter(df[" pickup_latitude"].between(40.69857,40.85793)).filter(df["Phour"].between(17,20))

df_pd_D_TB=df_MH_DP.filter(df_MH_DP[" pickup_longitude"].between(-73.93868,-73.80333)).filter(df_MH_DP[" pickup_latitude"]>=40.78441).agg({' passenger_count':'sum'}).collect()[0]
df_pd_P_TB=df_MH_PK.filter(df_MH_PK[" dropoff_longitude"].between(-73.93868,-73.80333)).filter(df_MH_PK[" dropoff_latitude"]>=40.78441).agg({' passenger_count':'sum'}).collect()[0]

df_pd_D_BL=df_MH_DP.filter(df_MH_DP[" pickup_longitude"].between(-74.0435,-73.84426)).filter(df_MH_DP[" pickup_latitude"].between(40.54169,40.78441)).agg({' passenger_count':'sum'}).collect()[0]
df_pd_P_BL=df_MH_PK.filter(df_MH_PK[" dropoff_longitude"].between(-74.0435,-73.84426)).filter(df_MH_PK[" dropoff_latitude"].between(40.54169,40.78441)).agg({' passenger_count':'sum'}).collect()[0]

df_pd_D_QU=df_MH_DP.filter(df_MH_DP[" pickup_longitude"]>=-73.84426).filter(df_MH_DP[" pickup_latitude"].between(40.57038,40.95939)).agg({' passenger_count':'sum'}).collect()[0]
df_pd_P_QU=df_MH_PK.filter(df_MH_PK[" dropoff_longitude"]>=-73.84426).filter(df_MH_PK[" dropoff_latitude"].between(40.57038,40.95939)).agg({' passenger_count':'sum'}).collect()[0]

df_pd_D_SI=df_MH_DP.filter(df_MH_DP[" pickup_longitude"].between(-74.25094,-74.05032)).filter(df_MH_DP[" pickup_latitude"].between(40.5015,40.64128)).agg({' passenger_count':'sum'}).collect()[0]
df_pd_P_SI=df_MH_PK.filter(df_MH_PK[" dropoff_longitude"].between(-74.25094,-74.05032)).filter(df_MH_PK[" dropoff_latitude"].between(40.5015,40.64128)).agg({' passenger_count':'sum'}).collect()[0]

df_pd_D_MH=df_MH_DP.filter(df_MH_DP[" pickup_longitude"].between(-74.02454,-73.92906)).filter(df_MH_DP[" pickup_latitude"].between(40.69857,40.85793)).agg({' passenger_count':'sum'}).collect()[0]
df_pd_P_MH=df_MH_PK.filter(df_MH_PK[" dropoff_longitude"].between(-74.02454,-73.92906)).filter(df_MH_PK[" dropoff_latitude"].between(40.69857,40.85793)).agg({' passenger_count':'sum'}).collect()[0]

df_pd_D=pd.DataFrame([["The Bronx",df_pd_D_TB['sum( passenger_count)']],

                      ["Brooklyn",df_pd_D_BL['sum( passenger_count)']],

                      ["Queens",df_pd_D_QU['sum( passenger_count)']],
                      ["Staten Island",df_pd_D_SI['sum( passenger_count)']],
                      ["Manhattan",df_pd_D_MH['sum( passenger_count)']]],columns=['Pickup Area','passenger_count'])

df_pd_P=pd.DataFrame([["The Bronx",df_pd_P_TB['sum( passenger_count)']],

                      ["Brooklyn",df_pd_P_BL['sum( passenger_count)']],

                      ["Queens",df_pd_P_QU['sum( passenger_count)']],
                      ["Staten Island",df_pd_P_SI['sum( passenger_count)']],
                      ["Manhattan",df_pd_P_MH['sum( passenger_count)']]],columns=['Dropoff Area','passenger_count'])

print(df_pd_D)

df_pd_D.to_csv('/home/hadoop/Q3_pickup.csv')
df_pd_P.to_csv('/home/hadoop/Q3_dropoff.csv')
