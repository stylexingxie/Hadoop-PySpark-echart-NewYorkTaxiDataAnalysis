from pyspark.sql import SparkSession
import pandas as pd 

spark=SparkSession.builder.getOrCreate()

df_1=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_3.csv',header='True').select(" passenger_count"," pickup_longitude"," pickup_latitude",
                                                                                                        " dropoff_longitude"," dropoff_latitude")
df_2=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_4.csv',header='True').select(" passenger_count"," pickup_longitude"," pickup_latitude",
                                                                                                        " dropoff_longitude"," dropoff_latitude")
df_3=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_5.csv',header='True').select(" passenger_count"," pickup_longitude"," pickup_latitude",
                                                                                                        " dropoff_longitude"," dropoff_latitude")
df_4=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_6.csv',header='True').select(" passenger_count"," pickup_longitude"," pickup_latitude",
                                                                                                        " dropoff_longitude"," dropoff_latitude")
df_5=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_7.csv',header='True').select(" passenger_count"," pickup_longitude"," pickup_latitude",
                                                                                                        " dropoff_longitude"," dropoff_latitude")

df=spark.createDataFrame(spark.sparkContext.emptyRDD(), df_1.schema)

df=df_5.unionByName(df_4.unionByName(df_3.unionByName(df_2.unionByName(df_1))))

df_TB_PK=df.filter(df[" pickup_longitude"].between(-73.93868,-73.80333)).filter(df[" pickup_latitude"]>=40.78441)
df_BL_PK=df.filter(df[" pickup_longitude"].between(-74.0435,-73.84426)).filter(df[" pickup_latitude"].between(40.54169,40.78441))
df_QU_PK=df.filter(df[" pickup_longitude"]>=-73.84426).filter(df[" pickup_latitude"].between(40.57038,40.95939))
df_MH_PK=df.filter(df[" pickup_longitude"].between(-74.02454,-73.92906)).filter(df[" pickup_latitude"].between(40.69857,40.85793))
df_SI_PK=df.filter(df[" pickup_longitude"].between(-74.25094,-74.05032)).filter(df[" pickup_latitude"].between(40.5015,40.64128))

pc_TB2BL=df_TB_PK.filter(df[" dropoff_longitude"].between(-74.0435,-73.84426)).filter(df[" dropoff_latitude"].between(40.54169,40.78441)).agg({' passenger_count':'sum'}).collect()[0]
pc_TB2QU=df_TB_PK.filter(df[" dropoff_longitude"]>=-73.84426).filter(df[" dropoff_latitude"].between(40.57038,40.95939)).agg({' passenger_count':'sum'}).collect()[0]
pc_TB2MH=df_TB_PK.filter(df[" dropoff_longitude"].between(-74.02454,-73.92906)).filter(df[" dropoff_latitude"].between(40.69857,40.85793)).agg({' passenger_count':'sum'}).collect()[0]
pc_TB2SI=df_TB_PK.filter(df[" dropoff_longitude"].between(-74.25094,-74.05032)).filter(df[" dropoff_latitude"].between(40.5015,40.64128)).agg({' passenger_count':'sum'}).collect()[0]
pc_TB2TB=df_TB_PK.filter(df[" dropoff_longitude"].between(-73.93868,-73.80333)).filter(df[" dropoff_latitude"]>=40.78441).agg({' passenger_count':'sum'}).collect()[0]

pd_TB=pd.DataFrame([["toBrooklyn",pc_TB2BL["sum( passenger_count)"]],
                    ["toTheBronx",pc_TB2TB["sum( passenger_count)"]],
                    ["toQueens",pc_TB2QU["sum( passenger_count)"]],
                    ["toManhattan",pc_TB2MH["sum( passenger_count)"]],
                    ["toStatenIsland",pc_TB2SI["sum( passenger_count)"]]],columns=['Destination','passenger_count'])
pd_TB.to_csv('/home/hadoop/Q4_tb2any.csv')

pc_BL2TB=df_BL_PK.filter(df[" dropoff_longitude"].between(-73.93868,-73.80333)).filter(df[" dropoff_latitude"]>=40.78441).agg({' passenger_count':'sum'}).collect()[0]
pc_BL2QU=df_BL_PK.filter(df[" dropoff_longitude"]>=-73.84426).filter(df[" dropoff_latitude"].between(40.57038,40.95939)).agg({' passenger_count':'sum'}).collect()[0]
pc_BL2MH=df_BL_PK.filter(df[" dropoff_longitude"].between(-74.02454,-73.92906)).filter(df[" dropoff_latitude"].between(40.69857,40.85793)).agg({' passenger_count':'sum'}).collect()[0]
pc_BL2SI=df_BL_PK.filter(df[" dropoff_longitude"].between(-74.25094,-74.05032)).filter(df[" dropoff_latitude"].between(40.5015,40.64128)).agg({' passenger_count':'sum'}).collect()[0]

pd_BL=pd.DataFrame([["toTheBronx",pc_BL2TB["sum( passenger_count)"]],
                    ["toQueens",pc_BL2QU["sum( passenger_count)"]],
                    ["toManhattan",pc_BL2MH["sum( passenger_count)"]],
                    ["toStatenIsland",pc_BL2SI["sum( passenger_count)"]]],columns=['Destination','passenger_count'])
pd_BL.to_csv('/home/hadoop/Q4_bl2any.csv')

pc_MH2TB=df_MH_PK.filter(df[" dropoff_longitude"].between(-73.93868,-73.80333)).filter(df[" dropoff_latitude"]>=40.78441).agg({' passenger_count':'sum'}).collect()[0]
pc_MH2QU=df_MH_PK.filter(df[" dropoff_longitude"]>=-73.84426).filter(df[" dropoff_latitude"].between(40.57038,40.95939)).agg({' passenger_count':'sum'}).collect()[0]
pc_MH2SI=df_MH_PK.filter(df[" dropoff_longitude"].between(-74.25094,-74.05032)).filter(df[" dropoff_latitude"].between(40.5015,40.64128)).agg({' passenger_count':'sum'}).collect()[0]
pc_MH2BL=df_MH_PK.filter(df[" dropoff_longitude"].between(-74.0435,-73.84426)).filter(df[" dropoff_latitude"].between(40.54169,40.78441)).agg({' passenger_count':'sum'}).collect()[0]

pd_MH=pd.DataFrame([["toTheBronx",pc_MH2TB["sum( passenger_count)"]],
                    ["toQueens",pc_MH2QU["sum( passenger_count)"]],
                    ["toStatenIsland",pc_MH2SI["sum( passenger_count)"]],
                    ["toBrooklyn",pc_MH2BL["sum( passenger_count)"]]],columns=['Destination','passenger_count'])
pd_MH.to_csv('/home/hadoop/Q4_mh2any.csv')

pc_QU2TB=df_QU_PK.filter(df[" dropoff_longitude"].between(-73.93868,-73.80333)).filter(df[" dropoff_latitude"]>=40.78441).agg({' passenger_count':'sum'}).collect()[0]
pc_QU2SI=df_QU_PK.filter(df[" dropoff_longitude"].between(-74.25094,-74.05032)).filter(df[" dropoff_latitude"].between(40.5015,40.64128)).agg({' passenger_count':'sum'}).collect()[0]
pc_QU2BL=df_QU_PK.filter(df[" dropoff_longitude"].between(-74.0435,-73.84426)).filter(df[" dropoff_latitude"].between(40.54169,40.78441)).agg({' passenger_count':'sum'}).collect()[0]
pc_QU2MH=df_QU_PK.filter(df[" dropoff_longitude"].between(-74.02454,-73.92906)).filter(df[" dropoff_latitude"].between(40.69857,40.85793)).agg({' passenger_count':'sum'}).collect()[0]

pd_QU=pd.DataFrame([["toTheBronx",pc_QU2TB["sum( passenger_count)"]],
                    ["toStatenIsland",pc_QU2SI["sum( passenger_count)"]],
                    ["toBrooklyn",pc_QU2BL["sum( passenger_count)"]],
                    ["toManhattan",pc_QU2MH["sum( passenger_count)"]]],columns=['Destination','passenger_count'])
pd_QU.to_csv('/home/hadoop/Q4_qu2any.csv')

pc_SI2TB=df_SI_PK.filter(df[" dropoff_longitude"].between(-73.93868,-73.80333)).filter(df[" dropoff_latitude"]>=40.78441).agg({' passenger_count':'sum'}).collect()[0]
pc_SI2BL=df_SI_PK.filter(df[" dropoff_longitude"].between(-74.0435,-73.84426)).filter(df[" dropoff_latitude"].between(40.54169,40.78441)).agg({' passenger_count':'sum'}).collect()[0]
pc_SI2MH=df_SI_PK.filter(df[" dropoff_longitude"].between(-74.02454,-73.92906)).filter(df[" dropoff_latitude"].between(40.69857,40.85793)).agg({' passenger_count':'sum'}).collect()[0]
pc_SI2QU=df_SI_PK.filter(df[" dropoff_longitude"]>=-73.84426).filter(df[" dropoff_latitude"].between(40.57038,40.95939)).agg({' passenger_count':'sum'}).collect()[0]

pd_SI=pd.DataFrame([["toTheBronx",pc_SI2TB["sum( passenger_count)"]],
                    ["toBrooklyn",pc_SI2BL["sum( passenger_count)"]],
                    ["toManhattan",pc_SI2MH["sum( passenger_count)"]],
                    ["toQueens",pc_SI2QU["sum( passenger_count)"]]],columns=['Destination','passenger_count'])
pd_SI.to_csv('/home/hadoop/Q4_si2any.csv')