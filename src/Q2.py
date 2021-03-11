from pyspark.sql import SparkSession
import pandas as pd

spark=SparkSession.builder.getOrCreate()

df_1=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_3.csv',header='True')
df_2=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_4.csv',header='True')
df_3=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_5.csv',header='True')
df_4=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_6.csv',header='True')
df_5=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_7.csv',header='True')
    '''
    df_6=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_6.csv',header='True')
    df_7=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_7.csv',header='True')
    df_8=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_8.csv',header='True')
    df_9=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_9.csv',header='True')
    df_10=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_10.csv',header='True')
    df_11=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_11.csv',header='True')
    df_12=spark.read.csv('hdfs://localhost:9000/NYC/trip_data_12.csv',header='True')
    '''

df=spark.createDataFrame(spark.sparkContext.emptyRDD(), df_1.schema)
df=df_5.unionByName(df_4.unionByName(df_3.unionByName(df_2.unionByName(df_1))))
data_yuan=df.select(" passenger_count"," trip_time_in_secs")

def Q2(num):

    max_time=120
 
    data_sec=data_yuan.filter(data_yuan[" passenger_count"]==num)
    data_num=data_sec.count()

    
    def CCount(start,end):
        data_thr=data_sec.filter(data_sec[" trip_time_in_secs"].between(start*60+1,end*60))
        return data_thr.count()
        
    df_pd_fir=pd.DataFrame(columns=['minit','count'])

    if data_num==0:
        return pd.DataFrame([[num,0,0]],columns=['passenger_count','minutes','propability'])
    else:
        for i in range(0,max_time,2):
            tmp=pd.DataFrame([[i+2,CCount(i,i+2)]],columns=['minit','count'],index=[i])
            df_pd_fir=pd.concat([df_pd_fir,tmp],axis=0)


        df_pd_sec=df_pd_fir.sort_values(by='count',ascending=False)
        df_pd_sec.reset_index(drop=True,inplace=True)

        if data_num>=3:
            minit=df_pd_sec.loc[0,'minit']*0.8+df_pd_sec.loc[1,'minit']*0.1+df_pd_sec.loc[2,'minit']*0.1
            prob=(df_pd_sec.loc[0,'count']+df_pd_sec.loc[1,'count']+df_pd_sec.loc[2,'count'])/data_num*100
        else:
            minit=df_pd_sec.loc[0,'minit']
            prob=df_pd_sec.loc[0,'count']/data_num*100
            
        print("according to the data, when the number of passenger is ")

        print(num)
        print("the trip almost like to take ",minit,"minutes")
        print("the probability may is about ")
        print(prob,"%")

        return pd.DataFrame([[num,minit,prob]],columns=['passenger_count','minutes','propability'])


answer=pd.DataFrame(columns=['passenger_count','minutes','propability'])
for i in range(1,10):
    tmp=Q2(i)
    answer=pd.concat([answer,tmp],axis=0)

answer.to_csv('/home/hadoop/Q2_an.csv')
print(answer)
