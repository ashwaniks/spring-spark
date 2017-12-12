
##How to - Multiline command on Spark
```
:paste

Paste the multiline command.
Press Ctl D

```


## Spark SQL
```

val filename = "/Users/ash/Tools/bigdata/spark-2.2.1-bin-hadoop2.7/examples/src/main/resources/people.json"
val filename = "/Users/ash/Workspaces/BigData/spark/spring-spark-example/src/test/resources/data-1.json"
val df = spark.read.json(filename)
df.printSchema()
df.createOrReplaceTempView("data")
val query =" SELECT " +
            " date_format(to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd'), \"d\") d , " + 
            " date_format(to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd'), \"M\") m , " + 
            " date_format(to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd'), \"y\") y , " + 
            " to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd') as ts, " + 
            " gpsd.Timestamp, " + 
            " gpsd.GPSLatitude, " +
            " gpsd.GPSLongitude, " +
            " gpsd.GPSSpeed " +
            " FROM data " +
            " LATERAL VIEW explode(GPSData) tab as gpsd "
spark.sql(query).show()

val sumQuery =" SELECT " +
            " date_format(to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd'), \"d\") d , " + 
            " date_format(to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd'), \"M\") m , " + 
            " date_format(to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd'), \"y\") y , " + 
            " sum(gpsd.Timestamp)  " +
            " FROM data " +
            " group by d, m , y tab as gpsd " +
             " LATERAL VIEW explode(GPSData) tab as gpsd "
            
            

```