package com.memrietch.spark;


import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

/**
 * Created by achat1 on 9/23/15.
 * Just an example to see if it works.
 */
@Component
public class WordCount {
    @Autowired
    private SparkSession sparkSession;


    /**
     * https://docs-snaplogic.atlassian.net/wiki/spaces/SD/pages/2458071/Date+Functions+and+Properties+Spark+SQL
     * @throws Exception
     */
    public void dataframeExample() throws Exception{
        String filename  =
                "src/test/resources/data-1.json";
        //"/Users/ash/Tools/bigdata/spark-2.2.1-bin-hadoop2.7/examples/src/main/resources/people.json";
        // "/Users/ash/Workspaces/BigData/spark/spring-spark-example/src/test/resources/data-1.json"
        Dataset<Row> df = sparkSession.read()
                .json(filename);
        df.createOrReplaceTempView("data");
//https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/functions.html
        //https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.sql.functions$
        String query =
                " SELECT date_format(current_date(), \"y-M-d\") dt , " +
                        " to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd') as `ts`, " +
                        " gpsd.Timestamp, " +
                        " gpsd.GPSLatitude, " +
                        " gpsd.GPSLongitude, " +
                        " gpsd.GPSSpeed " +
                        " FROM data " +
                        " LATERAL VIEW explode(GPSData) tab as gpsd ";
        sparkSession.sql(query).show();


        String sumQuery =
                " SELECT date_format(to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd'), \"d\") d , " +
                        " date_format(to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd'), \"M\") m , " +
                        " date_format(to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd'), \"y\") y , " +
                        " sum(gpsd.GPSSpeed) gps_speed_20 " +
                        " FROM data " +
                        " LATERAL VIEW explode(GPSData) tab as gpsd " +
                        "  GROUP BY d, m, y" ;
       // sparkSession.sql(sumQuery).show();


        String sumGroupByCaseQuery =
                " SELECT date_format(to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd'), \"d\") d , " +
                        " date_format(to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd'), \"M\") m , " +
                        " date_format(to_date(Timestamp(cast(gpsd.Timestamp/1000 as LONG)),'YYYY-MM-dd'), \"y\") y , " +
                        " sum(CASE WHEN gpsd.GPSSpeed > 22 THEN gpsd.GPSSpeed ELSE 0 END ) gps_speed_gt_22, " +
                        " sum(CASE WHEN gpsd.GPSSpeed > 20 AND gpsd.GPSSpeed < 23 THEN gpsd.GPSSpeed ELSE 0 END ) gps_speed_gt_20 " +
                        " FROM data " +
                        " LATERAL VIEW explode(GPSData) tab as gpsd " +
                        "  GROUP BY d, m, y" ;
        sparkSession.sql(sumGroupByCaseQuery).show();






        /*df.show();
        df.printSchema();
        df.select("GPSData");
        df.select(col("GPSData.GPSLatitude"), col("GPSData.Timestamp")).show();
*/
       /* df.createOrReplaceTempView("data");

        Dataset<Row> sqlDF = sparkSession.sql("SELECT GPSData FROM data");
        sqlDF.show();

        sqlDF.collectAsList().forEach(System.out::println);*/

       /* Dataset gpsDF = df.select(
                explode(col("GPSData"))).toDF();

        gpsDF.createGlobalTempView("gps_data");
        gpsDF.printSchema();*/
       // gpsDF.show();





        /*df.createGlobalTempView("data");

        Dataset<Row> gpsDataDF = sparkSession.sql("SELECT GPSData " +
                "  FROM global_temp.data");

        gpsDataDF.show();*/
       /* gpsDataDF.createGlobalTempView("gps_data");

        Dataset<Row> gpsRowsDS = sparkSession.sql("SELECT * " +
                "  FROM global_temp.gps_data");*/



    }

    public List<Count> count() {
        String input = "hello world hello hello hello";
        String[] _words = input.split(" ");
        List<Word> words = Arrays.stream(_words).map(Word::new).collect(Collectors.toList());
        Dataset<Row> dataFrame = sparkSession.createDataFrame(words, Word.class);
        dataFrame.show();
        //StructType structType = dataFrame.schema();

        RelationalGroupedDataset groupedDataset = dataFrame.groupBy(col("word"));
        groupedDataset.count().show();
        List<Row> rows = groupedDataset.count().collectAsList();//JavaConversions.asScalaBuffer(words)).count();
        return rows.stream().map(new Function<Row, Count>() {
            @Override
            public Count apply(Row row) {
                return new Count(row.getString(0), row.getLong(1));
            }
        }).collect(Collectors.toList());
    }
}