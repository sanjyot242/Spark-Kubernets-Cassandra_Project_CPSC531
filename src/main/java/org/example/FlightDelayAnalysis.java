package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class FlightDelayAnalysis {
    private static CassandraUtil cassandraUtil;
    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: Main <CassandraHost> <CassandraPort>");
            System.exit(1);
        }

        cassandraUtil = new CassandraUtil(args[0], Integer.parseInt(args[1]));

        // Setup Spark configuration
        SparkConf conf = setupSparkConf(args);
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();



        //spark.sparkContext().setLogLevel("DEBUG");

        FlightDelayAnalysis analysis = new FlightDelayAnalysis();

        Dataset<Row> df = analysis.loadDataAndInitialTransform(spark);

        df = analysis.addDerivedColumns(df);
        //analysis.saveToCassandra(df,"flight_delay_analysis","cleaned_data","etl");

        Dataset<Row> reducedDf = df.select("flight_month","origin_city_name","marketing_airline_network", "carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay","dep_delay_minutes","arr_delay_minutes");



        //df.show(5);

        Dataset<Row> monthlyDelays = analysis.calculateMonthlyAverageDelays(reducedDf);
        Dataset<Row> avgAirportDelay = analysis.calculateAirportWithMaxDelay(reducedDf);
        Dataset<Row> avgAirLineDelay = analysis.calculateAirLineDelay(reducedDf);
        //Dataset<Row> timeSeriesData = analysis.prepareDelayTimeSeries(df);
        //monthlyDelays.printSchema();
        analysis.saveToCassandra(monthlyDelays, "flight_delay_analysis", "monthly_delay_stats","Monthly_Delay");
        analysis.saveToCassandra(avgAirportDelay,"flight_delay_analysis", "airport_delay_stats","Airport_Delay");
        analysis.saveToCassandra(avgAirLineDelay,"flight_delay_analysis","airline_delay_stats","Airline_Delay");

        spark.stop();
    }

    public Dataset<Row> loadDataAndInitialTransform(SparkSession spark) {
        Dataset<Row> df = spark.read().parquet("Flight_Delay.parquet");
        df = df.drop("TaxiOut").drop("WheelsOff").drop("WheelsOn").drop("TaxiIn")
                .drop("Distance").drop("DistanceGroup").drop("AirTime").drop("CRSDepTime")
                .drop("CRSArrTime");
        df = df.withColumnRenamed("FlightDate","flight_date")
                .withColumnRenamed("DepDelayMinutes","dep_delay_minutes")
                .withColumnRenamed("OriginCityName","origin_city_name")
                .withColumnRenamed("DestCityName","dest_city_name")
                .withColumnRenamed("DepTime","dep_time")
                .withColumnRenamed("DepDelay","dep_delay")
                .withColumnRenamed("ArrDelayMinutes","arr_delay_minutes")
                .withColumnRenamed("ArrTime","arr_time")
                .withColumnRenamed("ArrDelay","arr_delay")
                .withColumnRenamed("ArrDelayMinutes","arr_delay_minutes")
                .withColumnRenamed("CRSElapsedTime","crs_elapsed_time")
                .withColumnRenamed("ActualElapsedTime","actual_elapsed_time")
                .withColumnRenamed("CarrierDelay","carrier_delay")
                .withColumnRenamed("WeatherDelay","weather_delay")
                .withColumnRenamed("NASDelay","nas_delay")
                .withColumnRenamed("SecurityDelay","security_delay")
                .withColumnRenamed("LateAircraftDelay","late_aircraft_delay")
                .withColumnRenamed("DayofMonth","day_of_month")
                .withColumnRenamed("Year","year")
                .withColumnRenamed("Marketing_Airline_Network","marketing_airline_network")
                .withColumnRenamed("Month","month")
                .withColumn("flight_date",to_date(col("flight_date"),"yyyy-MM-dd"));
        return df;
    }

    public Dataset<Row> addDerivedColumns(Dataset<Row> df) {
        df = df.withColumn("day_of_week", dayofweek(col("flight_date")))
                .withColumn("flight_month", month(col("flight_date")));
        return df;
    }

    public Dataset<Row> calculateMonthlyAverageDelays(Dataset<Row> df) {
        Dataset<Row> monthlyAvgDelays = df.groupBy(col("flight_month"))
                .agg(avg("dep_delay_minutes").alias("average_departure_delay"),
                        avg("arr_delay_minutes").alias("average_arrival_delay"));
        return monthlyAvgDelays;
    }

    public Dataset<Row> calculateAirportWithMaxDelay(Dataset<Row> df) {
        Dataset<Row> airportWithMaxDelay = df.groupBy(col("origin_city_name"))
                .agg(avg("dep_delay_minutes").alias("average_departure_delay"),
                        avg("arr_delay_minutes").alias("average_arrival_delay"));
        return airportWithMaxDelay;
    }

    public Dataset<Row> calculateAirLineDelay(Dataset<Row> df) {
        Dataset<Row> airlineWithMaxDelay = df.groupBy(col("marketing_airline_network"))
                .agg(avg("dep_delay_minutes").alias("average_departure_delay"),
                        avg("arr_delay_minutes").alias("average_arrival_delay"),
                        avg("carrier_delay").alias("average_carrier_delay"),
                        avg("weather_delay").alias("average_weather_delay"),
                        avg("nas_delay").alias("average_nas_delay"),
                        avg("security_delay").alias("average_security_delay"),
                        avg("late_aircraft_delay").alias("average_late_aircraft_delay"));
        return airlineWithMaxDelay;
    }







    public void saveToCassandra(Dataset<Row> df, String keyspace, String table,String analysisName) {
        try(CqlSession session = cassandraUtil.createSession()) {
            createKeyspaceIfNotExists(session, keyspace);
            cassandraUtil.dropTable(session,keyspace,table);
            cassandraUtil.createTable(session,keyspace,table,analysisName);

            if (Arrays.asList(df.columns()).contains("__index_level_0__")) {
                df = df.drop("__index_level_0__");
            }
            df.write()
                    .format("org.apache.spark.sql.cassandra")
                    .option("keyspace", keyspace)
                    .option("table", table)
                    .mode(SaveMode.Append)
                    .save();
        }catch(Exception e){
            e.printStackTrace();
        }

    }

    private static SparkConf setupSparkConf(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark Cassandra Integration");
        conf.set("spark.master", "local");
        conf.set("spark.cassandra.connection.host", args[0]);
        conf.set("spark.cassandra.connection.port", args[1]);
        conf.set("spark.cassandra.connection.timeout_ms", "5000") ; // Increase if needed
        conf.set("spark.cassandra.read.timeout_ms", "20000");
        return conf;
    }

    private void createKeyspaceIfNotExists(CqlSession session,String KeySpace) {
        session.execute(SchemaBuilder.createKeyspace(KeySpace)
                .ifNotExists()
                .withSimpleStrategy(1)
                .build());
        System.out.println("Keyspace checked/created");
    }

//    private void createTableIfNotExists(CqlSession session,String Keyspace ,String Table) {
//        cassandraUtil.dropTable(session,Keyspace,Table);
//        cassandraUtil.createTable(session,Keyspace,Table);
//        session.execute(SchemaBuilder.createTable(Keyspace, Table)
//                .ifNotExists()
//                .withPartitionKey("month", DataTypes.INT)
//                .withColumn("average_departure_delay", DataTypes.DOUBLE)
//                .withColumn("average_arrival_delay", DataTypes.DOUBLE)
//                .build());
//        System.out.println("Table checked/created");
//    }
}