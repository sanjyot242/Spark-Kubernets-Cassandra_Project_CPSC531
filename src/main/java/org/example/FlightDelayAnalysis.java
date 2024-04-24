package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

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

        FlightDelayAnalysis analysis = new FlightDelayAnalysis();

        Dataset<Row> df = analysis.loadDataAndInitialTransform(spark);
         df = analysis.addDerivedColumns(df);
        analysis.saveToCassandra(df,"Flight_Delay_Analysis","Cleaned_data","ETL");
        Dataset<Row> monthlyDelays = analysis.calculateMonthlyAverageDelays(df);
        //Dataset<Row> timeSeriesData = analysis.prepareDelayTimeSeries(df);
        analysis.saveToCassandra(monthlyDelays, "Flight_Delay_Analysis", "monthly_delay_stats","Monthly_Delay");

//        // Read data from Cassandra
//        Dataset<Row> dataset = spark.read()
//                .format("org.apache.spark.sql.cassandra")
//                .option("keyspace", "test")
//                .option("table", "data")
//                .load();

        // Show the data read from Cassandra
        //dataset.show();

        // Stop Spark session
        spark.stop();
    }

    public Dataset<Row> loadDataAndInitialTransform(SparkSession spark) {
        Dataset<Row> df = spark.read().parquet("Flight_Delay.parquet");
        df = df.withColumnRenamed("FlightDate","flight_date")
                .withColumnRenamed("DepDelayMinutes","dep_delay_minutes")
                .withColumnRenamed("ArrDelayMinutes","arr_delay_minutes")
                .withColumn("flight_date",to_date(col("flight_date"),"yyyy-MM-dd"));
        return df;
    }

    public Dataset<Row> addDerivedColumns(Dataset<Row> df) {
        df = df.withColumn("day_of_week", dayofweek(col("flight_date")))
                .withColumn("month", month(col("flight_date")));
        return df;
    }

    public Dataset<Row> calculateMonthlyAverageDelays(Dataset<Row> df) {
        Dataset<Row> monthlyAvgDelays = df.groupBy(col("month"))
                .agg(avg("dep_delay_minutes").alias("average_departure_delay"),
                        avg("arr_delay_minutes").alias("average_arrival_delay"));
        return monthlyAvgDelays;
    }



    public void saveToCassandra(Dataset<Row> df, String keyspace, String table,String analysisName) {
        try(CqlSession session = cassandraUtil.createSession()) {
            createKeyspaceIfNotExists(session,keyspace);
            cassandraUtil.dropTable(session,keyspace,table);
            cassandraUtil.createTable(session,keyspace,table,analysisName);


            df.write()
                    .format("org.apache.spark.sql.cassandra")
                    .option("keyspace", keyspace)
                    .option("table", table)
                    .mode(SaveMode.Append)
                    .save();
        }catch(Exception e){

        }

    }

    private static SparkConf setupSparkConf(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark Cassandra Integration");
        conf.set("spark.master", "local");
        conf.set("spark.cassandra.connection.host", args[0]);
        conf.set("spark.cassandra.connection.port", args[1]);// Assuming the port is passed as the second argument
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