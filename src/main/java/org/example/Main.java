package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    private static SparkConf setupSparkConf(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark Cassandra Integration");
        conf.set("spark.master", "local");
        conf.set("spark.cassandra.connection.host", args[0]);
        conf.set("spark.cassandra.connection.port", args[1]);  // Assuming the port is passed as the second argument
        return conf;
    }
    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: Main <CassandraHost> <CassandraPort>");
            System.exit(1);
        }

        // Setup Spark configuration
        SparkConf conf = setupSparkConf(args);
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // Read data from Cassandra
        Dataset<Row> dataset = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "test")
                .option("table", "data")
                .load();

        // Show the data read from Cassandra
        dataset.show();

        // Stop Spark session
        spark.stop();
    }
}