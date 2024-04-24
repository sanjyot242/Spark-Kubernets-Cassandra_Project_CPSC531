package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.net.InetSocketAddress;

public class CassandraUtil {
    private String ipAddress;
    private int port;


    public CassandraUtil(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;

    }

    public CqlSession createSession() {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(ipAddress, port))
                .withLocalDatacenter("datacenter1")
                .build();
    }

    public void dropTable(CqlSession session, String keyspace, String tableName) {
        session.execute(SchemaBuilder.dropTable(keyspace, tableName).ifExists().build());
    }

    public void createTable(CqlSession session, String keyspace, String tableName, String analysisType) {
        switch (analysisType.toLowerCase()) {
            case "etl":
                configureEtlTable(session, keyspace, tableName);
                break;
            case "monthly_delay":
                configureMonthlyDelayTable(session, keyspace, tableName);
                break;
            default:
                throw new IllegalArgumentException("Unsupported analysis type: " + analysisType);
        }
        System.out.println("Table created for " + analysisType + " analysis.");
    }

    private void configureEtlTable(CqlSession session, String keyspace, String tableName) {
        CreateTableWithOptions createTable = SchemaBuilder.createTable(keyspace, tableName)
                .ifNotExists()
                .withPartitionKey("year", DataTypes.INT)
                .withPartitionKey("month", DataTypes.INT)
                .withPartitionKey("day_of_month", DataTypes.INT)
                .withColumn("flight_date", DataTypes.TEXT)
                .withColumn("Marketing_Airline_Network", DataTypes.TEXT)
                .withColumn("OriginCityName", DataTypes.TEXT)
                .withColumn("DestCityName", DataTypes.TEXT)
                .withColumn("CRSDepTime", DataTypes.INT)
                .withColumn("DepTime", DataTypes.DOUBLE)
                .withColumn("DepDelay", DataTypes.DOUBLE)
                .withColumn("dep_delay_minutes", DataTypes.DOUBLE)
                .withColumn("TaxiOut", DataTypes.DOUBLE)
                .withColumn("WheelsOff", DataTypes.DOUBLE)
                .withColumn("WheelsOn", DataTypes.DOUBLE)
                .withColumn("TaxiIn", DataTypes.DOUBLE)
                .withColumn("CRSArrTime", DataTypes.INT)
                .withColumn("ArrTime", DataTypes.DOUBLE)
                .withColumn("ArrDelay", DataTypes.DOUBLE)
                .withColumn("arr_delay_minutes", DataTypes.DOUBLE)
                .withColumn("CRSElapsedTime", DataTypes.DOUBLE)
                .withColumn("ActualElapsedTime", DataTypes.DOUBLE)
                .withColumn("AirTime", DataTypes.DOUBLE)
                .withColumn("Distance", DataTypes.DOUBLE)
                .withColumn("DistanceGroup", DataTypes.INT)
                .withColumn("CarrierDelay", DataTypes.DOUBLE)
                .withColumn("WeatherDelay", DataTypes.DOUBLE)
                .withColumn("NASDelay", DataTypes.DOUBLE)
                .withColumn("SecurityDelay", DataTypes.DOUBLE)
                .withColumn("LateAircraftDelay", DataTypes.DOUBLE);
        session.execute(createTable.build());
        System.out.println("Table checked/created");
    }

    private void configureMonthlyDelayTable(CqlSession session, String keyspace, String tableName) {
        CreateTableWithOptions createTable = SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
                .withPartitionKey("month", DataTypes.INT)
                .withColumn("average_departure_delay", DataTypes.DOUBLE)
                .withColumn("average_arrival_delay", DataTypes.DOUBLE);
        session.execute(createTable.build());
        System.out.println("Table checked/created");
    }


}
