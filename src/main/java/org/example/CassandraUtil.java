package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.net.InetSocketAddress;
import java.time.Duration;

public class CassandraUtil {
    private String ipAddress;
    private int port;


    public CassandraUtil(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;

    }

    public CqlSession createSession() {
        DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
                .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(10))
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(10)) // Increase timeout to 10 seconds
                .build();
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(ipAddress, port))
                .withLocalDatacenter("datacenter1")
                .withConfigLoader(loader)
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
            case"airport_delay":
                configureAirportDelayTable(session, keyspace, tableName);
                break;
            case"airline_delay":
                configureAirlineDelayTable(session, keyspace, tableName);
                break;
            default:
                throw new IllegalArgumentException("Unsupported analysis type: " + analysisType);
        }
        System.out.println("Table created for " + analysisType + " analysis.");
    }

    private void configureAirportDelayTable(CqlSession session, String keyspace, String tableName) {
        CreateTableWithOptions createTable = SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
                .withPartitionKey("origin_city_name", DataTypes.TEXT)
                .withColumn("average_departure_delay", DataTypes.DOUBLE)
                .withColumn("average_arrival_delay", DataTypes.DOUBLE);
        session.execute(createTable.build());
        System.out.println("Table checked/created");
    }

    private void configureAirlineDelayTable(CqlSession session, String keyspace, String tableName) {
        CreateTableWithOptions createTable = SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
                .withPartitionKey("marketing_airline_network", DataTypes.TEXT)
                .withColumn("average_departure_delay", DataTypes.DOUBLE)
                .withColumn("average_arrival_delay", DataTypes.DOUBLE)
                .withColumn("average_carrier_delay", DataTypes.DOUBLE)
                .withColumn("average_weather_delay", DataTypes.DOUBLE)
                .withColumn("average_nas_delay", DataTypes.DOUBLE)
                .withColumn("average_security_delay", DataTypes.DOUBLE)
                .withColumn("average_late_aircraft_delay", DataTypes.DOUBLE);
        session.execute(createTable.build());
        System.out.println("Table checked/created");
    }

    private void configureEtlTable(CqlSession session, String keyspace, String tableName) {
        CreateTableWithOptions createTable = SchemaBuilder.createTable(keyspace, tableName)
                .ifNotExists()
                .withPartitionKey("year", DataTypes.INT) // Year: converting long to int
                .withPartitionKey("month", DataTypes.INT) // Month: already integer
                .withPartitionKey("day_of_month", DataTypes.INT)
                .withColumn("flight_date", DataTypes.DATE) // flight_date: date
                .withColumn("marketing_airline_network", DataTypes.TEXT) // Marketing_Airline_Network: string
                .withColumn("origin_city_name", DataTypes.TEXT) // OriginCityName: string
                .withColumn("dest_city_name", DataTypes.TEXT) // DestCityName: string
                .withColumn("dep_time", DataTypes.DOUBLE) // DepTime: double
                .withColumn("dep_delay", DataTypes.DOUBLE) // DepDelay: double
                .withColumn("dep_delay_minutes", DataTypes.DOUBLE) // dep_delay_minutes: double
                .withColumn("arr_time", DataTypes.DOUBLE) // ArrTime: double
                .withColumn("arr_delay", DataTypes.DOUBLE) // ArrDelay: double
                .withColumn("arr_delay_minutes", DataTypes.DOUBLE) // arr_delay_minutes: double
                .withColumn("crs_elapsed_time", DataTypes.DOUBLE) // CRSElapsedTime: double
                .withColumn("actual_elapsed_time", DataTypes.DOUBLE) // ActualElapsedTime: double
                .withColumn("carrier_delay", DataTypes.DOUBLE) // CarrierDelay: double
                .withColumn("weather_delay", DataTypes.DOUBLE) // WeatherDelay: double
                .withColumn("nas_delay", DataTypes.DOUBLE) // NASDelay: double
                .withColumn("security_delay", DataTypes.DOUBLE) // SecurityDelay: double
                .withColumn("late_aircraft_delay", DataTypes.DOUBLE)
                .withColumn("day_of_week",DataTypes.TEXT)
                .withColumn("flight_month",DataTypes.TEXT);// LateAircraftDelay: double

        session.execute(createTable.build());
        System.out.println("Table checked/created");
    }

    private void configureMonthlyDelayTable(CqlSession session, String keyspace, String tableName) {
        CreateTableWithOptions createTable = SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
                .withPartitionKey("flight_month", DataTypes.INT)
                .withColumn("average_departure_delay", DataTypes.DOUBLE)
                .withColumn("average_arrival_delay", DataTypes.DOUBLE);
        session.execute(createTable.build());
        System.out.println("Table checked/created");
    }


}
