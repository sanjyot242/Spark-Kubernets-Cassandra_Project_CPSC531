package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;

import java.net.InetSocketAddress;

public class CassandraUtil {
    private static String ipAddress;
    private static int port;

    public CassandraUtil(String ipAddress, int port) {
        CassandraUtil.ipAddress = ipAddress;
        CassandraUtil.port = port;
    }

    public static CqlSession createSession() {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(ipAddress,port ))
                .withLocalDatacenter("datacenter1")
                .build();
    }

    public static void dropTable(CqlSession session, String keyspace, String tableName) {
        Drop dropTable = SchemaBuilder.dropTable(keyspace, tableName).ifExists();
        session.execute(dropTable.build());
    }
}
