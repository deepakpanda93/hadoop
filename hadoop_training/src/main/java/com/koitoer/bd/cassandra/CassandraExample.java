package com.koitoer.bd.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * To connect to the cassandra box
 * rpc_address: 0.0.0.0 broadcast_rpc_address: 1.2.3.4
 * sudo iptables -A INPUT -i eth0 -j ACCEPT
 * Created by mauricio.mena on 08/09/2016.
 */
public class CassandraExample {

    public static void main(String[] args) {
        String node = "10.128.3.188";
        Cluster cluster = Cluster.builder().addContactPoint(node).build();
        Session session = cluster.connect();

        ResultSet resultSet = session.execute("SELECT ticker, value FROM testdb.stocks;");
        for(Row row : resultSet){
            System.out.println(String.format("%-10s\t%-10f", row.getString("ticker"), row.getDouble("value")));
        }

        cluster.close();
    }
}
