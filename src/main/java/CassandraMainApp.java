import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import static java.lang.System.out;

/**
 * This is a simple program to work with Cassandra on AWS.
 * Created by stevesun on 05/09/17.
 */
public class CassandraMainApp {
    public static void main(String[] args) {
        out.println("Entered.");
//        moreVerboseConstructor();
        doingNow();
        out.println("Finished.");
    }

    private static void moreVerboseConstructor() {
//        final CassandraConnectorV0 client = new CassandraConnectorV0();
//        final String ipAddress = args.length > 0 ? args[0] : "localhost";
//        final int port = args.length > 1 ? Integer.parseInt(args[1]) : 9042;
//        out.println("Connecting to IP Address " + ipAddress + ":" + port + "...");
//        client.connect(ipAddress, port);
//        Session session = client.getSession();
//        client.close();
    }

    private static void doingNow() {
        String serverIP = "127.0.0.1";
        String keyspace = "system";
        Cluster cluster = Cluster.builder()
                .addContactPoints(serverIP)
                .build();
        Session session = cluster.connect(keyspace);
        String cqlStatement = "SELECT * FROM local";
        for (Row row : session.execute(cqlStatement)) {
            System.out.println(row.toString());
        }

        // for all three it works the same way (as a note the 'system' keyspace cant
// be modified by users so below im using a keyspace name 'exampkeyspace' and
// a table (or columnfamily) called users

        String cqlStatementC = "INSERT INTO exampkeyspace.users (username, password) " +
                "VALUES ('Serenity', 'fa3dfQefx')";

        String cqlStatementU = "UPDATE exampkeyspace.users " +
                "SET password = 'zzaEcvAf32hla'," +
                "WHERE username = 'Serenity';";

        String cqlStatementD = "DELETE FROM exampkeyspace.users " +
                "WHERE username = 'Serenity';";

        session.execute(cqlStatementC);
        System.out.println("executed cqlStatementC");
        session.execute(cqlStatementD);
        System.out.println("executed cqlStatementD");

        //creating a keyspace
        String cqlStatementCreateKeyspace = "CREATE KEYSPACE exampkeyspace WITH " +
                "replication = {'class':'SimpleStrategy','replication_factor':1}";
        session.execute(cqlStatementCreateKeyspace);
        System.out.println("executed cqlStatementCreateKeyspace");

        //create a ColumnFamily (aka table)
        // based on the above keyspace, we would change the cluster and session as follows:
        session = cluster.connect("exampkeyspace");

        String cqlStatementCreateColumnFamily = "CREATE TABLE users (" +
                " username varchar PRIMARY KEY," +
                " password varchar " +
                ");";
        session.execute(cqlStatementCreateColumnFamily);
        System.out.println("executed cqlStatementCreateColumnFamily");

    }
}
