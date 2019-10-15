package ai.quod.challenge.database;
import java.io.IOException;
import java.sql.*;

import static ai.quod.challenge.database.InitDatabase.DB_NAME;

public class SQLiteConnection {
    private static final String DRIVER = "org.sqlite.JDBC";
    private static final String PATH = "jdbc:sqlite:";


    public Connection openConnection(String dbName) throws IOException {
        Connection connection = null;
        try {
            Class.forName(DRIVER);
            String url = PATH + dbName;
            connection = DriverManager.getConnection(url);
            connection.setAutoCommit(true);
        } catch (ClassNotFoundException|SQLException ex) {
            logException(ex);
        }
        return connection;
    }

    private void logException(final Exception ex) {
        System.err.println("SQLite > " + ex.getMessage());
        ex.printStackTrace();
    }

}
