package ai.quod.challenge.utils;
import java.sql.*;

public class SQLite {
    private static final String DRIVER = "org.sqlite.JDBC";
    private static final String PATH = "jdbc:sqlite:";
    private static final String dbName = "gharchive.db";

    public Connection openConnection() {
        String url = PATH + dbName;
        Connection connection = null;
        try {
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(url);
            connection.setAutoCommit(true);
            //this.statement = connection.createStatement();
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
