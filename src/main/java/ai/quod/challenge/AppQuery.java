package ai.quod.challenge;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class AppQuery {
    private static final String DRIVER = "org.sqlite.JDBC";
    private static final String PATH = "jdbc:sqlite:";
    private Connection connection;
    private Statement statement;

    AppQuery(String dbName) {
        openConnection(dbName);
    }

    /**
     * Opens a connection to the SQLite database.
     * @param dbName the name of the database file (don't include file extension)
     */
    private void openConnection(String dbName) {
        try {
            Class.forName(DRIVER);
            this.connection = DriverManager.getConnection(PATH + dbName);
            this.connection.setAutoCommit(false);
            this.statement = connection.createStatement();
        } catch (ClassNotFoundException|SQLException ex) {
            logException(ex);
        }
    }

    /**
     * Executes a command on the SQLite database using a raw SQL query.
     * @param sql the SQL query to run
     */
    public void execSql(String sql) {
        try {
            this.statement.executeUpdate(sql);
            this.connection.commit();
        } catch (SQLException ex) {
            logException(ex);
        } finally {
//            releaseReference();
        }
    }

    private void logException(final Exception ex) {
        System.err.println("SQLite > " + ex.getMessage());
        ex.printStackTrace();
    }

}
