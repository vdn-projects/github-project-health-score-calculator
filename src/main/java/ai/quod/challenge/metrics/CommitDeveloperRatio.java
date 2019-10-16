package ai.quod.challenge.metrics;

import ai.quod.challenge.database.SQLiteConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static ai.quod.challenge.utils.FileHandling.SQLITE_DB_PATH;

public class CommitDeveloperRatio {
    public static void process() {
        try {
            System.out.println("Process ratio of commit per developers metric");
            insertData();
            insertMetric();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertData() throws SQLException {
        Connection connection = null;
        Statement stmt = null;
        String sql =
                "INSERT INTO commit_developer_ratio_data(org,repo_name,num_commits,num_developers,ratio) " +
                "SELECT 	org, " +
                "		repo_name, " +
                "		COUNT(*) num_commits, " +
                "		COUNT(DISTINCT(actor)) num_developers, " +
                "		COUNT(*)*1.0/COUNT(DISTINCT(actor)) ratio " +
                "FROM fact " +
                "WHERE \"type\" = 'PushEvent' " +
                "GROUP BY org,repo_name ";
        try {
            connection = new SQLiteConnection().openConnection(SQLITE_DB_PATH);
            stmt = connection.createStatement();
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stmt != null)
                stmt.close();
            if (connection != null)
                connection.close();
        }
    }

    private static void insertMetric() throws SQLException {
        Connection connection = null;
        Statement stmt = null;
        String sql1 =   "CREATE TEMP TABLE IF NOT EXISTS var (name TEXT PRIMARY KEY, value float);";
        String sql2 =   "INSERT OR REPLACE INTO var SELECT 'max_ratio', MAX(ratio) FROM commit_developer_ratio_data;";
        String sql3 =   "INSERT INTO commit_developer_ratio_metric(org,repo_name,ratio,max_ratio,metric) " +
                        "SELECT org, " +
                        "		repo_name, " +
                        "		ratio, " +
                        "		(SELECT value FROM var WHERE name = 'max_ratio') max_ratio, " +
                        "		ratio*1.0/(SELECT value FROM var WHERE name = 'max_ratio') metric " +
                        "FROM commit_developer_ratio_data; ";
        try {
            connection = new SQLiteConnection().openConnection(SQLITE_DB_PATH);
            connection.setAutoCommit(false);
            stmt = connection.createStatement();
            stmt.addBatch(sql1);
            stmt.addBatch(sql2);
            stmt.addBatch(sql3);
            stmt.executeBatch();
            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stmt != null)
                stmt.close();
            if (connection != null)
                connection.close();
        }
    }
}
