package ai.quod.challenge.metrics;

import ai.quod.challenge.database.SQLiteConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static ai.quod.challenge.utils.FileHandling.SQLITE_DB_PATH;

public class MergedPullRequest {
    public static void process() {
        try {
            System.out.println("Process merged pull request duration metric");
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
                "INSERT INTO pr_merged_data(org,repo_name,payload_no,opened_at,merged_at,merged_duration) " +
                "SELECT org,repo_name,payload_no,opened_at,merged_at,(merged_at-opened_at) merged_duration " +
                "FROM " +
                "	(SELECT 	org," +
                "			repo_name," +
                "			payload_no," +
                "			MIN(CASE WHEN payload_action='opened' THEN created_at END) opened_at, " +
                "			MAX(CASE WHEN payload_action='closed' THEN created_at END) merged_at " +
                "	FROM " +
                "		(SELECT org,repo_name,payload_no,payload_action,is_merged,created_at " +
                "		FROM fact" +
                "		WHERE \"type\"='PullRequestEvent'" +
                "		AND payload_action = 'opened'" +
                "		OR (payload_action = 'closed' AND is_merged = 1)" +
                "		) t1 " +
                "	GROUP BY org,repo_name,payload_no" +
                "	) t2 " +
                "WHERE opened_at IS NOT NULL " +
                "AND	merged_at IS NOT NULL";
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
        String sql1 =   "CREATE TEMP TABLE IF NOT EXISTS var (name TEXT PRIMARY KEY, value int);";
        String sql2 =   "INSERT OR REPLACE INTO var SELECT 'min_merged_duration', MIN(merged_duration) FROM pr_merged_data;";
        String sql3 =   "INSERT INTO pr_merged_metric(org,repo_name,avg_merged_duration,min_merged_duration,metric) " +
                        "SELECT 	org," +
                        "		repo_name," +
                        "		AVG(merged_duration) avg_merged_duration," +
                        "		(SELECT value FROM var WHERE name='min_merged_duration') min_merged_duration," +
                        "		(SELECT value FROM var WHERE name='min_merged_duration')/AVG(merged_duration) metric " +
                        "FROM pr_merged_data " +
                        "GROUP BY org,repo_name;";
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
