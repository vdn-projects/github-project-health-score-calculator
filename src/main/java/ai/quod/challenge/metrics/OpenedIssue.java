package ai.quod.challenge.metrics;

import ai.quod.challenge.database.SQLiteConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static ai.quod.challenge.utils.FileHandling.SQLITE_DB_PATH;

public class OpenedIssue {
    public static void process(long nowTick) {
        try {
            System.out.println("Process opened issue duration metric");
            insertData(nowTick);
            insertMetric();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private static void insertData(long nowTick) throws SQLException {
        Connection connection = null;
        PreparedStatement pstmt = null;
        String sql =
                //One issue is considered as opened only if the latest payload_action is 'closed'
                "INSERT INTO issue_opened_data(org,repo_name,payload_no,opened_at,now,opened_duration) " +
                "SELECT org,repo_name,payload_no,opened_at,? now,(?-opened_at) opened_duration " +
                "FROM " +
                "	(SELECT 	org," +
                "			repo_name," +
                "			payload_no," +
                "			MIN(CASE WHEN payload_action='opened' THEN created_at END) opened_at," +
                "			MAX(CASE WHEN payload_action='reopened' THEN created_at END) reopened_at," +
                "			MAX(CASE WHEN payload_action='closed' THEN created_at END) closed_at " +
                "	FROM " +
                "		(SELECT org,repo_name,payload_no,payload_action,created_at " +
                "		FROM fact " +
                "		WHERE \"type\"='IssuesEvent' " +
                "		) t1 " +
                "	GROUP BY org,repo_name,payload_no " +
                "	) t2 " +
                "WHERE opened_at IS NOT NULL " +
                "AND	(	(reopened_at IS NULL AND closed_at IS NULL) " +
                "		OR (reopened_at IS NOT NULL AND closed_at IS NOT NULL AND reopened_at > closed_at));";
        try {
            connection = new SQLiteConnection().openConnection(SQLITE_DB_PATH);
            pstmt = connection.prepareStatement(sql);
            pstmt.setLong(1,nowTick);
            pstmt.setLong(2,nowTick);
            pstmt.execute();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (pstmt != null)
                pstmt.close();
            if (connection != null)
                connection.close();
        }
    }

    private static void insertMetric() throws SQLException {
        Connection connection = null;
        Statement stmt = null;
        String sql1 =   "CREATE TEMP TABLE IF NOT EXISTS var (name TEXT PRIMARY KEY, value int);";
        String sql2 =   "INSERT OR REPLACE INTO var SELECT 'min_opened_duration', MIN(opened_duration) FROM issue_opened_data;";
        String sql3 =   "INSERT INTO issue_opened_metric(org,repo_name,avg_opened_duration,min_opened_duration,metric) " +
                        "SELECT 	org," +
                        "		repo_name," +
                        "		AVG(opened_duration) avg_opened_duration," +
                        "		(SELECT value FROM var WHERE name='min_opened_duration') min_opened_duration," +
                        "		(SELECT value FROM var WHERE name='min_opened_duration')/AVG(opened_duration) metric " +
                        "FROM issue_opened_data " +
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
