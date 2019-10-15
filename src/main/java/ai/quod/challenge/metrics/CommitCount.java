package ai.quod.challenge.metrics;

import ai.quod.challenge.database.SQLiteConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static ai.quod.challenge.database.InitDatabase.DB_NAME;

public class CommitCount {
    public static void process(double num_of_days) {
        try {
            insertData(num_of_days);
            insertMetric();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertData(double num_of_days) throws SQLException {
        Connection connection = null;
        PreparedStatement pstmt = null;
        String sql =
                //num_commits are calculated as average number of commit by day
                "INSERT INTO commit_count_data(org,repo_name,num_commits) " +
                "SELECT org,repo_name,COUNT(*)/? num_commits " +
                "FROM fact " +
                "WHERE \"type\"='PushEvent' " +
                "GROUP BY org, repo_name";
        try {
            connection = new SQLiteConnection().openConnection(DB_NAME);
            pstmt = connection.prepareStatement(sql);
            pstmt.setDouble(1,num_of_days);
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

        //Get max number of commits and put into an variable to improve insert processing time
        String sql1 =   "CREATE TEMP TABLE IF NOT EXISTS var (name TEXT PRIMARY KEY, value int);";
        String sql2 =   "INSERT OR REPLACE INTO var SELECT 'max_num_commits', MAX(num_commits) FROM commit_count_data;";
        String sql3 =   "INSERT INTO commit_count_metric(org,repo_name,num_commits,max_num_commits, metric) " +
                        "SELECT 	org," +
                        "		repo_name," +
                        "		num_commits," +
                        "		(SELECT value FROM var WHERE name='max_num_commits') max_num_commits," +
                        "		num_commits*1.0/(SELECT value FROM var WHERE name='max_num_commits') metric " +
                        "FROM commit_count_data;";
        try {
            connection = new SQLiteConnection().openConnection(DB_NAME);
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
