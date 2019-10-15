package ai.quod.challenge.metrics;

import ai.quod.challenge.database.SQLiteConnection;
import ai.quod.challenge.utils.FileHandling;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static ai.quod.challenge.database.InitDatabase.DB_NAME;

public class HealthMetrics {
    public static void exportHealthMetric() throws SQLException {
        Connection connection = null;
        Statement stmt = null;
        String sql =
                "SELECT 	h.org,  " +
                "		h.repo_name, " +
                "		printf(\"%.2f\", (ifnull(cc.metric,0) + ifnull(io.metric,0) + ifnull(pr.metric,0) + ifnull(cd.metric,0))) health_score, " +
                "		 " +
                "		cc.num_commits, " +
                "		cc.max_num_commits, " +
                "		cc.metric commit_count_metric, " +
                "		 " +
                "		io.avg_opened_duration, " +
                "		io.min_opened_duration, " +
                "		io.metric opened_issued_metric, " +
                "		 " +
                "		pr.avg_merged_duration, " +
                "		pr.min_merged_duration, " +
                "		pr.metric merged_pr_metric, " +
                "		 " +
                "		cd.ratio, " +
                "		cd.max_ratio, " +
                "		cd.metric commit_developer_metric " +
                "FROM org_repo h " +
                "LEFT JOIN commit_count_metric cc ON cc.org = h.org AND cc.repo_name = h.repo_name " +
                "LEFT JOIN issue_opened_metric io ON io.org = h.org AND io.repo_name = h.repo_name " +
                "LEFT JOIN pr_merged_metric pr ON pr.org = h.org AND pr.repo_name = h.repo_name " +
                "LEFT JOIN commit_developer_ratio_metric cd ON cd.org = h.org AND cd.repo_name = h.repo_name " +
                "ORDER BY health_score DESC " +
                "LIMIT 1000; ";

        try {
            connection = new SQLiteConnection().openConnection(DB_NAME);
            stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            FileHandling.extractCsv(resultSet, "output.csv");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stmt != null)
                stmt.close();
            if (connection != null)
                connection.close();
        }
    }

    public static void insertOrgRepo() throws SQLException {
        Connection connection = null;
        Statement stmt = null;
        String sql =
                "INSERT INTO org_repo(org,repo_name) " +
                "SELECT DISTINCT org,repo_name " +
                "FROM fact " +
                "WHERE \"type\" IN ('PushEvent', 'IssuesEvent', 'PullRequestEvent') ";

        try {
            connection = new SQLiteConnection().openConnection(DB_NAME);
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
}
