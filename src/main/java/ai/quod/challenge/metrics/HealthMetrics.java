package ai.quod.challenge.metrics;

import ai.quod.challenge.database.SQLiteConnection;
import ai.quod.challenge.utils.FileHandling;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static ai.quod.challenge.utils.FileHandling.SQLITE_DB_PATH;

public class HealthMetrics {

    public static void exportHealthMetric() throws SQLException, IOException {
        System.out.println("Exporting health metric ...");
        Path dbFilePath = FileSystems.getDefault().getPath(FileHandling.HEALTH_SCORE_OUTPUT_PATH);
        Files.deleteIfExists(dbFilePath);
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
            connection = new SQLiteConnection().openConnection(SQLITE_DB_PATH);
            stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            FileHandling.extractCsv(resultSet, FileHandling.HEALTH_SCORE_OUTPUT_PATH);
            Desktop.getDesktop().open(new File(FileHandling.OUTPUT_PATH));
            System.out.println("Health metric result file is placed in: " + FileHandling.HEALTH_SCORE_OUTPUT_PATH);
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
}
