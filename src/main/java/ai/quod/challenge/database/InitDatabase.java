package ai.quod.challenge.database;

import ai.quod.challenge.utils.FileHandling;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class InitDatabase {

    private final static String createFact =
            "CREATE TABLE fact(" +
            "id text PRIMARY KEY," +
            "org text," +
            "repo_name text," +
            "type text," +
            "actor text," +
            "payload_no int," +
            "payload_action text," +
            "is_merged boolean," +
            "created_at datetime" +
            ")";

    private final static String createOrgRepo =
            "CREATE TABLE org_repo(" +
            "org text," +
            "repo_name text" +
            ")";

    private final static String createHealthMetric =
            "CREATE TABLE health_metric(" +
            "org text," +
            "repo_name text," +
            "health_score float," +

            //Number of commits (push event)
            "num_commits int," +
            "max_num_commits int," +
            "commit_count_metric float," +

            //Duration from opened to closed for an issue
            "avg_opened_issued_duration float," +
            "min_opened_issued_duration float," +
            "opened_issued_metric float," +

            //Duration from opened to merged for a pull request
            "avg_merged_pr_duration float," +
            "min_merged_pr_duration float," +
            "merged_pr_metric float," +

            //Ration of commit per developers
            "commit_developer_ratio float," +
            "max_commit_developer_ratio float," +
            "commit_developer_metric float," +

            "PRIMARY KEY(org, repo_name)" +
            ")";

    private final static String createCommitData =
            "CREATE TABLE commit_count_data(" +
            "org text," +
            "repo_name text," +
            "num_commits int," +
            "PRIMARY KEY(org, repo_name)" +
            ")";

    private final static String createCommitMetric =
            "CREATE TABLE commit_count_metric(" +
            "org text," +
            "repo_name text," +
            "num_commits int," +
            "max_num_commits int," +
            "metric float," +
            "PRIMARY KEY(org, repo_name)" +
            ")";

    private final static String createIssueOpenedData =
            "CREATE TABLE issue_opened_data(" +
            "org text," +
            "repo_name text," +
            "payload_no text," +
            "opened_at datetime," +
            "now datetime," +
            "opened_duration int," +
            "PRIMARY KEY(org,repo_name,payload_no)" +
            ")";

    private final static String createIssueOpenedMetric =
            "CREATE TABLE issue_opened_metric(" +
            "org text," +
            "repo_name text," +
            "avg_opened_duration int," +
            "min_opened_duration int," +
            "metric float," +
            "PRIMARY KEY(org, repo_name)" +
            ")";

    private final static String createPRMergedData =
            "CREATE TABLE pr_merged_data(" +
            "org text," +
            "repo_name text," +
            "payload_no text," +
            "opened_at datetime," +
            "merged_at datetime," +
            "merged_duration int," +
            "PRIMARY KEY(org,repo_name,payload_no)" +
            ")";

    private final static String createPRMergedMetric =
            "CREATE TABLE pr_merged_metric(" +
            "org text," +
            "repo_name text," +
            "avg_merged_duration int," +
            "min_merged_duration int," +
            "metric float," +
            "PRIMARY KEY(org, repo_name)" +
            ")";

    private final static String createCommitDeveloperRatioData =
            "CREATE TABLE commit_developer_ratio_data(" +
            "org text," +
            "repo_name text," +
            "num_commits int," +
            "num_developers int," +
            "ratio float," +
            "PRIMARY KEY(org,repo_name)" +
            ")";

    private final static String createCommitDeveloperRatioMetric =
            "CREATE TABLE commit_developer_ratio_metric(" +
            "org text," +
            "repo_name text," +
            "ratio float," +
            "max_ratio float," +
            "metric float," +
            "PRIMARY KEY(org, repo_name)" +
            ")";


    public static void  createTables() throws IOException {
        Path dbFilePath = FileSystems.getDefault().getPath(FileHandling.DB_PATH);
        Files.deleteIfExists(dbFilePath);

        execStmtSql(createFact);
        execStmtSql(createOrgRepo);
//        execStmtSql(createHealthMetric);

        execStmtSql(createCommitData);
        execStmtSql(createCommitMetric);

        execStmtSql(createIssueOpenedData);
        execStmtSql(createIssueOpenedMetric);

        execStmtSql(createPRMergedData);
        execStmtSql(createPRMergedMetric);

        execStmtSql(createCommitDeveloperRatioData);
        execStmtSql(createCommitDeveloperRatioMetric);
    }


    private static void execStmtSql(String sql) {
        try (Connection conn = new SQLiteConnection().openConnection(FileHandling.DB_PATH);
             Statement statement = conn.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException | IOException ex) {
            System.err.print(ex.getMessage());
        }
    }
}
