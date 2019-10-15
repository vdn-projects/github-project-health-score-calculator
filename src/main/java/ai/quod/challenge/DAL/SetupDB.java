package ai.quod.challenge.DAL;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import static ai.quod.challenge.utils.SQLite.execStmtSql;

public final class SetupDB {
    public static final String DB_NAME = "gharchive.db";

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

    private final static String createCommitData =
            "CREATE TABLE commit_data(" +
            "org text," +
            "repo_name text," +
            "num_commits int," +
            "PRIMARY KEY(org, repo_name)" +
            ")";

    private final static String createCommitMetric =
            "CREATE TABLE commit_metric(" +
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


    public static void  createTables() throws IOException {
        Path dbFilePath = FileSystems.getDefault().getPath(DB_NAME);
        Files.deleteIfExists(dbFilePath);

//        execSql(createRepo);
//        execSql(createActor);
//        execSql(createOrg);
        execStmtSql(createFact);

        execStmtSql(createCommitData);
        execStmtSql(createCommitMetric);

        execStmtSql(createIssueOpenedData);
        execStmtSql(createIssueOpenedMetric);

        execStmtSql(createPRMergedData);
        execStmtSql(createPRMergedMetric);
    }


}
