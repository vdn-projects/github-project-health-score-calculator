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
            "created_at datetime" +
            ")";
    private final static String createCommit =
            "CREATE TABLE avg_commit(" +
            "org text," +
            "repo_name text," +
            "num_commits int," +
            "PRIMARY KEY(org, repo_name)" +
            ")";
    private final static String createMetric =
            "CREATE TABLE health_metric(" +
            "org text," +
            "repo_name text," +
            "num_commits int," +
            "max_num_commits int," +
            "PRIMARY KEY(org, repo_name) ON CONFLICT IGNORE" +
            ")";


    public static void  createTables() throws IOException {
        Path dbFilePath = FileSystems.getDefault().getPath(DB_NAME);
        Files.deleteIfExists(dbFilePath);

//        execSql(createRepo);
//        execSql(createActor);
//        execSql(createOrg);
        execStmtSql(createFact);
        execStmtSql(createCommit);
        execStmtSql(createMetric);
    }


}
