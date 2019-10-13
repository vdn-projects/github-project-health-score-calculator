package ai.quod.challenge.DAL;

import ai.quod.challenge.utils.SQLite;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class SetupDB {
    public static final String DB_NAME = "gharchive.db";

    private final static String createRepo =
            "CREATE TABLE repo(" +
            "id int PRIMARY KEY," +
            "name text," +
            "url text" +
            ");";

    private final static String createActor =
            "CREATE TABLE actor(" +
            "id int PRIMARY KEY," +
            "login text," +
            "gravatar_id text," +
            "avatar_url text," +
            "url text" +
            ");";

    private final static String createOrg =
            "CREATE TABLE org(" +
            "id int PRIMARY KEY," +
            "login text," +
            "gravatar_id text," +
            "avatar_url text," +
            "url text" +
            ");";

    private final static String createFact1 =
            "CREATE TABLE fact(" +
            "id text PRIMARY KEY," +
            "type text," +
            "public boolean," +
            //"payload text," +
            "repo_id int," +
            "actor_id int," +
            "org_id int," +
            "created_at text" +
            "other text," +
            "FOREIGN KEY (repo_id) REFERENCES repo(id)," +
            "FOREIGN KEY (actor_id) REFERENCES actor(id)," +
            "FOREIGN KEY (org_id) REFERENCES org(id)" +
            ");";

    private final static String createFact =
            "CREATE TABLE fact(" +
            "id text PRIMARY KEY," +
            "org text," +
            "repo_name text," +
            "type text," +
            "actor text," +
            "payload_no int," +
            "payload_action text," +
            "created_at date" +
            ")";

    public static void  createTables() throws IOException {
        Path dbFilePath = FileSystems.getDefault().getPath(DB_NAME);
        Files.deleteIfExists(dbFilePath);

//        execSql(createRepo);
//        execSql(createActor);
//        execSql(createOrg);
        execSql(createFact);
    }

    public static void execSql(String sql) {
        try (Connection conn = new SQLite().openConnection(DB_NAME);
             Statement statement = conn.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException | IOException ex) {
            System.err.print(ex.getMessage());
        }

    }

}
