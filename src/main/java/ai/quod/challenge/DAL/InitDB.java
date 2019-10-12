package ai.quod.challenge.DAL;

import ai.quod.challenge.utils.SQLite;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class InitDB {
    private Connection conn = new SQLite();

    public static void  createTables()
    {
        //Create table
        String sql =
                //Create repo table
                "CREATE TABLE repo(" +
                "id int PRIMARY KEY," +
                "name text," +
                "url text" +
                ");" +

                //actor
                "CREATE TABLE actor(" +
                "id int PRIMARY KEY," +
                "login text," +
                "gravatar_id text," +
                "avatar_url text," +
                "url text" +
                ");" +

                //org
                "CREATE TABLE org(" +
                "id int PRIMARY KEY," +
                "login text," +
                "gravatar_id text," +
                "avatar_url text," +
                "url text" +
                ");" +

                //fact
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
    }
    public static void createRepoTable(){
        String sql =
                "CREATE TABLE repo(" +
                "id int PRIMARY KEY," +
                "name text," +
                "url text" +
                ");";
        public void execSql(String sql) {
//        try {
//            this.statement.executeUpdate(sql);
//            this.connection.commit();
//        } catch (SQLException ex) {
//            logException(ex);
//        } finally {
////            releaseReference();
//        }

            try (Connection conn = openConnection();
                 Statement statement = conn.createStatement()) {
                statement.executeUpdate(sql);
            } catch (SQLException ex) {
                logException(ex);
            }

        }
    }
}
