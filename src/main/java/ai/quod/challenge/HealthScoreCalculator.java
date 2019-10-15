package ai.quod.challenge;
import ai.quod.challenge.DAL.*;
import ai.quod.challenge.models.FactModel;
import ai.quod.challenge.models.issue.PayloadIssueModel;
import ai.quod.challenge.models.pullrequest.PayloadPRModel;
import ai.quod.challenge.utils.SQLite;
import com.google.gson.*;
import com.opencsv.CSVWriter;

import javax.swing.plaf.nimbus.State;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;

import java.time.Instant;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;


import static ai.quod.challenge.DAL.SetupDB.DB_NAME;


public class HealthScoreCalculator {

    public static void main(String[] args) throws IOException, SQLException {
        double num_of_days = 1.0/24.0;

        String toDate = "2019-10-15T20:11:23Z";
        Instant instant = Instant.parse( toDate );
        long nowTick = instant.toEpochMilli() ;

        SetupDB.createTables();
//        //Download file
//        String url = "https://data.gharchive.org/2019-10-05-23.json.gz";
//        downloadWithJavaIO(url, "./data/test.gz");

        //Extract file
        String jsonFilePath = "./data/test.json";
        decompressGzip(new File("./data/test.gz"), new File(jsonFilePath));

        //Insert into database
        json2SQLite(jsonFilePath);

        //Get metric
        //Insert commit data
        insertCommitData(num_of_days);
        insertCommitMetric();

        //Opened issue metric
        insertIssueOpenedData(nowTick);
        insertIssueOpenedMetric();

        //Merged request metric
        insertPRMergedData();
        insertPRMergedMetric();

        //Export health metric

//        exportHealthMetric();



    }

    public static String readFile(String path) throws IOException {
        String content = Files.lines(Paths.get(path), StandardCharsets.UTF_8)
                .collect(Collectors.joining(System.lineSeparator()));
        return content;
    }

    public static void downloadWithJavaIO(String url, String localFilename) {

        System.setProperty("http.agent", "Safari");
        try (BufferedInputStream in = new BufferedInputStream(new URL(url).openStream()); FileOutputStream fileOutputStream = new FileOutputStream(localFilename)) {

            byte dataBuffer[] = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void decompressGzip(File input, File output) throws IOException {
        try (GZIPInputStream in = new GZIPInputStream(new FileInputStream(input))){
            try (FileOutputStream out = new FileOutputStream(output)){
                byte[] buffer = new byte[1024];
                int len;
                while((len = in.read(buffer)) != -1){
                    out.write(buffer, 0, len);
                }
            }
        }
    }

    public static void json2SQLite(String jsonPath) throws SQLException {
        Connection connection = null;
        PreparedStatement pstmt = null;
        String sql = "INSERT INTO fact(id,org,repo_name,type,actor,payload_no,payload_action,is_merged,created_at) VALUES(?,?,?,?,?,?,?,?,?);";
        try {
            FileReader fileReader = new FileReader(jsonPath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String jsonLine = null;
            int count = 0;
            int batchSize = 20000;

            connection = new SQLite().openConnection(DB_NAME);
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(sql);

            while ((jsonLine = bufferedReader.readLine()) != null) {
                //System.out.println(jsonLine);
                count++;
                FactModel fact = new Gson().fromJson(jsonLine, FactModel.class);

                pstmt.setString(1,fact.getId());

                String org = fact.getRepo().getName().split("/")[0];
                String repo_name = fact.getRepo().getName().split("/")[1];
                pstmt.setString(2,org);
                pstmt.setString(3,repo_name);

                pstmt.setString(4,fact.getType());
                pstmt.setString(5,fact.getActor().getLogin());

                if (fact.getType().equals("IssuesEvent")) {
                    PayloadIssueModel issue = new Gson().fromJson(jsonLine, PayloadIssueModel.class);
                    pstmt.setLong(6,issue.getPayload().getIssue().getNumber());
                    pstmt.setString(7,issue.getPayload().getAction());
                    pstmt.setNull(8,Types.BOOLEAN);

                }else if (fact.getType().equals("PullRequestEvent")){
                    PayloadPRModel pr = new Gson().fromJson(jsonLine, PayloadPRModel.class);
                    pstmt.setLong(6,pr.getPayload().getNumber());
                    pstmt.setString(7,pr.getPayload().getAction());
                    pstmt.setBoolean(8,pr.getPayload().getPull_request().getMerged());
                }else {
                    pstmt.setNull(6, Types.INTEGER);
                    pstmt.setNull(7, Types.VARCHAR);
                    pstmt.setNull(8,Types.BOOLEAN);
                }

                pstmt.setTimestamp(9,fact.getCreated_at());

                pstmt.addBatch();

                //Commit batch for every batchSize
                //This technique is to increase the speed of INSERT
                if(count % batchSize == 0){
                    int[] result = pstmt.executeBatch();
                    System.out.println("Number of rows inserted: "+ result.length);
                    connection.commit();
                }
            }
            bufferedReader.close();
            System.out.println("Total rows inserted:" + count);

        } catch (Exception e) {
            e.printStackTrace();
            // connection.rollBack();
        } finally {
            if (pstmt != null)
                pstmt.close();
            if (connection != null)
                connection.close();
        }
    }

    public static void insertCommitData(double num_of_days) throws SQLException {
        Connection connection = null;
        PreparedStatement pstmt = null;
        String sql =
                //num_commits are calculated as average number of commit by day
                "INSERT INTO commit_data(org,repo_name,num_commits) " +
                "SELECT org,repo_name,COUNT(*)/? num_commits " +
                "FROM fact " +
                "WHERE \"type\"='PushEvent' " +
                "GROUP BY org, repo_name";
        try {
            connection = new SQLite().openConnection(DB_NAME);
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

    public static void insertCommitMetric() throws SQLException {
        Connection connection = null;
        Statement stmt = null;

        //Get max number of commits and put into an variable to improve insert processing time
        String sql1 =   "CREATE TEMP TABLE IF NOT EXISTS var (name TEXT PRIMARY KEY, value int);";
        String sql2 =   "INSERT OR REPLACE INTO var SELECT 'max_num_commits', MAX(num_commits) FROM commit_data;";
        String sql3 =   "INSERT INTO commit_metric(org,repo_name,num_commits,max_num_commits, metric) " +
                        "SELECT 	org," +
                        "		repo_name," +
                        "		num_commits," +
                        "		(SELECT value FROM var WHERE name='max_num_commits') max_num_commits," +
                        "		num_commits*1.0/(SELECT value FROM var WHERE name='max_num_commits') metric " +
                        "FROM commit_data;";
        try {
            connection = new SQLite().openConnection(DB_NAME);
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

    public static void exportHealthMetric() throws SQLException {
        Connection connection = null;
        Statement stmt = null;
        String sql =
                "SELECT org,repo_name,num_commits,printf(\"%.2f\", (num_commits*1.0/max_num_commits)) health_score " +
                "FROM health_metric " +
                "ORDER BY health_score DESC " +
                "LIMIT 1000;";

        try {
            connection = new SQLite().openConnection(DB_NAME);
            stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            extractCsv(resultSet, "output.csv");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stmt != null)
                stmt.close();
            if (connection != null)
                connection.close();
        }
    }

    public static void extractCsv(ResultSet resultSet, String csvPath) throws IOException, SQLException {
        CSVWriter writer = new CSVWriter(new FileWriter(csvPath), CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER);
        writer.writeAll(resultSet, true);
        writer.close();
    }

    public static void insertIssueOpenedData(long nowTick) throws SQLException {
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
            connection = new SQLite().openConnection(DB_NAME);
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

    public static void insertIssueOpenedMetric() throws SQLException {
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
            connection = new SQLite().openConnection(DB_NAME);
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

    public static void insertPRMergedData() throws SQLException {
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
            connection = new SQLite().openConnection(DB_NAME);
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

    public static void insertPRMergedMetric() throws SQLException {
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
            connection = new SQLite().openConnection(DB_NAME);
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
