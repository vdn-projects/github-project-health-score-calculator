package ai.quod.challenge;
import ai.quod.challenge.database.*;
import ai.quod.challenge.metrics.CommitCount;
import ai.quod.challenge.metrics.CommitDeveloperRatio;
import ai.quod.challenge.metrics.MergedPullRequest;
import ai.quod.challenge.metrics.OpenedIssue;
import ai.quod.challenge.database.SQLiteConnection;
import ai.quod.challenge.utils.FileHandling;

import java.io.*;
import java.sql.*;

import java.time.Instant;


import static ai.quod.challenge.database.InitDatabase.DB_NAME;


public class HealthScoreCalculator {

    public static void main(String[] args) throws IOException, SQLException {
        double num_of_days = 1.0/24.0;

        String toDate = "2019-10-15T20:11:23Z";
        Instant instant = Instant.parse( toDate );
        long nowTick = instant.toEpochMilli() ;

        InitDatabase.createTables();
        //Download file
//        String url = "https://data.gharchive.org/2019-10-05-23.json.gz";
//        FileHandling.downloadWithJavaIO(url, "./data/test.gz");

        //Extract file
        String jsonFilePath = "./data/test.json";
        FileHandling.decompressGzip(new File("./data/test.gz"), new File(jsonFilePath));

        //Insert into database
        LoadData.ingestFactData(jsonFilePath);

        //Process data for every single metric
        CommitCount.process(num_of_days);
        OpenedIssue.process(nowTick);
        MergedPullRequest.process();
        CommitDeveloperRatio.process();

//        exportHealthMetric();
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

}
