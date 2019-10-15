package ai.quod.challenge;
import ai.quod.challenge.database.*;
import ai.quod.challenge.metrics.*;
import ai.quod.challenge.utils.FileHandling;

import java.io.*;
import java.sql.*;

import java.time.Instant;


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

        HealthMetrics.insertOrgRepo();

        //Process data for every single metric
        CommitCount.process(num_of_days);
        OpenedIssue.process(nowTick);
        MergedPullRequest.process();
        CommitDeveloperRatio.process();

        HealthMetrics.exportHealthMetric();
    }

}
