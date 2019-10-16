package ai.quod.challenge;
import ai.quod.challenge.database.*;
import ai.quod.challenge.metrics.*;
import ai.quod.challenge.utils.FileHandling;
import ai.quod.challenge.utils.TimeHandling;

import java.io.*;
import java.sql.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import static javax.swing.JOptionPane.showMessageDialog;


public class HealthScoreCalculator {

    public static void main(String[] args) throws IOException, SQLException {

//        if(args.length != 2){
//            System.out.println("Two arguments required (dateFrom & dateTo)");
//            return;
//        }
//
//        String dateFrom = args[0];
//        String dateTo = args[1];
//

        String dateFrom = "2019-08-01T12:00:00Z";
        String dateTo = "2019-08-02T12:00:00Z";

        //Validate input arguments
        if (!TimeHandling.validateInputDate(dateFrom, dateTo)) return;

        //Get hour list to build an array of file names to download
        Instant isDateFrom = Instant.parse(dateFrom);
        Instant isDateTo = Instant.parse(dateTo);
        ArrayList<String> hourList = TimeHandling.getHourNameList(isDateFrom, isDateTo);

        //Download and extract json by hour
        FileHandling.download(hourList);
        FileHandling.decompress(hourList);

        InitDatabase.createTables();
        LoadData.ingestJson2DB(hourList);

        HealthMetrics.insertOrgRepo();

        //Process metrics
        double duration_in_days = ((int) Math.ceil((Duration.between(isDateFrom, isDateTo).toMinutes()*1.0/60)))*1.0/24;
        long toDateTick = isDateTo.toEpochMilli() ;

        CommitCount.process(duration_in_days);
        OpenedIssue.process(toDateTick);
        MergedPullRequest.process();
        CommitDeveloperRatio.process();

        HealthMetrics.exportHealthMetric();
    }

}
