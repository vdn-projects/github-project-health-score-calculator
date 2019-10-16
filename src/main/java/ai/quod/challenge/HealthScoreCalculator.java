package ai.quod.challenge;
import ai.quod.challenge.database.*;
import ai.quod.challenge.metrics.*;
import ai.quod.challenge.utils.FileHandling;
import ai.quod.challenge.utils.TimeHandling;

import java.io.*;
import java.sql.*;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

import static javax.swing.JOptionPane.showMessageDialog;


public class HealthScoreCalculator {

    public static void main(String[] args) throws IOException, SQLException {
        System.out.println("Program started!");
        Instant isDateFrom = null;
        Instant isDateTo = null;

        //Input arguments handling
        if(args.length == 0){
            //Instant.now() gets current time in UTC
            Instant now = Instant.now();
            isDateFrom = now.plus(-2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS);
            isDateTo = now.plus(-1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS);

        } else if(args.length == 2){
            //Validate input arguments
            String dateFrom = args[0];
            String dateTo = args[1];
            if (!TimeHandling.validateInputDate(dateFrom, dateTo))
                return;

            isDateFrom = Instant.parse(dateFrom);
            isDateTo = Instant.parse(dateTo);

        }else {
            showMessageDialog(null, "use \"gradle run\" to extract last one hour data.\n" +
                    "Else, \"gradle run --args='dateFrom dateTo'\" to extract metric between dateFrom to dateTo.\n" +
                    "Date must be in ISO 8601 format, eg. 2019-08-01T00:00:00Z.");
            return;
        }

        //Get hour list to build an array of file names to download
        ArrayList<String> hourList = TimeHandling.getHourNameList(isDateFrom, isDateTo);

        //Download and extract json by hour
        FileHandling.download(hourList);
        FileHandling.decompress(hourList);

        //SQLite database handling
        InitDatabase.createTables();
        DataAccess.ingestJson2DB(hourList);
        DataAccess.createFactIndex(); //optional, create index to improve metric data handling when data is too big
        HealthMetrics.insertOrgRepo();

        //Process metrics
        double duration_in_days = ((int) Math.ceil((Duration.between(isDateFrom, isDateTo).toMinutes()*1.0/60)))*1.0/24;
        long toDateTick = isDateTo.toEpochMilli() ;

        CommitCount.process(duration_in_days);
        OpenedIssue.process(toDateTick);
        MergedPullRequest.process();
        CommitDeveloperRatio.process();

        //Extract health metric result
        HealthMetrics.exportHealthMetric();

        System.out.println("Program finished!");
    }

}
