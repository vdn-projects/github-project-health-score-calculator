package ai.quod.challenge.utils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

import static javax.swing.JOptionPane.showMessageDialog;

public class TimeHandling {

    public static boolean validateInputDate(String dateFrom, String dateTo){
        //check ISO format
        if(!isValidIsoDateTime(dateFrom) || !isValidIsoDateTime(dateTo)){
            showMessageDialog(null, "DateFrom/DateTo must be in ISO-8601 format. Eg: 2019-08-01T00:00:00Z\n" +
                    "Please run again with appropriate format");
            return false;
        }

        //Check date comparision
        Instant isDateFrom = Instant.parse(dateFrom);
        Instant isDateTo = Instant.parse(dateTo);
        if(isDateFrom.compareTo(isDateTo) >= 0){
            showMessageDialog(null, "DateFrom cannot be equal or greater than DateTo");
            return false;
        }

        return true;
    }

    private static boolean isValidIsoDateTime(String date) {
        try {
            DateTimeFormatter.ISO_DATE_TIME.parse(date);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    public static ArrayList<String> getHourNameList(Instant isDateFrom, Instant isDateTo) {
        ArrayList<String> hourList = new ArrayList<String>();
        Instant iterInstant = isDateFrom;
        while(iterInstant.compareTo(isDateTo) < 0){
            String downloadName =   String.format("%04d", iterInstant.atZone(ZoneOffset.UTC).getYear()) + "-" +
                                    String.format("%02d", iterInstant.atZone(ZoneOffset.UTC).getMonthValue()) + "-" +
                                    String.format("%02d", iterInstant.atZone(ZoneOffset.UTC).getDayOfMonth()) + "-" +
                                    iterInstant.atZone(ZoneOffset.UTC).getHour();
                                    ;

            hourList.add(downloadName);
            iterInstant = iterInstant.plus(1, ChronoUnit.HOURS);
        }
        return hourList;
    }

}
