package ai.quod.challenge.utils;

import com.opencsv.CSVWriter;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class FileHandling {
    public static final String BASE_PATH = System.getProperty("user.dir");
    public static final String DATA_PATH = BASE_PATH + "/data/";
    public static final String DB_PATH = BASE_PATH + "/data/gharchive.db";
    private static final String GH_ARCHIVE_LINK = "https://data.gharchive.org/";
    public static final String CSV_OUTPUT_PATH = BASE_PATH + "/output/health_scores.csv";

    public static void decompress(ArrayList<String> hourList) throws IOException {
        for (String hour: hourList
        ) {
            try {
                String zipFile = DATA_PATH + hour + ".gz";
                String jsonFile = DATA_PATH + hour + ".json";
                System.out.println("Start extracting file " + zipFile);
                decompressGzip(new File(zipFile), new File(jsonFile));
                System.out.println("Complete extracting file " + zipFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void download(ArrayList<String> hourList) {
        deleteFiles(DATA_PATH, new String[]{"gz", "json"});
        for (String hour: hourList
        ) {
            try {
                String linkDownload = GH_ARCHIVE_LINK + hour + ".json.gz";
                System.out.println("Start downloading file " + linkDownload);
                downloadWithJavaIO(linkDownload, DATA_PATH + hour + ".gz");
                System.out.println("Complete downloading file " + linkDownload);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void downloadWithJavaIO(String url, String localFilename) {
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

    private static void decompressGzip(File input, File output) throws IOException {
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

    private static String readFile(String path) throws IOException {
        String content = Files.lines(Paths.get(path), StandardCharsets.UTF_8)
                .collect(Collectors.joining(System.lineSeparator()));
        return content;
    }

    public static void extractCsv(ResultSet resultSet, String csvPath) throws IOException, SQLException {
        CSVWriter writer = new CSVWriter(new FileWriter(csvPath), CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER);
        writer.writeAll(resultSet, true);
        writer.close();
    }

    private static void deleteFiles(String path, String[] extensions){
        try {
            for (String extension:extensions
                 ) {
                Arrays.stream(new File(path).listFiles((f, p) -> p.endsWith(extension))).forEach(File::delete);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
