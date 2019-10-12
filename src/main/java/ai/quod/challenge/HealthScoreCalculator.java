package ai.quod.challenge;
import ai.quod.challenge.models.FactModel;
import ai.quod.challenge.utils.SQLite;
import com.google.gson.*;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;


public class HealthScoreCalculator {

    public static void main(String[] args) throws IOException {
        String dbName = "gharchive.db";
        Path dbFilePath = FileSystems.getDefault().getPath(dbName);
        Files.deleteIfExists(dbFilePath);

        SQLite cursor = new SQLite(dbName);

        //Create table
        String queryCreateTables =
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

        cursor.execSql(queryCreateTables);
//
//        //Download file
//        String url = "https://data.gharchive.org/2019-10-05-23.json.gz";
//        downloadWithJavaIO(url, "./data/test.gz");

        //Extract file
        String jsonFilePath = "./data/test.json";
        decompressGzip(new File("./data/test.gz"), new File(jsonFilePath));

        //Parse data
        readStream(jsonFilePath);

        //Insert into database


        //Get metric


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

    public static void readStream(String jsonPath) {
        try {
            FileReader fileReader = new FileReader(jsonPath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String jsonLine = null;
            int count = 0;
            while((jsonLine = bufferedReader.readLine()) != null) {
                System.out.println(jsonLine);
                count++;

                Gson gson = new Gson();
                FactModel factModel = gson.fromJson(jsonLine, FactModel.class);

                int i = 0;

            }
            bufferedReader.close();
            System.out.println("Total rows: " + count);



        } catch (FileNotFoundException e) {
            System.err.print(e.getMessage());
        } catch (IOException e) {
            System.err.print(e.getMessage());
        }
    }
}
