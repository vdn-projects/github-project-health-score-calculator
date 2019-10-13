package ai.quod.challenge;
import ai.quod.challenge.DAL.*;
import ai.quod.challenge.models.FactModel;
import ai.quod.challenge.models.issue.PayloadIssueModel;
import ai.quod.challenge.models.pullrequest.PayloadPRModel;
import ai.quod.challenge.utils.SQLite;
import com.google.gson.*;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static ai.quod.challenge.DAL.SetupDB.DB_NAME;


public class HealthScoreCalculator {

    public static void main(String[] args) throws IOException, SQLException {
        SetupDB.createTables();
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



    public static void readStream(String jsonPath) throws SQLException {
        Connection connection = null;
        PreparedStatement pstmt = null;
        String sql = "INSERT INTO fact(id,org,repo_name,type,actor,payload_no,payload_action,created_at) VALUES(?,?,?,?,?,?,?,?);";
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
                System.out.println(jsonLine);
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

                }else if (fact.getType().equals("PullRequestEvent")){
                    PayloadPRModel pr = new Gson().fromJson(jsonLine, PayloadPRModel.class);
                    pstmt.setLong(6,pr.getPayload().getNumber());
                    pstmt.setString(7,pr.getPayload().getAction());

                }else {
                    pstmt.setNull(6, Types.INTEGER);
                    pstmt.setNull(7, Types.VARCHAR);
                }

                pstmt.setTimestamp(8,fact.getCreated_at());

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
}
