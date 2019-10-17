package ai.quod.challenge.database;

import ai.quod.challenge.models.FactModel;
import ai.quod.challenge.models.issue.PayloadIssueModel;
import ai.quod.challenge.models.pullrequest.PayloadPRModel;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.ArrayList;

import static ai.quod.challenge.utils.FileHandling.DATA_PATH;
import static ai.quod.challenge.utils.FileHandling.SQLITE_DB_PATH;

public class DataAccess {
    public static void ingestJson2DB(ArrayList<String> hourList) throws SQLException {
        for (String hour: hourList
        ) {
            String jsonPath = DATA_PATH + hour + ".json";
            ingestJson2DB(jsonPath);
        }
    }

    public static void ingestJson2DB(String jsonPath) throws SQLException {
        System.out.println("Start ingesting data from " + jsonPath);
        Connection connection = null;
        PreparedStatement pstmt = null;
        String sql = "INSERT INTO fact(id,org,repo_name,type,actor,payload_no,payload_action,is_merged,created_at) VALUES(?,?,?,?,?,?,?,?,?);";
        try {
            FileReader fileReader = new FileReader(jsonPath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String jsonLine;
            int count = 0;
            int batchSize = 20000;

            connection = new SQLiteConnection().openConnection(SQLITE_DB_PATH);
            connection.setAutoCommit(false);
            pstmt = connection.prepareStatement(sql);

            while ((jsonLine = bufferedReader.readLine()) != null) {
                //System.out.println(jsonLine);
                count++;
                FactModel fact = new Gson().fromJson(jsonLine, FactModel.class);

                //Interested in number of event types
                if (!fact.getType().equals("PushEvent") &&
                    !fact.getType().equals("IssuesEvent") &&
                    !fact.getType().equals("PullRequestEvent"))
                    continue;

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
                    pstmt.setNull(8, Types.BOOLEAN);

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
                    connection.commit();
                }
            }
            bufferedReader.close();
            System.out.println("Completed with total " + count + " rows inserted.");

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

    public static void createFactIndex() throws SQLException {
        System.out.println("Creating INDEX on fact(type,org,repo_name) columns.");
        Connection connection = null;
        Statement stmt = null;
        String sql =
                "CREATE INDEX fact_index ON fact(type,org,repo_name)";
        try {
            connection = new SQLiteConnection().openConnection(SQLITE_DB_PATH);
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
}
