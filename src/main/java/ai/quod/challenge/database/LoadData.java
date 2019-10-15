package ai.quod.challenge.database;

import ai.quod.challenge.database.SQLiteConnection;
import ai.quod.challenge.models.FactModel;
import ai.quod.challenge.models.issue.PayloadIssueModel;
import ai.quod.challenge.models.pullrequest.PayloadPRModel;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import static ai.quod.challenge.database.InitDatabase.DB_NAME;

public class LoadData {
    public static void ingestFactData(String jsonPath) throws SQLException {
        Connection connection = null;
        PreparedStatement pstmt = null;
        String sql = "INSERT INTO fact(id,org,repo_name,type,actor,payload_no,payload_action,is_merged,created_at) VALUES(?,?,?,?,?,?,?,?,?);";
        try {
            FileReader fileReader = new FileReader(jsonPath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String jsonLine = null;
            int count = 0;
            int batchSize = 20000;

            connection = new SQLiteConnection().openConnection(DB_NAME);
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
