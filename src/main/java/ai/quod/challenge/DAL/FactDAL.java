package ai.quod.challenge.DAL;

import ai.quod.challenge.models.FactModel1;
import ai.quod.challenge.utils.SQLite;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import static ai.quod.challenge.DAL.SetupDB.DB_NAME;

public class FactDAL {
    public static void insert(FactModel1 factModel){
        String sql = "INSERT INTO fact(id,type,public,repo_id,actor_id,org_id,created_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING";

        try (Connection conn = new SQLite().openConnection(DB_NAME);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, factModel.getId());
            pstmt.setString(2, factModel.getType());
            pstmt.setBoolean(3, factModel.isPublic());
            pstmt.setLong(4, factModel.getRepo().getId());
            pstmt.setLong(5, factModel.getActor().getId());

            if(factModel.getOrg() == null){
                pstmt.setNull(6, Types.INTEGER);
            }else {
                pstmt.setLong(6, factModel.getOrg().getId());
            }

            pstmt.setTimestamp(7, factModel.getCreated_at());

            pstmt.executeUpdate();

        } catch (IOException | SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
