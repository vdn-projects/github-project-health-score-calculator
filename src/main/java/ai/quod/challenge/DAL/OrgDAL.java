package ai.quod.challenge.DAL;

import ai.quod.challenge.models.OrgModel;
import ai.quod.challenge.utils.SQLite;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static ai.quod.challenge.DAL.SetupDB.DB_NAME;

public class OrgDAL {
    public static void insert(OrgModel  orgModel){
        if (orgModel == null) return;

        String sql = "INSERT INTO org(id,login,gravatar_id,avatar_url,url) VALUES(?,?,?,?,?) ON CONFLICT(id) DO NOTHING";

        try (Connection conn = new SQLite().openConnection(DB_NAME);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, orgModel.getId());
            pstmt.setString(2, orgModel.getLogin());
            pstmt.setString(3, orgModel.getGravatar_id());
            pstmt.setString(4, orgModel.getAvatar_url());
            pstmt.setString(5, orgModel.getUrl());
            pstmt.executeUpdate();
        } catch (IOException | SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
