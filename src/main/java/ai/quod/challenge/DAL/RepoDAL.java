package ai.quod.challenge.DAL;

import ai.quod.challenge.models.RepoModel1;
import ai.quod.challenge.utils.SQLite;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static ai.quod.challenge.DAL.SetupDB.DB_NAME;

public class RepoDAL {
    public static void insert(RepoModel1 repoModel){
        String sql = "INSERT INTO repo(id,name,url) VALUES(?,?,?) ON CONFLICT(id) DO NOTHING";
        try (Connection conn = new SQLite().openConnection(DB_NAME);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, repoModel.getId());
            pstmt.setString(2, repoModel.getName());
            pstmt.setString(3, repoModel.getUrl());
            pstmt.executeUpdate();
        } catch (IOException | SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
