package ai.quod.challenge.DAL;

import ai.quod.challenge.models.ActorModel1;
import ai.quod.challenge.utils.SQLite;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static ai.quod.challenge.DAL.SetupDB.DB_NAME;

//Data Access Layer where CRUD actions can be implemented
public class ActorDAL {
    public static void insert(ActorModel1 actorModel){
        String sql = "INSERT INTO actor(id,login,gravatar_id,avatar_url,url) VALUES(?,?,?,?,?) ON CONFLICT(id) DO NOTHING";

        try (Connection conn = new SQLite().openConnection(DB_NAME);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, actorModel.getId());
            pstmt.setString(2, actorModel.getLogin());
            pstmt.setString(3, actorModel.getGravatar_id());
            pstmt.setString(4, actorModel.getAvatar_url());
            pstmt.setString(5, actorModel.getUrl());
            pstmt.executeUpdate();
        } catch (SQLException | IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
