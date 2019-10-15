package ai.quod.challenge.models;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class FactModel {
    private String id;
    private RepoModel repo;
    private String type;
    private ActorModel actor;

    private Timestamp created_at;

    public FactModel(String id, RepoModel repo, String type, ActorModel actor, String created_at) throws ParseException {
        this.id = id;
        this.repo = repo;
        this.type = type;
        this.actor = actor;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        this.created_at = new Timestamp(sdf.parse(created_at).getTime());

    }

    public String getId() {
        return id;
    }

    public RepoModel getRepo() {
        return repo;
    }

    public String getType() {
        return type;
    }

    public ActorModel getActor() {
        return actor;
    }

    public Timestamp getCreated_at() {
        return created_at;
    }
}
