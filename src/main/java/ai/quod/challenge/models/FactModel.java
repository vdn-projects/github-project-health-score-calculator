package ai.quod.challenge.models;

import java.sql.Timestamp;

public class FactModel {
    private String id;
    private String type;
    private boolean isPublic;
//    private String payload;

    private RepoModel repo;
    private ActorModel actor;
    private OrgModel org;

    private Timestamp created_at;
    private String other;

    public FactModel(String id, String type, boolean isPublic, RepoModel repo, ActorModel actor, OrgModel org, Timestamp created_at, String other) {
        this.id = id;
        this.type = type;
        this.isPublic = isPublic;
        this.repo = repo;
        this.actor = actor;
        this.org = org;
        this.created_at = created_at;
        this.other = other;
    }
}
