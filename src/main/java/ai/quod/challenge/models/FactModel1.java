package ai.quod.challenge.models;

import java.sql.Timestamp;

public class FactModel1 {
    private String id;
    private String type;
    private boolean isPublic;
//    private String payload;

    private RepoModel1 repo;
    private ActorModel1 actor;
    private OrgModel org;

    private Timestamp created_at;
    private String other;

    public FactModel1(String id, String type, boolean isPublic, RepoModel1 repo, ActorModel1 actor, OrgModel org, Timestamp created_at, String other) {
        this.id = id;
        this.type = type;
        this.isPublic = isPublic;
        this.repo = repo;
        this.actor = actor;
        this.org = org;
        this.created_at = created_at;
        this.other = other;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public boolean isPublic() {
        return isPublic;
    }

    public RepoModel1 getRepo() {
        return repo;
    }

    public ActorModel1 getActor() {
        return actor;
    }

    public OrgModel getOrg() {
        return org;
    }

    public Timestamp getCreated_at() {
        return created_at;
    }

    public String getOther() {
        return other;
    }
}
