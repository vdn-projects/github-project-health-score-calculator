package ai.quod.challenge.models;

import com.sun.org.apache.xpath.internal.operations.Or;

import java.sql.Timestamp;

public class Fact {
    private String id;
    private String type;
    private boolean isPublic;
//    private String payload;

    private Repo repo;
    private Actor actor;
    private Org org;

    private Timestamp created_at;
    private String other;

    public Fact(String id, String type, boolean isPublic, Repo repo, Actor actor, Org org, Timestamp created_at, String other) {
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
