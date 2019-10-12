package ai.quod.challenge.models;

public class Actor {
    private long id;
    private String login;
    private String gravatar_id;
    private String avatar_url;
    private String url;

    public Actor(long id, String login, String gravatar_id, String avatar_url, String url) {
        this.id = id;
        this.login = login;
        this.gravatar_id = gravatar_id;
        this.avatar_url = avatar_url;
        this.url = url;
    }
    public long getId(){
        return id;
    }
}
