package ai.quod.challenge.models;

import jdk.nashorn.internal.objects.annotations.Getter;

public class ActorModel1
{
    private long id;
    private String login;
    private String gravatar_id;
    private String avatar_url;
    private String url;

    public ActorModel1(long id, String login, String gravatar_id, String avatar_url, String url) {
        this.id = id;
        this.login = login;
        this.gravatar_id = gravatar_id;
        this.avatar_url = avatar_url;
        this.url = url;
    }

    public long getId(){
        return id;
    }

    public String getLogin() {
        return login;
    }

    public String getGravatar_id() {
        return gravatar_id;
    }

    public String getAvatar_url() {
        return avatar_url;
    }

    public String getUrl() {
        return url;
    }
}
