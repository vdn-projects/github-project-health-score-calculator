package ai.quod.challenge.models;

public class Repo {
    private long id;
    private String name;
    private String url;

    public Repo(long id, String name, String url) {
        this.id = id;
        this.name = name;
        this.url = url;
    }
}
