package ai.quod.challenge.models;

public class RepoModel {
    private long id;
    private String name;
    private String url;

    public RepoModel(long id, String name, String url) {
        this.id = id;
        this.name = name;
        this.url = url;
    }
}
