package ai.quod.challenge.models.pullrequest;

public class PRModel {
    private String action;
    private long number;

    public PRModel(String action, long number) {
        this.action = action;
        this.number = number;
    }

    public String getAction() {
        return action;
    }

    public long getNumber() {
        return number;
    }
}
