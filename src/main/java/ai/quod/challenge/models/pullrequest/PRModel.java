package ai.quod.challenge.models.pullrequest;

public class PRModel {
    private String action;
    private long number;
    private PRDetailModel pull_request;

    public PRModel(String action, long number, PRDetailModel pull_request) {
        this.action = action;
        this.number = number;
        this.pull_request = pull_request;
    }

    public String getAction() {
        return action;
    }

    public long getNumber() {
        return number;
    }

    public PRDetailModel getPull_request() {
        return pull_request;
    }
}
