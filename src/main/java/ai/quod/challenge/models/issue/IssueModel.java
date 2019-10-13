package ai.quod.challenge.models.issue;

public class IssueModel {
    private String action;
    private IssueDetailModel issue;

    public IssueModel(String action, IssueDetailModel issue) {
        this.action = action;
        this.issue = issue;
    }

    public String getAction() {
        return action;
    }

    public IssueDetailModel getIssue() {
        return issue;
    }
}
