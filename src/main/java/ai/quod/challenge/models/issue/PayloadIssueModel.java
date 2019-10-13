package ai.quod.challenge.models.issue;

public class PayloadIssueModel {
    private IssueModel payload;

    public PayloadIssueModel(IssueModel payload) {
        this.payload = payload;
    }

    public IssueModel getPayload() {
        return payload;
    }
}

