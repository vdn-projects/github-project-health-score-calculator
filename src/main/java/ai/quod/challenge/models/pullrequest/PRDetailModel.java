package ai.quod.challenge.models.pullrequest;

public class PRDetailModel {
    private Boolean merged;

    public PRDetailModel(Boolean merged) {
        this.merged = merged;
    }

    public Boolean getMerged() {
        return merged;
    }
}
