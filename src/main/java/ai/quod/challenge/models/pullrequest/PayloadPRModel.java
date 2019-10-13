package ai.quod.challenge.models.pullrequest;

import ai.quod.challenge.models.pullrequest.PRModel;

public class PayloadPRModel {
    private PRModel payload;

    public PayloadPRModel(PRModel payload) {
        this.payload = payload;
    }

    public PRModel getPayload() {
        return payload;
    }
}

