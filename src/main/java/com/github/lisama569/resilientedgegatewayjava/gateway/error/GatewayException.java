package com.github.lisama569.resilientedgegatewayjava.gateway.error;

import com.github.lisama569.resilientedgegatewayjava.gateway.model.FailureReason;

public class GatewayException extends RuntimeException {
    private final FailureReason reason;

    public GatewayException(FailureReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public FailureReason getReason() {
        return reason;
    }
}
