package com.sminoh.paymentservice.outbox;

public enum OutboxStatus {
    INIT,
    SEND_SUCCESS,
    SEND_FAIL,
    DEAD
}
