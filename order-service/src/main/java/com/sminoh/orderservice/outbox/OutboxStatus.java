package com.sminoh.orderservice.outbox;

public enum OutboxStatus {
    INIT,
    SEND_SUCCESS,
    SEND_FAIL,
    DEAD
}
