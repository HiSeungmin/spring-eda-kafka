package com.sminoh.paymentservice.repository;

import com.sminoh.paymentservice.domain.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, String> {
    boolean existsByOrderId(String orderId);
}
