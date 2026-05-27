package com.sminoh.paymentservice.service;

import com.sminoh.paymentservice.event.consumed.OrderCreatedEvent;
import com.sminoh.paymentservice.event.published.PaymentCompletedEvent;
import com.sminoh.paymentservice.event.published.PaymentFailedEvent;
import com.sminoh.paymentservice.domain.Payment;
import com.sminoh.paymentservice.event.published.PaymentEventPublisher;
import com.sminoh.paymentservice.repository.PaymentRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {
    private final PaymentRepository paymentRepository;
    private final PaymentEventPublisher paymentEventPublisher;


    public void processPayment(OrderCreatedEvent event){
        if(paymentRepository.existsByOrderId(event.getOrderId())){
            log.warn("event=DUPLICATE orderId={} skip", event.getOrderId());
        }

        // 1. 결제 PENDING 상태 저장
        Payment payment = createPendingPayment(event);

        // 2. 외부 PG사 API호출
        boolean success = simulatPaymentAPI();

        // 3. 결과에 따른 상태 업데이트 후 이벤트 발행
        if(success){
            completePayment(payment);
        }else{
            failPayment(payment);
        }

    }

    @Transactional
    protected Payment createPendingPayment(OrderCreatedEvent event) {
        Payment payment = Payment.create(event.getOrderId(), event.getUserId(), event.getTotalAmount());
        return paymentRepository.save(payment);
    }

    protected boolean simulatPaymentAPI(){
        log.info("action=PG_API_CALL status=requesting");
        return true;
    }

    @Transactional
    protected void completePayment(Payment payment){
        payment.complete();
        paymentRepository.save(payment);

        paymentEventPublisher.publishCompleted(new PaymentCompletedEvent(
                payment.getId(),
                payment.getOrderId(),
                payment.getUserId(),
                payment.getAmount(),
                payment.getPaidAt()
        ));

        log.info("action=PAYMENT_COMPLETED orderId={} amount={}", payment.getOrderId(), payment.getAmount());
    }

    @Transactional
    protected void failPayment(Payment payment){
        payment.fail();
        paymentRepository.save(payment);

        paymentEventPublisher.publishFailed(new PaymentFailedEvent(
                payment.getOrderId(),
                payment.getUserId(),
                "PG사 결제 실패"
        ));

        log.warn("action=PAYMENT_FAILED orderId={} reason=PG_REJECT", payment.getOrderId());
    }
}
