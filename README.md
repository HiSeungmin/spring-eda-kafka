# Spring EDA Kafka

Spring Boot와 Kafka 기반의 이벤트 드리븐 아키텍처(EDA) 주문 시스템

## 📌 프로젝트 개요

마이크로서비스 간 비동기 통신을 Kafka로 처리하는 주문 처리 시스템입니다. 주문, 결제 서비스를 중심으로 구현되어 있으며, 도메인 이벤트로 통신하여 서비스 간 느슨한 결합을 유지합니다.

## 🏗 아키텍처

### 전체 구조

```
┌─────────────┐     order.created       ┌──────────────┐
│   Client    │ ────────────────────→   │ Order Service│
└─────────────┘                         └──────┬───────┘
                                               │ publish
                                               ↓
                                        ┌──────────────┐
                                        │    Kafka     │
                                        └──────┬───────┘
                                               │
                  ┌────────────────────────────┼────────────────────────┐
                  │                            │                        │
                  ↓                            ↓                        ↓
          ┌───────────────┐           ┌──────────────┐         ┌──────────────┐
          │Payment Service│           │ Stock Service│         │Notification..│
          └──────┬────────┘           └──────────────┘         └──────────────┘
                 │ payment.completed
                 │ payment.failed
                 ↓
          ┌──────────────┐
          │    Kafka     │
          └──────┬───────┘
                 │
                 ↓
          ┌────────────────┐
          │  Order Service │
          └────────────────┘
```

### 이벤트 흐름

```
1. Client → Order Service
   ├─ orders 테이블에 주문 저장 (status=PENDING)
   └─ order.created 이벤트 발행

2. Payment Service ← order.created
   ├─ 결제 처리 (PG API 호출)
   ├─ payments 테이블에 결제 저장
   └─ payment.completed / payment.failed 이벤트 발행

3. Order Service ← payment.completed
   └─ 주문 상태 업데이트 (CONFIRMED)

```

---

## 🎯 설계 결정

### 1. Kafka 기본 설정

Kafka 토픽은 개발 환경 기준 아래와 같이 설정했습니다.

| 설정                 | 값        | 설명                      |
| ------------------ | -------- |-------------------------|
| Partitions         | 3        | 개발 환경 기준(운영 환경은 최대 40개) |
| Replication Factor | 1        | 로컬 개발 환경 기준(운영 환경은 3)   |
| Ack Mode           | all      | 메시지 유실 방지               |
| Auto Offset Reset  | earliest | 초기 구독 시 전체 이벤트 조회       |



#### 설계 의도

[처리량 기반 파티션 설계]

- 목표 트래픽 규모: 2,000 TPS
- 결제 서비스는 외부 API 호출을 포함하므로 컨슈머 1개당 처리량을 보수적으로 100 TPS로 산정
- 파티션 수 계산: 2,000 TPS / 100 TPS = 20 파티션 × 1.5배 ~ 2배 여유 = 30 ~ 40 파티션
- 컨슈머 수는 파티션 수와 동일하게 40개 (인스턴스 수에 따라 분배 조정)
- 파티션은 줄일 수 없으므로 운영 환경에서는 초기부터 넉넉하게 설정

[신뢰성 설계]

- `acks=all` 설정으로 브로커 장애 상황에서도 데이터 유실 방지
- 운영 환경 Replication Factor는 3, 개발 환경에서는 1로 구분

[순서 보장]

- 동일 orderId를 Kafka 메시지 Key로 지정하여 같은 주문의 이벤트는 항상 동일 파티션으로 라우팅
- 주문 생성 → 결제 요청 → 결제 완료 흐름의 순서 일관성 보장

---

### 2. 토픽 정의

도메인 이벤트 중심으로 토픽을 분리했습니다.

| Topic               | Producer         | Consumer | 설명        |
| ------------------- | ---------------- |----------| --------- |
| `order.created`     | Order Service    | Payment  | 주문 생성 이벤트 |
| `payment.completed` | Payment Service  | Order    | 결제 완료 이벤트 |
| `payment.failed`    | Payment Service  | Order    | 결제 실패 이벤트 |


#### 설계 의도

- 토픽명을 “도메인.행위” 형식으로 통일하여 이벤트 의미를 명확하게 표현
- 서비스 간 직접 호출 대신 이벤트 기반 비동기 통신 사용
- Consumer가 Producer 구현을 몰라도 되도록 느슨한 결합 유지
- 이벤트 추가 시 기존 서비스 수정 없이 Consumer만 확장 가능

---

### 3. Outbox 패턴

주문 저장과 이벤트 발행 간 데이터 정합성을 보장하기 위해 Outbox 패턴을 적용했습니다.

#### 문제 상황

일반적인 Kafka 발행 방식에서는 아래 문제가 발생할 수 있습니다.

```text
1. DB 저장 성공
2. Kafka 발행 실패
→ 데이터와 이벤트 상태 불일치 발생
```

이 경우 주문은 생성되었지만 다른 서비스는 이벤트를 받지 못하는 문제가 발생합니다.

#### outbox 구현 방식 비교

| 방식                   | 장점                | 단점                        |
|----------------------| ----------------- | ------------------------- |
| 1. Polling Publisher | 구현 단순             | Polling 주기만큼 지연 발생        |
| 2. Event Listener    | Spring 친화적, 구현 간단 | 애플리케이션 의존적                |
| 3. CDC(Debezium)     | 실시간 처리            | Kafka Connect 등 운영 복잡도 증가 |

#### 최종 선택

CDC(Debezium)은 운영 인프라 부담으로 인해 `TransactionalEventListener` 기반 Event Listener 방식을 채택했습니다.

#### 이벤트 발행 흐름

1. 주문 저장
2. BEFORE_COMMIT 단계에서 Outbox 이벤트 저장
3. Transaction Commit
4. AFTER_COMMIT 단계에서 Kafka 이벤트 발행
5. 발행 성공 시 Outbox 상태 업데이트
6. 발행에 실패한 이벤트는 배치 작업으로 재처리

```java
@TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
public void record(){
    // outbox 테이블 저장
}
```

```java
@Async("outboxTaskExecutor")
@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
public void send(OrderCreatedAppEvent appEvent){
    // kafka 이벤트 발행
}
```

#### 기대 효과

* DB 저장과 이벤트 발행 간 정합성 보장
* 트랜잭션 실패 시 이벤트 미발행 보장
* Kafka 장애 상황에서도 재처리 가능 구조 확보

---

### 4. 멱등성 보장

Kafka는 At-Most-Once, At-Least-Once, Exactly-Once 세 가지 전달 보장을 지원하지만, 외부 시스템(DB)과의 통합에서는 Exactly-Once가 Kafka 내부에서만 유효하다는 한계가 있습니다. 따라서 **At-Least-Once + Consumer 멱등성** 조합이 업계 표준 패턴이며, 본 시스템도 이 방식을 따릅니다.

#### At-Least-Once 채택 이유

- Producer 멱등성(`enable.idempotence=true`)으로 Kafka 내 중복 발행 방지
- Outbox 패턴으로 발행 신뢰성 확보 (유실 방지)
- Exactly-Once는 외부 시스템 통합 시 보장이 깨지고 성능/복잡도 비용이 큼
- "At-Least-Once + Consumer 멱등성"으로 실질적 Exactly-Once 효과 달성

#### Consumer 멱등성 처리 방식

이벤트의 `eventId`를 키로 처리 이력을 추적하여 중복 수신을 차단합니다.

```text
1. 이벤트 수신
2. processed_events 테이블에서 eventId 존재 여부 확인
3. 이미 처리된 eventId면 skip
4. 미처리 이벤트만 비즈니스 로직 수행 + 처리 이력 저장 (같은 트랜잭션)
```

#### 중복 발생 시나리오 대응

| 시나리오          | 원인 | 대응 |
|---------------|------|------|
| Producer 재발행  | Outbox 배치가 미발행 건 재시도 | eventId 동일 → Consumer skip |
| Consumer 재시작  | 처리 후 offset 커밋 직전 다운 | eventId 동일 → Consumer skip |
| Consumer 재수신 | 네트워크 장애 등 At-Least-Once 특성 | eventId 동일 → Consumer skip |

#### 설계 의도

- 결제/재고 차감처럼 중복 실행 시 데이터 정합성이 깨지는 비즈니스 로직 보호
- 비즈니스 처리와 멱등성 기록을 **같은 트랜잭션**에 묶어 원자성 보장

---

## 🛠 기술 스택

### Backend
- **Java 25**
- **Spring Boot 4.0.6**
- **Spring Kafka** - 이벤트 기반 통신
- **Spring Data JPA** - 영속성 관리
- **Hibernate** - ORM

### Infrastructure
- **Apache Kafka** - 메시지 브로커
- **PostgreSQL** - 서비스별 독립 데이터베이스 (Database per Service)
- **Docker Compose** - 로컬 인프라 관리

### 개발 도구
- **Gradle (Multi-module)** - 빌드 관리
- **spring-dotenv** - 환경 변수 관리

## 📁 프로젝트 구조

```
spring-eda-kafka/
├── docker-compose.yml              # Kafka + PostgreSQL 인프라
├── settings.gradle
├── build.gradle
│
├── order-service/                  # 주문 서비스 (port 8081)
│   └── src/main/java/com/sminoh/orderservice/
│       ├── config/                 # Kafka 설정
│       ├── controller/             # REST API
│       ├── domain/                 # JPA 엔티티
│       ├── dto/                    # 요청/응답 DTO
│       ├── event/
│       │   ├── internel/           # Spring App 이벤트
│       │   ├── consumed/           # 구독 이벤트 (PaymentCompletedEvent 등)
│       │   └── published/          # 발행 이벤트 (OrderCreatedEvent)
│       ├── outbox/                 # outbox 이벤트
│       ├── repository/
│       └── service/
│
├── payment-service/                # 결제 서비스 (port 8082)
│   └── src/main/java/com/sminoh/paymentservice/
│       ├── config/
│       ├── domain/
│       ├── event/
│       │   ├── internel/           # Spring App 이벤트
│       │   ├── consumed/           # 구독 이벤트 (OrderCreatedEvent)
│       │   └── published/          # 발행 이벤트 (PaymentCompletedEvent 등)
│       ├── outbox/                 # outbox 이벤트
│       ├── repository/
│       └── service/
│
├── delivery-service/               # 배송 서비스 (port 8083) [예정]
└── notification-service/           # 알림 서비스 (port 8084) [예정]
```

---

## 🚀 실행 방법

### 1. 사전 요구사항

- Java 25 (LTS)
- Docker Compose (로컬 실행 환경)

### 2. 환경 변수 설정

각 서비스 디렉토리에 `.env` 파일을 생성합니다.

**order-service/.env**
```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ORDER_DB_URL=jdbc:postgresql://localhost:5432/order_db
ORDER_DB_USERNAME=postgres
ORDER_DB_PASSWORD=postgres
```

**payment-service/.env**
```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
PAYMENT_DB_URL=jdbc:postgresql://localhost:5433/payment_db
PAYMENT_DB_USERNAME=postgres
PAYMENT_DB_PASSWORD=postgres
```

### 3. 인프라 실행

```bash
docker-compose up -d
```

Kafka(9092), PostgreSQL(5432, 5433)가 실행됩니다.

### 4. 애플리케이션 실행

IntelliJ에서 각 서비스의 `Application.java` 실행 또는 터미널에서:

```bash
# Order Service
./gradlew :order-service:bootRun

# Payment Service (별도 터미널)
./gradlew :payment-service:bootRun
```

---

## 🧪 API 테스트

### 주문 생성

```bash
curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-123",
    "items": [
      {
        "productId": "product-1",
        "quantity": 2,
        "price": 7500
      }
    ]
  }'
```

**응답 예시**
```json
{
  "createdAt": "2026-05-28T15:28:31.4723389",
  "orderId": "abc-123",
  "status": "PENDING",
  "totalAmount": 15000,
  "userId": "user-123"
}
```

### 흐름 확인

주문 후 각 서비스의 로그 파일에서 이벤트 흐름을 확인할 수 있습니다.

**[Order Service] order-service/logs/order-service.log**
```
action=ORDER_CREATED orderId=ce4d4c3a... userId=user-123 totalAmount=30000
action=OUTBOX_RECORDED type=OrderCreated eventId=716bcd9f... orderId=ce4d4c3a...
event=PUBLISH topic=order.created eventId=716bcd9f... orderId=ce4d4c3a...
```

**[Payment Service] payment-service/logs/payment-service.log**
```
event=CONSUME topic=order.created orderId=ce4d4c3a...
action=PAYMENT_COMPLETED orderId=ce4d4c3a... amount=30000
action=OUTBOX_RECORDED type=PaymentCompleted eventId=aadbce66... orderId=ce4d4c3a...
event=PUBLISH topic=payment.completed eventId=aadbce66... orderId=ce4d4c3a...
```

**[Order Service] 결제 결과 수신**
```
event=CONSUME topic=payment.completed orderId=ce4d4c3a...
action=ORDER_COMPLETED orderId=ce4d4c3a...
```

### kafka 이벤트 생성 확인

토픽 목록 확인
```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
```
__consumer_offsets
order.created
payment.completed
```

메시지 확인
```
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order.created --from-beginning
```
```
{"orderId":"842da73e-18dc-4f5f-bc64-44314c777360","userId":"user-123","totalAmount":30000,"items":[{"productId":"product-001","quantity":2,"price":15000}],"createdAt":"2026-05-28T15:28:31.5136482","eventId":"538f9036-49ab-46cb-a28a-3d9fd8aae5d9"}
```



### DB 확인

```bash
# Order DB
docker exec -it order-db psql -U postgres -d order_db -c "SELECT * FROM orders;"

# Payment DB
docker exec -it payment-db psql -U postgres -d payment_db -c "SELECT * FROM payments;"
```

---

## 📚 참고

- [kafka 공식 문서](https://kafka.apache.org/43/getting-started/introduction/)
- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [트랜잭션 아웃박스 패턴의 실제 구현 사례 - 29cm](https://medium.com/@greg.shiny82/%ED%8A%B8%EB%9E%9C%EC%9E%AD%EC%85%94%EB%84%90-%EC%95%84%EC%9B%83%EB%B0%95%EC%8A%A4-%ED%8C%A8%ED%84%B4%EC%9D%98-%EC%8B%A4%EC%A0%9C-%EA%B5%AC%ED%98%84-%EC%82%AC%EB%A1%80-29cm-0f822fc23edb)