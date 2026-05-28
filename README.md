# Spring EDA Kafka

Spring Boot와 Kafka 기반의 이벤트 드리븐 아키텍처(EDA) 주문 시스템

## 📌 프로젝트 개요

마이크로서비스 간 비동기 통신을 Kafka로 처리하는 주문 처리 시스템입니다. 주문, 결제, 재고 도메인을 독립된 서비스로 분리하고, 도메인 이벤트로 통신하여 서비스 간 느슨한 결합을 유지합니다.

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
          ┌──────────────┐           ┌──────────────┐         ┌──────────────┐
          │Payment Service│          │ Stock Service│         │Notification..│
          └──────┬───────┘           └──────────────┘         └──────────────┘
                 │ payment.completed
                 │ payment.failed
                 ↓
          ┌──────────────┐
          │    Kafka     │
          └──────┬───────┘
                 │
                 ↓
          ┌────────────────┐
          │Delivery Service│
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

4. Stock Service ← order.created
   ├─ 재고 차감
   └─ stock.decreased 이벤트 발행

5. Delivery Service ← payment.completed
   ├─ 배송 시작
   └─ delivery.started 이벤트 발행
```

## 🎯 설계 결정

### 1. 토픽 정의

도메인 이벤트 중심으로 토픽을 분리했습니다.

| Topic               | Producer         | Consumer        | 설명        |
| ------------------- | ---------------- | --------------- | --------- |
| `order.created`     | Order Service    | Payment, Stock  | 주문 생성 이벤트 |
| `payment.completed` | Payment Service  | Order, Delivery | 결제 완료 이벤트 |
| `payment.failed`    | Payment Service  | Order           | 결제 실패 이벤트 |


#### 설계 의도

* 토픽명을 “도메인.행위” 형식으로 통일하여 이벤트 의미를 명확하게 표현
* 서비스 간 직접 호출 대신 이벤트 기반 비동기 통신 사용
* Consumer가 Producer 구현을 몰라도 되도록 느슨한 결합 유지
* 이벤트 추가 시 기존 서비스 수정 없이 Consumer만 확장 가능

---

### 2. 토픽 기본 설정

Kafka 토픽은 개발 환경 기준 아래와 같이 설정했습니다.

| 설정                 | 값        | 설명                |
| ------------------ | -------- | ----------------- |
| Partitions         | 3        | Consumer 확장 고려    |
| Replication Factor | 1        | 로컬 개발 환경 기준       |
| Ack Mode           | all      | 메시지 유실 방지         |
| Auto Offset Reset  | earliest | 초기 구독 시 전체 이벤트 조회 |

#### 설계 의도

* 파티션을 분리해 Consumer Scale-Out 가능
* 운영 환경에서는 Replication Factor를 3 이상으로 확장 예정
* `acks=all` 설정으로 브로커 장애 상황에서도 데이터 신뢰성 확보
* 동일 `orderId` 기준으로 Key를 지정하여 이벤트 순서 보장

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

#### 해결 방식

트랜잭션 내부에서 Outbox 이벤트를 먼저 저장하고, 트랜잭션 커밋 이후 Kafka 이벤트를 발행하도록 구현했습니다.

1. 주문 저장
2. BEFORE_COMMIT 단계에서 Outbox 이벤트 저장
3. Transaction Commit
4. AFTER_COMMIT 단계에서 Kafka 이벤트 발행
5. 발행 성공 시 Outbox 상태 업데이트
6. 발행 실패 이벤트는 배치 작업으로 재처리

#### 구현 방식 비교

| 방식                | 장점                | 단점                        |
| ----------------- | ----------------- | ------------------------- |
| Polling Publisher | 구현 단순             | Polling 주기만큼 지연 발생        |
| CDC(Debezium)     | 실시간 처리            | Kafka Connect 등 운영 복잡도 증가 |
| Event Listener    | Spring 친화적, 구현 간단 | 애플리케이션 의존적                |

#### 최종 선택

현재 프로젝트는 학습 및 로컬 환경 중심이므로 `TransactionalEventListener` 기반 Event Listener 방식을 채택했습니다.

```java
@TransactionalEventListener(
    phase = TransactionPhase.AFTER_COMMIT
)
```

#### 기대 효과

* DB 저장과 이벤트 발행 간 정합성 보장
* 트랜잭션 실패 시 이벤트 미발행 보장
* Kafka 장애 상황에서도 재처리 가능 구조 확보

---

### 4. 멱등성 보장

Kafka는 At-Least-Once 전달 방식을 사용하므로 동일 이벤트가 중복 전달될 수 있습니다.

이를 방지하기 위해 Consumer에서 `eventId` 기반 멱등성 처리를 적용합니다.

#### 처리 방식

```text
1. 이벤트 수신
2. eventId 중복 여부 확인
3. 이미 처리된 이벤트면 Skip
4. 미처리 이벤트만 비즈니스 로직 수행
```

#### 설계 의도

* Consumer 재시작 상황에서도 중복 처리 방지
* Kafka 재전송 상황 대응
* 결제/재고 차감 같은 중요 로직의 중복 수행 방지

#### 향후 개선 예정

* Redis 기반 이벤트 중복 캐시 적용
* processed_events 테이블 관리
* Exactly-Once 처리 전략 검토

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
│       │   └── published/          # 발행 이벤트 (OrderCreatedEvent)
│       ├── repository/
│       └── service/
│
├── payment-service/                # 결제 서비스 (port 8082)
│   └── src/main/java/com/sminoh/paymentservice/
│       ├── config/
│       ├── domain/
│       ├── event/
│       │   ├── consumed/           # 구독 이벤트 (OrderCreatedEvent)
│       │   └── published/          # 발행 이벤트 (PaymentCompletedEvent 등)
│       ├── repository/
│       └── service/
│
├── delivery-service/               # 배송 서비스 (port 8083) [예정]
└── notification-service/           # 알림 서비스 (port 8084) [예정]
```

## 🚀 실행 방법

### 1. 사전 요구사항

- Java 25
- Docker & Docker Compose

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

Kafka(9092), PostgreSQL(5432, 5433)가 기동됩니다.

### 4. 애플리케이션 실행

IntelliJ에서 각 서비스의 `Application.java` 실행 또는 터미널에서:

```bash
# Order Service
./gradlew :order-service:bootRun

# Payment Service (별도 터미널)
./gradlew :payment-service:bootRun
```

## 🧪 API 테스트

### 주문 생성

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-1",
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
  "orderId": "842da73e-18dc-4f5f-bc64-44314c777360",
  "status": "PENDING",
  "totalAmount": 30000,
  "userId": "user-123"
}
```

### 흐름 확인

주문 후 콘솔 로그에서 이벤트 흐름을 확인할 수 있습니다.

```
[Order Service]
action=ORDER_CREATED orderId=abc-123 userId=user-1 totalAmount=15000
event=PUBLISH topic=order.created orderId=abc-123

[Payment Service]
event=CONSUME topic=order.created orderId=abc-123
action=PG_API_CALL status=requesting
action=PAYMENT_COMPLETED orderId=abc-123 amount=15000
event=PUBLISH topic=payment.completed orderId=abc-123
```

### DB 확인

```bash
# Order DB
docker exec -it <order-db-container> psql -U postgres -d order_db -c "SELECT * FROM orders;"

# Payment DB
docker exec -it <payment-db-container> psql -U postgres -d payment_db -c "SELECT * FROM payments;"
```

### 로그 흐름 추적

`orderId`로 grep하면 전체 흐름이 한 번에 보입니다.

```bash
grep "orderId=abc-123" logs/*.log
```

## 📋 진행 현황

- [x] Order Service - 주문 생성 + 이벤트 발행
- [x] Payment Service - 결제 처리 + 이벤트 구독 및 발행
- [x] 로그 포맷 표준화 (event=, action= 접두어)
- [x] 이벤트 발행 테스트 코드 작성
- [x] Outbox Pattern (이벤트 발행 신뢰성)
- [ ] 멱등성 검증 및 테스트
- [ ] JAVA 25 문법 적용
- [ ] 예외 처리 (Business Exception)


## 📚 참고

- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
