# Spring EDA Kafka(미완)

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
          ┌──────────────┐
          │Delivery Service│
          └──────────────┘
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

각 서비스가 자신만의 이벤트 DTO를 정의하고, JSON 계약으로만 통신합니다.

**이유**: common 공유 시 한 필드 추가만으로도 모든 서비스 재배포가 필요해져 마이크로서비스의 독립 배포 원칙이 깨집니다.

**구현 방식**:
- Producer: 
- Consumer: 
- 각 서비스는 

### 2. 토픽 기본 설정

파티션 갯수, 컨슈머 갯수은 다음과 같이 지정한다.

### 3. outbox 패턴

Transactional outbox 패턴 

### 4. 멱등성 보장

Kafka의 At-Least-Once 보장 특성상 중복 이벤트가 발생할 수 있어, Consumer에서 `eventId` 기반 멱등성 처리를 합니다.

## 🛠 기술 스택

### Backend
- **Java 25**
- **Spring Boot 4.x**
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
  "orderId": "abc-123",
  "userId": "user-1",
  "totalAmount": 15000,
  "status": "PENDING"
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
- [x] 공통 모듈 제거 (서비스별 독립 Event)
- [x] 로그 포맷 표준화 (event=, action= 접두어)
- [x] PaymentService 단위 테스트
- [ ] Order Service - payment.completed 구독 (주문 CONFIRMED 처리)
- [ ] Outbox Pattern (이벤트 발행 신뢰성)
- [ ] Idempotency Table (Consumer 멱등성)
- [ ] Stock Service
- [ ] Delivery Service
- [ ] Notification Service
- [ ] Correlation ID 기반 분산 추적

## 📚 참고

- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)