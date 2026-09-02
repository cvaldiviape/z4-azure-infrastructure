# Z4 Event Commerce

Proyecto educativo con cuatro microservicios Spring Boot 4, Java 21, PostgreSQL y Kafka. Las aplicaciones se ejecutan desde IntelliJ para permitir debugging; Docker se utiliza únicamente para Kafka y las bases de datos.

## Documentación

| Componente | Documentación |
|---|---|
| Infraestructura local | [infra/README.md](infra/README.md) |
| Estrategia de bases de datos | [docs/database/README.md](docs/database/README.md) |
| Autenticación | [backend/auth-service/README.md](backend/auth-service/README.md) |
| Pedidos y Saga de compra | [backend/order-service/README.md](backend/order-service/README.md) |
| Inventario | [backend/inventory-service/README.md](backend/inventory-service/README.md) |
| Pagos | [backend/payment-service/README.md](backend/payment-service/README.md) |

El README raíz explica el sistema completo. Cada README específico documenta las decisiones y variables propias de su componente. Cuando cambie un puerto, topic, consumer group, variable o responsabilidad, se debe actualizar el README del componente afectado en el mismo commit.

## Flujo

```text
ORDER_CREATED
→ STOCK_RESERVED
→ PAYMENT_REQUESTED
→ PAYMENT_APPROVED
→ ORDER_CONFIRMED
```

Cuando el pago falla:

```text
PAYMENT_FAILED
→ RELEASE_STOCK
→ STOCK_RELEASED
→ ORDER_CANCELLED
```

## Requisitos

- JDK 21.
- IntelliJ IDEA.
- Docker Desktop con Docker Compose.
- Postman o curl.

Verifica que Java y Maven utilicen JDK 21:

```bash
java -version
mvn -version
```

## Infraestructura local

Inicia Docker Desktop. Después, desde la raíz del proyecto, ejecuta:

```bash
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
```

La infraestructura queda disponible en:

| Componente | Dirección local |
|---|---|
| Kafka | `localhost:9092` |
| Auth PostgreSQL | `localhost:5433/auth_db` |
| Orders PostgreSQL | `localhost:5434/orders_db` |
| Inventory PostgreSQL | `localhost:5435/inventory_db` |
| Payments PostgreSQL | `localhost:5436/payments_db` |
| pgAdmin | [http://localhost:5050](http://localhost:5050) |

Los datos se conservan en volúmenes de Docker al ejecutar `docker compose -f infra/docker-compose.yml down`.

## Ejecutar y depurar desde IntelliJ

Abre cada repositorio Maven como módulo y configura JDK 21 como Project SDK y Maven Runner JDK. Ejecuta en modo **Debug** las siguientes clases:

| Aplicación | Puerto |
|---|---:|
| `AuthServiceApplication` | 8081 |
| `OrderServiceApplication` | 8082 |
| `InventoryServiceApplication` | 8083 |
| `PaymentServiceApplication` | 8084 |

Breakpoints recomendados para seguir la Saga:

```text
OrderController
→ OrderServiceImpl
→ OrderEventProducer
→ InventoryEventConsumer
→ InventoryServiceImpl
→ PurchaseSagaEventConsumer
→ PurchaseSagaServiceImpl
→ StockReservedEventStrategy
→ PaymentServiceImpl
→ PaymentApprovedEventStrategy
```

Cada microservicio se ejecuta como un proceso Java independiente. IntelliJ mostrará una sesión de debugging para cada aplicación.

## Compilar

Cada backend es un proyecto Maven independiente. Entra en el backend que deseas validar; por ejemplo:

```bash
cd backend/order-service
./mvnw clean package -DskipTests
```

Validación completa con la infraestructura iniciada:

```bash
./mvnw clean install
```

`install` ejecuta `contextLoads`, por lo que PostgreSQL debe estar disponible.

## Probar el flujo

Registra un usuario:

```bash
curl -X POST localhost:8081/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"student@test.com","password":"secret123","firstName":"Kafka","lastName":"Student"}'
```

Inicia sesión:

```bash
curl -X POST localhost:8081/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"student@test.com","password":"secret123"}'
```

Copia `data.accessToken` y crea un pedido:

```bash
curl -X POST localhost:8082/orders \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer TOKEN' \
  -d '{"listItems":[{"productId":100,"productName":"Laptop","unitPrice":3500.00,"quantity":1}],"paymentToken":"TEST_APPROVED"}'
```

Utiliza un valor diferente de `TEST_APPROVED` para observar la compensación por pago rechazado.

## Detener la infraestructura

```bash
docker compose -f infra/docker-compose.yml down
```

No agregues `-v` al comando anterior si deseas conservar los datos.

> El secreto JWT y las credenciales predeterminadas son exclusivamente para desarrollo local.
