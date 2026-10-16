# Z4 Event Commerce

Proyecto educativo con cuatro microservicios Spring Boot 4, Java 21, PostgreSQL y Kafka. Las aplicaciones se ejecutan desde IntelliJ para permitir debugging; Docker se utiliza únicamente para Kafka y las bases de datos.

## Documentación de servicios

| Componente | Documentación |
|---|---|
| Infraestructura local | [docs/01-infrastructure-local.md](docs/01-infrastructure-local.md) |
| Autenticación | [backend/auth-service/README.md](../backend/auth-service/README.md) |
| Pedidos y Saga de compra | [backend/order-service/README.md](../backend/order-service/README.md) |
| Inventario | [backend/inventory-service/README.md](../backend/inventory-service/README.md) |
| Pagos | [backend/payment-service/README.md](../backend/payment-service/README.md) |

## Guías centrales

Toda la documentación de infraestructura está centralizada en [`docs`](docs):

### Arquitectura y confiabilidad

Orden de lectura recomendado para entender el procesamiento distribuido:

| Orden | Guía | Contenido |
|---:|---|---|
| 05 | [Fundamentos de Kafka](docs/05-kafka-fundamentals.md) | Brokers, topics, particiones, offsets y consumer groups. |
| 07 | [Reintentos y DLT](docs/07-kafka-retries-dlt.md) | Clasificación de errores, reintentos y mensajes fallidos. |
| 08 | [Saga de compra](docs/08-saga-pattern.md) | Orquestación, estados, compensaciones y timeout. |
| 09 | [Transactional Inbox y Outbox](docs/09-transactional-inbox-outbox.md) | Consistencia, idempotencia y publicación confiable. |
| 12 | [Reconciliación de Sagas](docs/12-saga-reconciliation.md) | Diagnóstico por lotes previo al replay. |
| 13 | [Convención de nombres para topics](docs/13-kafka-topic-naming.md) | Topics orientados a capacidades, solicitudes, resultados y eventos públicos. |

### Infraestructura y datos

| Guía | Contenido |
|---|---|
| [01 · Infraestructura local](docs/01-infrastructure-local.md) | Componentes, puertos, ejecución y credenciales locales. |
| [02 · Volúmenes y persistencia](docs/02-docker-volumes.md) | Volúmenes administrados, bind mounts y conservación de datos. |
| [03 · Estrategia de bases de datos](docs/03-database-strategy.md) | Database per service, Flyway, concurrencia y aislamiento. |
| [04 · pgAdmin](docs/04-pgadmin.md) | Conexiones locales y archivos de configuración. |
| [06 · Kafbat UI](docs/06-kafbat-ui-guide.md) | Inspección de mensajes, consumidores, offsets y lag. |

### Observabilidad

| Guía | Contenido |
|---|---|
| [10 · Logs con Alloy, Loki y Grafana](docs/10-observability.md) | Arquitectura y configuración de la plataforma de logs. |
| [11 · Consultas LogQL](docs/11-grafana-logql-queries.md) | Búsquedas y filtros prácticos para Grafana. |

Este README funciona como portada e índice. Cada guía documenta una responsabilidad concreta. Cuando cambie un puerto, topic, consumer group, variable o decisión operativa, se debe actualizar la guía correspondiente en el mismo commit.

## Flujo

```text
ORDER_CREATED
→ STOCK_RESERVED
→ PAYMENT_REQUESTED
→ PAYMENT_APPROVED
→ Order: CONFIRMED
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
docker compose -f infra/docker/docker-compose.yml up -d
docker compose -f infra/docker/docker-compose.yml ps
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

Los datos se conservan en volúmenes de Docker al ejecutar `docker compose -f infra/docker/docker-compose.yml down`.

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
docker compose -f infra/docker/docker-compose.yml down
```

No agregues `-v` al comando anterior si deseas conservar los datos.

> El secreto JWT y las credenciales predeterminadas son exclusivamente para desarrollo local.
