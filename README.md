# Z4 Event Commerce

Proyecto educativo con cinco microservicios Spring Boot 4, Java 21, PostgreSQL y Kafka. Las aplicaciones se ejecutan desde IntelliJ para permitir debugging; Docker se utiliza únicamente para Kafka y las bases de datos.

## Documentación de servicios

| #   | Componente               | Documentación                                                                 |
|-----|--------------------------|-------------------------------------------------------------------------------|
| 1   | Infraestructura local    | [docs/01-infrastructure-local.md](docs/01-infrastructure-local.md)            |
| 2   | Autenticación            | [backend/auth-service/README.md](../backend/auth-service/README.md)           |
| 3   | Pedidos y Saga de compra | [backend/order-service/README.md](../backend/order-service/README.md)         |
| 4   | Inventario               | [backend/inventory-service/README.md](../backend/inventory-service/README.md) |
| 5   | Pagos                    | [backend/payment-service/README.md](../backend/payment-service/README.md)     |
| 6   | Operaciones (DLT)        | [docs/18-ops-service.md](docs/18-ops-service.md)                              |
| 7   | Plan a futuro            | [docs/19-roadmap.md](docs/19-roadmap.md)                                      |

## Guías centrales

Toda la documentación de infraestructura está centralizada en [`docs`](docs):

### Arquitectura y confiabilidad

Orden de lectura recomendado para entender el procesamiento distribuido:

| Orden | Guía                                   | Contenido                                                           |
|-------|----------------------------------------|---------------------------------------------------------------------|
| 05    | [Fundamentos de Kafka][d05]            | Brokers, topics, particiones, offsets y consumer groups.            |
| 07    | [Reintentos y DLT][d07]                | Clasificación de errores, reintentos y mensajes fallidos.           |
| 08    | [Saga de compra][d08]                  | Orquestación, estados, compensaciones y timeout.                    |
| 09    | [Transactional Inbox y Outbox][d09]    | Consistencia, idempotencia y publicación confiable.                 |
| 12    | [Reconciliación de Sagas][d12]         | Diagnóstico por lotes previo al replay.                             |
| 13    | [Nombres de topics][d13]               | Topics por capacidad: solicitudes, resultados y eventos públicos.   |
| 14    | [Particiones y consumer groups][d14]   | Quién decide qué, `group-id`, instancias y cómo elegir particiones. |
| 15    | [Estado de confiabilidad][d15]         | Qué está aplicado, qué resuelve cada mecanismo y qué falta.         |
| 16    | [ACK del producer y del consumer][d16] | Confirmación de envío, commit del offset y qué pasa si falta.       |
| 17    | [Qué es un broker][d17]                | Responsabilidades, clúster, KRaft, listeners y el broker local.     |
| 18    | [Ops Service y librerías][d18]         | DLT guardado, replay, API de Sagas y librerías `z4-shared-*`.       |
| 19    | [Plan a futuro][d19]                   | Multi-tenant, los tres fronts, orden de trabajo y pendientes.       |

### Infraestructura y datos

| Guía                                     | Contenido                                                     |
|------------------------------------------|---------------------------------------------------------------|
| [01 · Infraestructura local][d01]        | Componentes, puertos, ejecución y credenciales locales.       |
| [02 · Volúmenes y persistencia][d02]     | Volúmenes administrados, bind mounts y conservación de datos. |
| [03 · Estrategia de bases de datos][d03] | Database per service, Flyway, concurrencia y aislamiento.     |
| [04 · pgAdmin][d04]                      | Conexiones locales y archivos de configuración.               |
| [06 · Kafbat UI][d06]                    | Inspección de mensajes, consumidores, offsets y lag.          |

### Observabilidad

| Guía                                       | Contenido                                              |
|--------------------------------------------|--------------------------------------------------------|
| [10 · Logs con Alloy, Loki y Grafana][d10] | Arquitectura y configuración de la plataforma de logs. |
| [11 · Consultas LogQL][d11]                | Búsquedas y filtros prácticos para Grafana.            |

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

Verifica que tu terminal use JDK 21:

```bash
java -version
```

Si muestra otra versión (por ejemplo 17), cámbiala en esa terminal:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```

Para dejarlo fijo en todas las terminales, agrega esa línea al final de `~/.zshrc`.

## Orden para levantar todo

| #   | Paso                     | Cuándo                                   | Sección                           |
|-----|--------------------------|------------------------------------------|-----------------------------------|
| 1   | Infraestructura (Docker) | Siempre, primero                         | Infraestructura local             |
| 2   | Librerías `z4-shared-*`  | La primera vez y al cambiar una librería | Librerías compartidas             |
| 3   | Servicios                | Siempre, al final                        | Ejecutar y depurar desde IntelliJ |

## Infraestructura local

Inicia Docker Desktop. Después, desde la raíz del proyecto, ejecuta:

```bash
docker compose -f infra/docker/docker-compose.yml up -d
docker compose -f infra/docker/docker-compose.yml ps
```

La infraestructura queda disponible en:

| #   | Componente           | Dirección local                                |
|-----|----------------------|------------------------------------------------|
| 1   | Kafka                | `localhost:9092`                               |
| 2   | Auth PostgreSQL      | `localhost:5433/auth_db`                       |
| 3   | Orders PostgreSQL    | `localhost:5434/orders_db`                     |
| 4   | Inventory PostgreSQL | `localhost:5435/inventory_db`                  |
| 5   | Payments PostgreSQL  | `localhost:5436/payments_db`                   |
| 6   | Ops PostgreSQL       | `localhost:5437/ops_db`                        |
| 7   | pgAdmin              | [http://localhost:5050](http://localhost:5050) |

Los datos se conservan en volúmenes de Docker al ejecutar `docker compose -f infra/docker/docker-compose.yml down`.

Sin la infraestructura arriba los servicios no arrancan: al iniciar se conectan a su base de datos y Flyway ejecuta las migraciones.

## Librerías compartidas

Los servicios dependen de tres librerías propias (`z4-shared-core`, `z4-shared-security` y `z4-shared-kafka`) que no están en Maven Central. Hay que instalarlas en `~/.m2` antes de compilar o ejecutar cualquier servicio, y otra vez después de cambiar una librería:

```bash
backend/libs/install-all.sh
```

El script elige el JDK 21 por su cuenta. Para instalar una sola librería a mano:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```

```bash
./mvnw install
```

En IntelliJ, después de instalarlas, usa **Reload Maven Project** en cada servicio para que tome los `.jar` nuevos. Por qué hace falta instalarlas si no tienen `main`: [backend/libs/docs/01-librerias-compartidas.md](../backend/libs/docs/01-librerias-compartidas.md).

## Ejecutar y depurar desde IntelliJ

Abre cada repositorio Maven como módulo y configura JDK 21 como Project SDK y Maven Runner JDK. Ejecuta en modo **Debug** las siguientes clases:

Orden recomendado:

| #   | Aplicación                    | Puerto | Por qué en este lugar                                          |
|-----|-------------------------------|--------|----------------------------------------------------------------|
| 1   | `AuthServiceApplication`      | 8081   | Da el token para registrarse e iniciar sesión                  |
| 2   | `InventoryServiceApplication` | 8083   | Debe escuchar cuando llegue la reserva de stock                |
| 3   | `PaymentServiceApplication`   | 8084   | Debe escuchar cuando llegue el cobro                           |
| 4   | `OrderServiceApplication`     | 8082   | Inicia la Saga; conviene que los demás ya estén arriba         |
| 5   | `OpsServiceApplication`       | 8085   | Solo guarda lo que llega al DLT; puede ir en cualquier momento |

Otro orden no rompe nada: los servicios no se llaman entre sí, se comunican por Kafka. Si inventory está apagado cuando se crea un pedido, el mensaje espera en el topic y se procesa cuando inventory arranque. El único riesgo es que tarde tanto que la Saga llegue a su timeout y el pedido se cancele; por eso order-service va al final.

Desde la terminal, con JDK 21 activo, cada servicio se arranca dentro de su carpeta con:

```bash
./mvnw spring-boot:run
```

Breakpoints recomendados para seguir la Saga:

```text
OrderController
→ OrderServiceImpl
→ OutboxRelay                  (z4-shared-kafka)
→ InventoryEventConsumer
→ InventoryCommandServiceImpl
→ PurchaseSagaEventConsumer
→ PurchaseSagaServiceImpl
→ StockReservedEventStrategy
→ PaymentEventConsumer
→ PaymentServiceImpl
→ PaymentApprovedEventStrategy
```

Cada microservicio se ejecuta como un proceso Java independiente. IntelliJ mostrará una sesión de debugging para cada aplicación.

## Compilar

Cada backend es un proyecto Maven independiente. Con las librerías ya instaladas y JDK 21 activo, entra en el backend que deseas validar; por ejemplo:

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

[d01]: docs/01-infrastructure-local.md
[d02]: docs/02-docker-volumes.md
[d03]: docs/03-database-strategy.md
[d04]: docs/04-pgadmin.md
[d05]: docs/05-kafka-fundamentals.md
[d06]: docs/06-kafbat-ui-guide.md
[d07]: docs/07-kafka-retries-dlt.md
[d08]: docs/08-saga-pattern.md
[d09]: docs/09-transactional-inbox-outbox.md
[d10]: docs/10-observability.md
[d11]: docs/11-grafana-logql-queries.md
[d12]: docs/12-saga-reconciliation.md
[d13]: docs/13-kafka-topic-naming.md
[d14]: docs/14-kafka-partitions-consumer-groups.md
[d15]: docs/15-reliability-status.md
[d16]: docs/16-kafka-acknowledgments.md
[d17]: docs/17-kafka-broker.md
[d18]: docs/18-ops-service.md
[d19]: docs/19-roadmap.md
