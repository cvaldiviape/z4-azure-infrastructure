# Bases de datos y tablas

Cada servicio tiene su propia base de datos (ver [03 · Estrategia de bases de datos](03-database-strategy.md)). Esta guía lista qué tablas tiene cada una y para qué sirven. El esquema se tomó de las bases locales después de aplicar todas las migraciones.

## Dónde se crea cada cosa

| #   | Qué                  | Dónde                                | Quién              | Cuándo                         |
|-----|----------------------|--------------------------------------|--------------------|--------------------------------|
| 1   | La base y su usuario | `docker-compose.yml` (`POSTGRES_DB`) | Imagen de Postgres | Primer arranque, volumen vacío |
| 2   | Tablas e índices     | `db/migration/V*.sql` del servicio   | Flyway             | Cada arranque, solo lo nuevo   |

No hay ningún `CREATE DATABASE` escrito a mano. Si se cambia `POSTGRES_DB` con el volumen ya creado, no pasa nada: hay que borrar el volumen (y sus datos) para que se aplique.

Todas las bases usan el usuario `ecommerce` con contraseña `ecommerce` (solo para desarrollo local). Todas tienen además `flyway_schema_history`, donde Flyway anota qué migraciones ya aplicó; no se modifica a mano.

## Resumen

| #   | Base           | Servicio          | Contenedor           | Puerto |
|-----|----------------|-------------------|----------------------|--------|
| 1   | `auth_db`      | auth-service      | `auth-postgres`      | 5433   |
| 2   | `orders_db`    | order-service     | `orders-postgres`    | 5434   |
| 3   | `inventory_db` | inventory-service | `inventory-postgres` | 5435   |
| 4   | `payments_db`  | payment-service   | `payments-postgres`  | 5436   |
| 5   | `ops_db`       | ops-service       | `ops-postgres`       | 5437   |

## `auth_db` · auth-service

| #   | Tabla        | Para qué                                 |
|-----|--------------|------------------------------------------|
| 1   | `users`      | Usuarios registrados                     |
| 2   | `roles`      | Catálogo de roles (`CUSTOMER`, `ADMIN`…) |
| 3   | `user_roles` | Qué roles tiene cada usuario             |

- `users`: `id`, `email`, `password_hash`, `first_name`, `last_name`, `status`, `created_at`, `updated_at`
- `roles`: `id`, `code`, `name`
- `user_roles`: `user_id`, `role_id`

Para usar los endpoints `/ops/**` hace falta un usuario con el rol `ADMIN` en `user_roles`; nadie lo tiene por defecto.

## `orders_db` · order-service

| #   | Tabla                     | Para qué                                             |
|-----|---------------------------|------------------------------------------------------|
| 1   | `orders`                  | Pedido del cliente y su estado                       |
| 2   | `order_items`             | Productos de cada pedido                             |
| 3   | `purchase_sagas`          | Estado actual de la Saga de cada pedido              |
| 4   | `purchase_saga_histories` | Línea de tiempo de la Saga, un registro por paso     |
| 5   | `outbox_events`           | Eventos por publicar en Kafka (ver *Outbox e inbox*) |
| 6   | `inbox_events`            | Eventos ya procesados (ver *Outbox e inbox*)         |

- `orders`: `id`, `customer_id`, `status`, `total_amount`, `currency`, `correlation_id`, `payment_token`, `created_at`, `updated_at`
- `order_items`: `id`, `order_id`, `product_id`, `product_name`, `unit_price`, `quantity`, `subtotal`
- `purchase_sagas`: `id`, `order_id`, `status`, `current_step`, `last_event_id`, `error_message`, `timed_out_at`, `version`, `created_at`, `updated_at`
- `purchase_saga_histories`: `id`, `purchase_saga_id`, `order_id`, `order_status`, `saga_status`, `event_type`, `event_id`, `error_message`, `created_at`

`purchase_saga_histories` alimenta la API `/ops/purchase-sagas/{orderId}`. Guarda cada cambio de estado de la Saga, no los comandos que envía: esos están en `outbox_events` (ver [22 · Historial de la Saga y outbox](22-saga-history-outbox.md)).

## `inventory_db` · inventory-service

| #   | Tabla                    | Para qué                                             |
|-----|--------------------------|------------------------------------------------------|
| 1   | `inventories`            | Stock por producto                                   |
| 2   | `inventory_reservations` | Stock apartado para un pedido                        |
| 3   | `inventory_movements`    | Auditoría de cada cambio de stock                    |
| 4   | `outbox_events`          | Eventos por publicar en Kafka (ver *Outbox e inbox*) |
| 5   | `inbox_events`           | Eventos ya procesados (ver *Outbox e inbox*)         |

- `inventories`: `id`, `product_id`, `available_quantity`, `reserved_quantity`, `version`, `updated_at`
- `inventory_reservations`: `id`, `reservation_id`, `order_id`, `product_id`, `quantity`, `status`, `created_at`, `updated_at`
- `inventory_movements`: `id`, `product_id`, `order_id`, `reservation_id`, `event_id`, `movement_type`, `quantity`, `available_before`, `available_after`, `reserved_before`, `reserved_after`, `created_at`

`version` en `inventories` es el bloqueo optimista: evita que dos reservas simultáneas descuenten el mismo stock.

## `payments_db` · payment-service

| #   | Tabla              | Para qué                                             |
|-----|--------------------|------------------------------------------------------|
| 1   | `payments`         | Pago de cada pedido y su resultado                   |
| 2   | `payment_attempts` | Cada intento de cobro                                |
| 3   | `outbox_events`    | Eventos por publicar en Kafka (ver *Outbox e inbox*) |
| 4   | `inbox_events`     | Eventos ya procesados (ver *Outbox e inbox*)         |

- `payments`: `id`, `payment_id`, `order_id`, `customer_id`, `amount`, `currency`, `status`, `failure_reason`, `created_at`, `updated_at`
- `payment_attempts`: `id`, `payment_id`, `attempt_number`, `result`, `error_message`, `created_at`

## `ops_db` · ops-service

| #   | Tabla                  | Para qué                                                                   |
|-----|------------------------|----------------------------------------------------------------------------|
| 1   | `dead_letter_messages` | Cada mensaje que llegó a un topic `*-dlt`, con su error y cómo se resolvió |

Columnas de `dead_letter_messages`, por grupo:

- **Dónde estaba en Kafka:** `dlt_topic`, `dlt_partition`, `dlt_offset`, `original_topic`, `original_partition`, `original_offset`, `consumer_group`
- **El mensaje:** `message_key`, `payload`, `event_id`, `event_type`, `correlation_id`, `aggregate_id`
- **El error:** `exception_class`, `exception_cause_class`, `exception_message`, `exception_stacktrace`, `failed_at`, `received_at`
- **La resolución:** `status` (`PENDING`, `REPLAYED`, `DISCARDED`), `resolved_by`, `resolved_at`, `resolution_note`, `replay_of_id`

`replay_of_id` apunta al mensaje original cuando uno reprocesado vuelve a fallar.

## Outbox e inbox

order, inventory y payment tienen estas dos tablas con las mismas columnas. Las crea la migración de cada servicio y las usa la librería `z4-shared-kafka`.

| #   | Tabla           | Para qué                                                                         |
|-----|-----------------|----------------------------------------------------------------------------------|
| 1   | `outbox_events` | Evento guardado junto con el cambio de negocio; `OutboxRelay` lo publica después |
| 2   | `inbox_events`  | Eventos ya procesados, para ignorar duplicados                                   |

- `outbox_events`: `event_id`, `aggregate_id`, `correlation_id`, `event_type`, `topic`, `event_key`, `payload`, `status`, `attempts`, `last_error`, `created_at`, `published_at`
- `inbox_events`: `event_id`, `event_type`, `aggregate_id`, `correlation_id`, `previous_event_id`, `producer`, `source_topic`, `processed_at`

Más detalle en [09 · Transactional Inbox y Outbox](09-transactional-inbox-outbox.md).

## Ver las tablas

En pgAdmin (`http://localhost:5050`) cada base ya está configurada. Desde la terminal, por ejemplo:

```bash
docker exec -it docker-orders-postgres-1 psql -U ecommerce -d orders_db -c '\dt'
```
