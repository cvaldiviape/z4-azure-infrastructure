# Estado de confiabilidad: aplicado y pendiente

Resumen de los mecanismos que protegen la compra distribuida: qué ya está implementado, qué problema resuelve cada uno y qué falta. Actualiza esta guía en el mismo commit en que implementes o cambies un mecanismo.

## Aplicado

| #   | Mecanismo                      | Qué protege                        | Dónde                             | Guía      |
|-----|--------------------------------|------------------------------------|-----------------------------------|-----------|
| 1   | Compensación (`RELEASE_STOCK`) | Pago rechazado o sin stock         | Estrategias de la Saga (Order)    | [08][g08] |
| 2   | `@Transactional` y rollback    | Fallo a mitad de una escritura     | Los tres servicios                | [03][g03] |
| 3   | Retry + DLT                    | Error temporal o mensaje imposible | `KafkaConsumerRetryConfig`        | [07][g07] |
| 4   | Clasificación de excepciones   | Reintentar solo lo temporal        | `Custom*KafkaException`           | [07][g07] |
| 5   | Transactional Inbox            | Mensaje duplicado                  | Tabla `inbox_events`              | [09][g09] |
| 6   | Transactional Outbox           | BD y Kafka desincronizados         | Tabla `outbox_events` + scheduler | [09][g09] |
| 7   | Timeout de Saga (`TIMED_OUT`)  | Saga detenida                      | `PurchaseSagaTimeoutScheduler`    | [08][g08] |
| 8   | Optimistic locking (`version`) | Dos eventos sobre la misma Saga    | `purchase_sagas.version`          | [03][g03] |

Inbox, Outbox y Retry + DLT están en los tres servicios; el timeout y el `version` de la Saga son de Order.

En una línea: **Outbox evita perder al enviar, Inbox evita duplicar al recibir, Retry/DLT aparta lo que falla y el timeout detecta lo que se detuvo.**

## Pendiente del plan original

En el orden recomendado:

1. **Alertas en Grafana** cuando un mensaje llega a un DLT o una Saga pasa a `TIMED_OUT`. Los logs ya existen (`action=event_sent_to_dlt`), pero nadie recibe aviso.
2. **Reconciliación**: consultar a Inventory y Payment qué ocurrió realmente con una Saga `TIMED_OUT`. Diseñada en la [guía 12](12-saga-reconciliation.md), sin implementar.
3. **Replay manual por lotes** de mensajes del DLT.
4. **Límite de replay y parking lot** para mensajes que fallan repetidamente.
5. **Estado `REQUIRES_ATTENTION`** para casos que no se pueden recuperar automáticamente.
6. Más adelante: transacciones Kafka, Schema Registry y varios brokers.

## Huecos detectados en la revisión del código

| #   | Hueco                                         | Consecuencia                                |
|-----|-----------------------------------------------|---------------------------------------------|
| 1   | Compensación fallida (`RELEASE_STOCK` en DLT) | Saga `TIMED_OUT` con el stock aún reservado |
| 2   | Motivo del rechazo de pago no guardado        | No se sabe por qué falló el pago            |
| 3   | `event_type` del historial guarda el paso     | Cuesta saber qué evento llegó               |
| 4   | Comandos enviados fuera del historial         | No se ve qué se pidió a cada servicio       |
| 5   | Historial sin `correlation_id`                | No se cruza con Kafka ni Grafana            |
| 6   | Estrategias sin validar el estado actual      | Un evento tardío podría aplicarse igual     |
| 7   | Sin cancelación por el cliente                | Ese flujo no existe                         |
| 8   | `order-status-events-topic` sin consumer      | Se publica y nadie lo usa                   |
| 9   | Una partición y un consumer por servicio      | Sin paralelismo ni respaldo ([14][g14])     |

## Qué problema resuelve cada patrón

| #   | Problema                              | Solución                  | Estado    |
|-----|---------------------------------------|---------------------------|-----------|
| 1   | Error temporal                        | Retry                     | Aplicado  |
| 2   | Mensaje imposible de procesar         | DLT                       | Aplicado  |
| 3   | Mensaje entregado varias veces        | Inbox (idempotencia)      | Aplicado  |
| 4   | Base de datos y Kafka desincronizados | Outbox                    | Aplicado  |
| 5   | Saga detenida                         | Timeout                   | Aplicado  |
| 6   | Saber qué pasó con una Saga detenida  | Reconciliación            | Pendiente |
| 7   | Reprocesar mensajes fallidos          | Replay controlado del DLT | Pendiente |
| 8   | Enterarse de los fallos               | Alertas                   | Pendiente |

## Siguiente paso recomendado

1. **Trazabilidad del historial**: añadir `correlation_id`, el evento recibido, los comandos enviados y el motivo del fallo de pago. Son cambios pequeños y la reconciliación los necesita.
2. **Alertas**: los logs ya existen, solo falta configurar las reglas.
3. **Reconciliación**, empezando por la compensación fallida que deja stock reservado.

[g03]: 03-database-strategy.md
[g07]: 07-kafka-retries-dlt.md
[g08]: 08-saga-pattern.md
[g09]: 09-transactional-inbox-outbox.md
[g14]: 14-kafka-partitions-consumer-groups.md
