# Kafka: ACK del producer y del consumer

En Kafka se habla de «ACK» (*acknowledgment*, confirmación) en dos momentos distintos que suelen confundirse. Esta guía explica los dos, cómo están configurados en Z4 y qué pasa cuando falta uno.

## Resumen

| #   | ACK              | Quién confirma | A quién     | Qué significa                           |
|-----|------------------|----------------|-------------|-----------------------------------------|
| 1   | ACK del producer | El broker      | Al producer | «Guardé tu mensaje en la partición»     |
| 2   | ACK del consumer | El consumer    | Al broker   | «Mi grupo ya procesó hasta este offset» |

Ninguno de los dos significa que el otro servicio procesó el mensaje. El producer nunca sabe si alguien lo leyó; eso solo lo dice el siguiente evento de la Saga.

```text
payment-service                 Broker Kafka                     order-service
  send() ───── mensaje ─────▶  escribe en P0 (offset 42)
         ◀──── 1. ACK ───────  «guardado en P0, offset 42»
                               ◀──── poll ───────────────────────  pide mensajes
                               ───── offset 42 ─────────────────▶  procesa
                               ◀──── 2. ACK (commit offset) ─────  «grupo listo hasta 42»
```

## 1. ACK del producer

### Qué es

Cuando el producer envía un mensaje, el broker responde si lo guardó o no. La respuesta incluye la partición y el offset asignados.

### Cómo se usa en Z4

El Outbox publica con `publishAndWait` (`OrderEventProducer` y sus equivalentes en Inventory y Payment):

```java
CompletableFuture<SendResult<String, String>> send = kafkaTemplate.send(topic, eventKey, payload);
SendResult<String, String> sendResult = send.get(sendTimeoutMilliseconds, TimeUnit.MILLISECONDS);
```

1. `send()` es asíncrono: devuelve un *future* de inmediato, sin confirmación todavía.
2. `get(timeout)` espera el ACK del broker, como máximo `app.outbox.send-timeout-milliseconds` (10 s por defecto).
3. Si llega el ACK, el Outbox marca la fila como `PUBLISHED` y el log `action=event_published` muestra la partición y el offset.
4. Si no llega o el broker rechaza el mensaje, se lanza `CustomRetryableKafkaException`. La fila sigue `PENDING` con `attempts + 1` y el scheduler la reintenta en el siguiente ciclo.

Por eso el Outbox nunca marca como publicado algo que Kafka no confirmó.

### El parámetro `acks`

El producer de Kafka decide cuándo considera confirmado un envío:

| #   | `acks` | El broker responde cuando…                   | Riesgo                                      |
|-----|--------|----------------------------------------------|---------------------------------------------|
| 1   | `0`    | No responde; el producer no espera           | Se puede perder sin enterarse               |
| 2   | `1`    | El líder de la partición escribió el mensaje | Se pierde si el líder cae antes de replicar |
| 3   | `all`  | El líder y todas las réplicas sincronizadas  | El más seguro; algo más de latencia         |

Z4 no configura `acks`, así que usa el valor por defecto de los clientes Kafka modernos: `acks=all` con el producer idempotente activado. En local hay un solo broker y una réplica, por lo que «todas las réplicas» es solo el líder. En producción, con varios brokers, `acks=all` evita perder mensajes si cae uno.

### ACK perdido y duplicados

Si el broker guardó el mensaje pero el ACK no llegó a tiempo, el Outbox lo considera fallido y lo reenvía. El mensaje queda dos veces en el topic con el mismo `eventId`. Es el motivo de la entrega *al menos una vez* y lo resuelve el Inbox del consumidor.

## 2. ACK del consumer (commit del offset)

### Qué es

Después de procesar un mensaje, el consumer le dice al broker hasta qué offset llegó su grupo. El broker lo guarda en el topic interno `__consumer_offsets`. Si el servicio se reinicia, continúa desde ese punto.

### Cómo se usa en Z4

Los tres servicios configuran:

```yaml
spring:
  kafka:
    listener:
      ack-mode: record
```

`record` confirma el offset **después de cada mensaje**, cuando el método `@KafkaListener` termina sin excepción o cuando el mensaje ya se envió al DLT. Spring desactiva el auto-commit del cliente Kafka, así que el commit lo controla el contenedor de Spring y no un temporizador.

### Modos de ACK en Spring Kafka

| #   | `ack-mode`         | Cuándo se confirma el offset                                |
|-----|--------------------|-------------------------------------------------------------|
| 1   | `record`           | Después de cada mensaje (usado en Z4)                       |
| 2   | `batch`            | Después de procesar todo el lote de un `poll` (por defecto) |
| 3   | `time` / `count`   | Cada cierto tiempo o cada cierta cantidad de mensajes       |
| 4   | `manual`           | Cuando el código llama a `Acknowledgment.acknowledge()`     |
| 5   | `manual_immediate` | Igual que `manual`, pero confirma en el momento             |

Z4 usa `record` porque con un error solo se reprocesa ese mensaje y no todo el lote, y porque encaja con el DLT: un mensaje fallido se aparta, se confirma y la partición sigue.

### Orden dentro del consumer

```text
1. Llega el mensaje (offset 42)
2. Transacción PostgreSQL:
   ├─ ¿eventId ya está en inbox_events? → sí: se ignora
   ├─ aplica el cambio de negocio
   ├─ registra el eventId en inbox_events
   └─ guarda el siguiente evento en outbox_events
3. Commit de la transacción
4. ACK del offset 42 al broker
```

El ACK va **después** del commit de la base de datos. Si se confirmara antes y el servicio cayera, el mensaje se perdería: Kafka creería que ya se procesó.

## Qué pasa cuando falla algo

| #   | Situación                                | Resultado                                   | Quién lo cubre   |
|-----|------------------------------------------|---------------------------------------------|------------------|
| 1   | El broker no confirma el envío           | La fila sigue `PENDING` y se reintenta      | Outbox           |
| 2   | Mensaje guardado, pero el ACK no llegó   | Se reenvía y queda duplicado en el topic    | Inbox            |
| 3   | Consumer cae antes del commit de la BD   | Rollback; el mensaje se vuelve a leer       | `@Transactional` |
| 4   | Consumer cae después del commit, sin ACK | El mensaje se vuelve a leer                 | Inbox            |
| 5   | El listener lanza una excepción          | Reintentos y luego DLT; después se confirma | Retry + DLT      |

Los casos 2 y 4 son los duplicados inevitables de Kafka: por eso cada consumer debe ser idempotente.

## Cómo verlo

- **ACK del producer**: en Grafana, busca `action=event_published`; cada línea trae `partition` y `offset` confirmados por el broker.
- **ACK del consumer**: en Kafbat UI, **Consumers** → el grupo → columna **Current offset**. Si detienes un servicio y se publican mensajes, el **Lag** sube; al reiniciarlo, baja a 0.

Ver también [fundamentos de Kafka](05-kafka-fundamentals.md), [reintentos y DLT](07-kafka-retries-dlt.md), [Inbox y Outbox](09-transactional-inbox-outbox.md) y [particiones y consumer groups](14-kafka-partitions-consumer-groups.md).
