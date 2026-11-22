# Kafka: particiones, consumers y consumer groups

Esta guía amplía la [guía 05](05-kafka-fundamentals.md) con las preguntas que suelen quedar abiertas: quién decide cada cosa, qué es físicamente una partición, para qué sirve el `group-id` y cómo se relacionan las instancias de un servicio con los consumers.

## Resumen en cinco frases

1. El **topic** es un nombre. Lo que se guarda en disco son sus **particiones**.
2. El **producer** decide en qué partición cae cada mensaje usando la **key**.
3. El **consumer** solo declara a qué **grupo** pertenece (`group-id`).
4. **Kafka** reparte las particiones entre los consumers del mismo grupo.
5. **Cada mensaje se entrega una vez por grupo**: dentro del grupo lo recibe un solo consumer y cada grupo distinto recibe su propia copia.

## Analogía: oficina de correos

| #   | Kafka               | Oficina de correos                                                     |
|-----|---------------------|------------------------------------------------------------------------|
| 1   | Topic               | Un mueble de casilleros con nombre, por ejemplo «pagos».               |
| 2   | Partición           | Cada casillero del mueble: 0, 1, 2…                                    |
| 3   | Offset              | La posición de una carta dentro de su casillero.                       |
| 4   | Producer            | El cartero: deja cada carta en un casillero según el número de pedido. |
| 5   | Consumer            | Un empleado que recoge cartas.                                         |
| 6   | Consumer group      | Un equipo de empleados con nombre; se reparten los casilleros.         |
| 7   | Offset confirmado   | El marcapáginas del equipo: «llegamos hasta la carta 57».              |
| 8   | Kafka (coordinador) | El jefe que asigna a cada empleado sus casilleros.                     |

## Qué es cada cosa a nivel de sistema

En local solo corren tres tipos de proceso:

| #   | Proceso                   | Dónde corre           | Qué hace                                          |
|-----|---------------------------|-----------------------|---------------------------------------------------|
| 1   | Broker Kafka              | Docker, puerto `9092` | Guarda particiones, coordina grupos y offsets     |
| 2   | Order, Inventory, Payment | JVM en IntelliJ       | `KafkaTemplate` produce, `@KafkaListener` consume |
| 3   | Kafbat UI                 | Docker, puerto `8090` | Solo inspecciona; no participa en el flujo        |

El broker usa la imagen `apache/kafka:4.0.0`.

El producer y el consumer **no son programas aparte**: son objetos dentro de tus servicios que se conectan por TCP al broker. Los servicios nunca se hablan entre sí; todo pasa por el broker.

```text
payment-service (JVM)                Docker: kafka                       order-service (JVM)
  KafkaTemplate  ──1 escribe──▶  Partición 0 (archivo .log)  ◀──3 lee (poll)──  @KafkaListener
                                 Coordinador de grupo        ◀──2 se une────
                                 __consumer_offsets          ◀──4 commit────
```

1. **Escribe**: `send(topic, key, value)` calcula la partición con la key y el broker añade el mensaje al final del archivo.
2. **Se une**: al arrancar, el consumer dice «soy del grupo X» y el coordinador le asigna particiones.
3. **Lee**: el consumer pide en bucle «¿hay algo nuevo en la partición 0 desde el offset 5?». Kafka no empuja mensajes; el consumer los pide (*pull*).
4. **Commit**: al terminar, el consumer confirma «el grupo X llegó hasta el offset 6». El broker lo guarda en el topic interno `__consumer_offsets`.

Leer **no borra** el mensaje. Kafka los elimina solo por retención (tiempo o tamaño), por eso varios grupos pueden leer el mismo topic y se puede releer moviendo el offset.

Las particiones son carpetas reales dentro del contenedor:

```bash
docker compose -f infra/docker/docker-compose.yml exec kafka ls /var/lib/kafka/data
```

Verás carpetas como `payment-processing-results-topic-0` (topic + número de partición) y `__consumer_offsets-0`. Se conservan gracias al volumen `kafka_data`.

## Quién decide qué

| #   | Decisión                           | Quién la toma           | Cuándo            | En el proyecto                    |
|-----|------------------------------------|-------------------------|-------------------|-----------------------------------|
| 1   | Cuántas particiones tiene un topic | Quien crea el topic     | Una vez           | `TopicBuilder` o valor del broker |
| 2   | En qué partición cae cada mensaje  | El producer, por la key | En cada envío     | `event_key` = id del pedido       |
| 3   | A qué grupo pertenece un consumer  | El consumer             | Al arrancar       | `group-id` en `application.yml`   |
| 4   | Qué consumer lee cada partición    | Kafka (coordinador)     | En cada rebalance | Automático                        |

Detalles:

- **Particiones**: se fijan con `TopicBuilder`, con el CLI `kafka-topics` o, si nadie las declara, con el valor por defecto del broker (`num.partitions`). En el proyecto, los DLT usan `TopicBuilder…partitions(1)` en `KafkaConsumerRetryConfig`; los topics principales no se declaran en el código y el broker los crea con su valor por defecto.
- **Partición de cada mensaje**: el producer calcula `hash(key) % particiones`.

Cada rol toma una sola decisión: el producer no sabe quién leerá, el consumer no elige particiones y Kafka no decide cuántas particiones hay.

## Particiones

Una partición es un registro ordenado (*append-only*) guardado en disco. Kafka las numera desde 0. En esta guía, `P0` es la abreviatura de partición 0.

```text
payment-processing-results-topic
├── partición 0: [offset 0][1][2][3]...
├── partición 1: [offset 0][1][2]...
└── partición 2: [offset 0][1]...
```

- El orden se garantiza **solo dentro de una partición**.
- Misma key implica misma partición. Como la key es el pedido, todos los eventos de un pedido llegan en orden aunque haya muchas particiones.
- El offset es único **por partición**, no por topic.

### ¿Las particiones son hilos?

No. Una partición es un **carril de almacenamiento**. Lo que corre en paralelo son los consumers. La partición permite que consumers distintos trabajen a la vez porque cada uno lee carriles diferentes.

**Paralelismo máximo de un grupo = número de particiones.** Con 3 particiones y 5 consumers, 2 consumers quedan ociosos.

### Cómo elegir el número de particiones

1. **Throughput**: `particiones ≥ mensajes por segundo esperados ÷ mensajes por segundo que procesa un consumer`.
2. **Escala futura**: dimensiona para el máximo de consumers que podrías necesitar, no para los de hoy.
3. **Aumentar después cuesta**: Kafka no permite reducir particiones, y al aumentarlas cambia `hash(key) % N`, de modo que un pedido puede cambiar de partición y perder el orden temporalmente.
4. **Demasiadas también cuesta**: más archivos en el broker y rebalances más lentos.
5. **Orden global**: si necesitas orden de todo el topic, solo puede haber una partición. Si basta el orden por entidad, como aquí, puedes tener muchas.

| #   | Situación                     | Particiones típicas                        |
|-----|-------------------------------|--------------------------------------------|
| 1   | Local o educativo             | 1–3                                        |
| 2   | Producción con carga moderada | 6–12                                       |
| 3   | Alto volumen                  | Según el cálculo de throughput, con margen |

Se suelen elegir números con muchos divisores (6, 12, 24) para repartir parejo entre 1, 2, 3, 4 o 6 consumers.

## Consumer group y `group-id`

### Dónde se define

Solo en la configuración del **consumer**. El producer no lo usa.

```yaml
spring:
  kafka:
    consumer:
      group-id: purchase-saga-consumer-group-id   # dentro de "consumer"
    producer:
      key-serializer: ...                          # el producer no tiene group-id
```

| #   | Servicio          | group-id                              |
|-----|-------------------|---------------------------------------|
| 1   | order-service     | `purchase-saga-consumer-group-id`     |
| 2   | inventory-service | `inventory-request-consumer-group-id` |
| 3   | payment-service   | `payment-request-consumer-group-id`   |

Los `@KafkaListener` del proyecto solo indican el topic. Spring les asigna el `group-id` del `application.yml`, así que los dos listeners de order-service pertenecen al mismo grupo. Se podría sobrescribir con `@KafkaListener(topics = "...", groupId = "otro")`, pero el proyecto no lo hace.

No hay que crear el grupo en Kafka: es solo un nombre. La primera vez que un consumer se conecta con ese nombre, el broker crea el grupo. Si el grupo no tiene offset guardado, `auto-offset-reset: earliest` hace que empiece desde el mensaje más antiguo.

### Para qué sirve

Es el **nombre del equipo que lee**. Con él Kafka sabe dos cosas:

1. **Repartir sin duplicar**: los consumers con el mismo `group-id` se reparten las particiones y cada mensaje lo recibe **uno solo** de ellos.
2. **Recordar por dónde va**: Kafka guarda el offset **por grupo**. Si reinicias order-service, continúa desde el último offset confirmado y no pierde mensajes.

Varios consumers con el mismo `group-id` se comportan como **un solo lector lógico con varias manos**.

La frase correcta es «cada **mensaje** lo recibe un solo consumer del grupo», no «cada consumer recibe un solo mensaje». Cada consumer recibe muchos mensajes: los de sus particiones.

### ¿Para qué tener varios consumers en un grupo?

- **Velocidad**: trabajan en paralelo sobre particiones distintas.
- **Tolerancia a fallos**: si uno cae, Kafka pasa sus particiones a los demás mediante un **rebalance**.

### Mismo grupo o grupos distintos

| Caso                          | group-id | Resultado                                                    |
|-------------------------------|----------|--------------------------------------------------------------|
| Instancias del mismo servicio | El mismo | Se reparten las particiones; cada mensaje se procesa una vez |
| Servicios distintos           | Distinto | Cada servicio recibe **todos** los mensajes con su offset    |

Ejemplo de servicios distintos: Order y un futuro servicio de notificaciones.

### Errores típicos

- **Dos servicios distintos con el mismo `group-id`**: Kafka los trata como un equipo y les reparte los mensajes. Si Order y Notificaciones compartieran grupo, un `PAYMENT_APPROVED` llegaría a solo uno de ellos y el pedido podría quedarse en `PAYMENT_PENDING` hasta el timeout. Además, reiniciar uno provocaría rebalances en el otro.
- **Cambiar el `group-id` de un servicio**: Kafka lo considera un grupo nuevo y, con `earliest`, relee todo el topic desde el inicio. El Inbox frenaría los duplicados, pero no conviene hacerlo por accidente.

## Instancias frente a consumers

| #   | Término        | Qué es                                                                      |
|-----|----------------|-----------------------------------------------------------------------------|
| 1   | Instancia      | Un proceso Java: un `java -jar`, un pod o una sesión Debug de IntelliJ      |
| 2   | Consumer       | Un cliente Kafka dentro de la instancia; puede haber varios (`concurrency`) |
| 3   | Consumer group | Todos los consumers, de todas las instancias, con el mismo `group-id`       |

```text
total consumers del grupo = instancias × concurrency
consumers útiles como máximo = número de particiones
```

Ejemplo con 6 particiones:

```text
A) 1 instancia × concurrency 3        B) 3 instancias × concurrency 1
   order-service :8082                   order-service :8082 → P0 P1
    ├─ consumer 1 → P0 P1                order-service :8092 → P2 P3
    ├─ consumer 2 → P2 P3                order-service :8102 → P4 P5
    └─ consumer 3 → P4 P5

C) 2 instancias × concurrency 3 = 6 consumers → una partición cada uno
```

Para Kafka, A y B son equivalentes. La diferencia es operativa:

- **Más hilos (A)**: más barato, pero si cae la instancia caen todos sus consumers. Cada hilo usa una conexión de base de datos, así que el pool de Hikari debe acompañar.
- **Más instancias (B)**: tolera fallos y reparte CPU y memoria.

En producción lo habitual es una mezcla: **al menos dos réplicas** con `concurrency` bajo (1–3), escalando réplicas según el lag (por ejemplo con KEDA en Kubernetes). Por eso las particiones se dimensionan como `réplicas máximas × concurrency`.

### Ejemplo con dos grupos y escalado

Topic `payment-processing-results-topic` con 6 particiones. `notification-service` es hipotético.

```text
Una instancia de cada servicio
  purchase-saga-consumer-group-id   order-service        → P0 P1 P2 P3 P4 P5
  notification-consumer-group-id    notification-service → P0 P1 P2 P3 P4 P5

Order escala a 3 instancias (rebalance solo en su grupo)
  purchase-saga-consumer-group-id   order-service :8082  → P0 P1
                                    order-service :8092  → P2 P3
                                    order-service :8102  → P4 P5
  notification-consumer-group-id    notification-service → P0 P1 P2 P3 P4 P5   (no se entera)
```

Un `PAYMENT_APPROVED` del pedido 103 que cae en P1 lo procesa solo la instancia `:8082` dentro del grupo de Order, y Notificaciones también lo recibe.

## Relación con Inbox y Outbox

La regla «cada mensaje una vez por grupo» se cumple en condiciones normales. Si un consumer procesa un mensaje y cae **antes** de confirmar el offset, tras el rebalance otro consumer del grupo lo recibe de nuevo. Por eso la entrega es *al menos una vez* y el proyecto usa el **Inbox** para descartar `eventId` ya procesados. Ver [Transactional Inbox y Outbox](09-transactional-inbox-outbox.md).

## Estado actual del proyecto

- Topics principales con una partición, que es el valor por defecto del broker, y DLT declarados con `partitions(1)`.
- Una instancia por servicio y sin `concurrency` configurado, es decir, un consumer por grupo.
- El procesamiento es en serie dentro de cada servicio: es determinista y fácil de depurar, pero no hay paralelismo ni respaldo.
- Si levantas tres instancias de order-service con una sola partición, solo una lee y las otras dos quedan ociosas.

Para observar paralelismo real habría que crear los topics con, por ejemplo, tres particiones y configurar `spring.kafka.listener.concurrency: 3`. La key por pedido ya preserva el orden de cada compra.

## Cómo verlo en Kafbat UI

1. Abre **Consumers** y selecciona `purchase-saga-consumer-group-id`.
2. Cada fila muestra una partición con su **Consumer ID** (uno por consumer activo), **Current offset** y **Lag**.
3. Levanta otra instancia del servicio y observa el rebalance: cambia el consumer asignado a cada partición.

Más detalles en la [guía de Kafbat UI](06-kafbat-ui-guide.md).
