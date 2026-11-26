# Kafka: qué es un broker

Un **broker** es el servidor de Kafka: un programa que recibe mensajes, los guarda en disco y los entrega a quien los pide. Cuando decimos «Kafka está en `localhost:9092`», nos referimos al broker.

## La idea en una frase

Los servicios no se hablan entre sí: **todos hablan con el broker**. El producer le deja mensajes y el consumer se los pide.

```text
payment-service ── escribe ──▶  BROKER  ◀── lee ── order-service
                               (guarda en disco)
```

### Analogía

El broker es la **oficina de correos**: tiene los casilleros (particiones), recibe las cartas del cartero (producer), las guarda y se las entrega a cada equipo de empleados (consumer group), recordando hasta dónde recogió cada uno.

## Qué hace un broker

| #   | Responsabilidad       | Detalle                                                             |
|-----|-----------------------|---------------------------------------------------------------------|
| 1   | Recibir mensajes      | Atiende `send()` de los producers y responde el ACK                 |
| 2   | Guardar en disco      | Cada partición es una carpeta con archivos `.log` de solo escritura |
| 3   | Entregar mensajes     | Responde los `poll` de los consumers desde un offset dado           |
| 4   | Coordinar grupos      | Asigna particiones a los consumers de cada grupo (rebalance)        |
| 5   | Guardar offsets       | Registra el avance de cada grupo en el topic `__consumer_offsets`   |
| 6   | Aplicar retención     | Borra mensajes viejos por tiempo o tamaño, nunca porque se leyeron  |
| 7   | Replicar (en clúster) | Copia sus particiones a otros brokers para no perder datos          |

Un broker **no** ejecuta lógica de negocio, no transforma mensajes y no sabe qué es un pedido. Solo guarda y entrega bytes.

## Broker y clúster

Un **clúster** es un grupo de brokers que trabajan juntos. Las particiones de un topic se reparten entre ellos.

```text
Clúster de producción (3 brokers)

Broker 1: P0 (líder)    P1 (réplica)
Broker 2: P1 (líder)    P2 (réplica)
Broker 3: P2 (líder)    P0 (réplica)
```

- **Líder**: el broker que atiende lecturas y escrituras de una partición.
- **Réplica**: una copia en otro broker. Si el líder cae, una réplica sincronizada pasa a ser líder.
- **Factor de replicación**: cuántas copias tiene cada partición. Con 3, se puede perder un broker sin perder datos.

### Controller y KRaft

Alguien tiene que decidir qué broker es líder de cada partición y llevar el registro de topics y brokers. Ese rol es el **controller**. Desde Kafka 4.0 se usa **KRaft** (Kafka Raft): los propios brokers eligen al controller entre ellos y ya no se necesita ZooKeeper.

## El broker de Z4 en local

Definido en `infra/docker/docker-compose.yml`:

| #   | Configuración                    | Valor en Z4                      | Significado                              |
|-----|----------------------------------|----------------------------------|------------------------------------------|
| 1   | Imagen                           | `apache/kafka:4.0.0`             | Kafka 4 con KRaft, sin ZooKeeper         |
| 2   | `KAFKA_NODE_ID`                  | `1`                              | Identificador del único nodo             |
| 3   | `KAFKA_PROCESS_ROLES`            | `broker,controller`              | El mismo proceso es broker y controller  |
| 4   | `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@kafka:9093`                   | El único votante del quórum es este nodo |
| 5   | Volumen                          | `kafka_data:/var/lib/kafka/data` | Las particiones sobreviven a reinicios   |
| 6   | Factor de replicación interno    | `1`                              | Una sola copia: no hay otros brokers     |

Es decir, Z4 corre **un clúster de un solo nodo**: suficiente para aprender, pero si el contenedor se detiene, Kafka deja de funcionar hasta que vuelva.

### Listeners: por qué hay tres puertos

Un *listener* es una dirección donde el broker escucha conexiones. Z4 define tres:

| #   | Listener     | Puerto  | Quién se conecta                        | Dirección anunciada |
|-----|--------------|---------|-----------------------------------------|---------------------|
| 1   | `EXTERNAL`   | `9092`  | Tus servicios en IntelliJ, desde el Mac | `localhost:9092`    |
| 2   | `INTERNAL`   | `29092` | Otros contenedores, como Kafbat UI      | `kafka:29092`       |
| 3   | `CONTROLLER` | `9093`  | El rol controller (KRaft)               | Solo interno        |

La **dirección anunciada** (`KAFKA_ADVERTISED_LISTENERS`) es la que el broker le devuelve al cliente para que se reconecte. Por eso hacen falta dos: desde el Mac el broker se llama `localhost`, pero desde otro contenedor se llama `kafka`. Si se anunciara la dirección equivocada, el cliente se conectaría al principio y luego fallaría.

### Cómo lo encuentran los servicios

Cada `application.yml` indica la dirección inicial del broker:

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
```

*Bootstrap* significa «punto de entrada»: el cliente se conecta ahí, pide la lista de brokers y particiones, y a partir de entonces habla directamente con el líder de cada partición. En un clúster se suelen poner varias direcciones por si una no responde.

## Local frente a producción

| #   | Aspecto               | Local (Z4)                 | Producción típica                      |
|-----|-----------------------|----------------------------|----------------------------------------|
| 1   | Brokers               | 1                          | 3 o más                                |
| 2   | Roles                 | Broker y controller juntos | Controllers dedicados o combinados     |
| 3   | Factor de replicación | 1                          | 3                                      |
| 4   | `acks` del producer   | `all` = solo el líder      | `all` = líder y réplicas sincronizadas |
| 5   | Si cae un broker      | Kafka no disponible        | Otra réplica asume como líder          |
| 6   | Seguridad             | `PLAINTEXT`, sin usuario   | TLS y autenticación (SASL)             |

## Comandos útiles

Ver si el broker está saludable:

```bash
docker compose -f infra/docker/docker-compose.yml ps kafka
```

Listar los topics que guarda:

```bash
docker compose -f infra/docker/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Ver sus particiones en disco:

```bash
docker compose -f infra/docker/docker-compose.yml exec kafka ls /var/lib/kafka/data
```

Ver también [fundamentos de Kafka](05-kafka-fundamentals.md), [particiones y consumer groups](14-kafka-partitions-consumer-groups.md) y [ACK del producer y del consumer](16-kafka-acknowledgments.md).
