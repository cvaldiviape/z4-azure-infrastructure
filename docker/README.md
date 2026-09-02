# Infraestructura local

Esta carpeta contiene la infraestructura requerida para ejecutar y depurar los microservicios desde IntelliJ. Docker ejecuta Kafka, Kafbat UI, PostgreSQL, pgAdmin y la plataforma local de logs; las aplicaciones Java no se ejecutan dentro de contenedores.

## Componentes

| Componente | Puerto del Mac | Puerto del contenedor | Base de datos |
|---|---:|---:|---|
| Kafka | 9092 | 9092 | — |
| Kafbat UI | 8090 | 8080 | — |
| Grafana | 3000 | 3000 | — |
| Loki | 3100 | 3100 | — |
| Alloy | 12345 | 12345 | — |
| auth-postgres | 5433 | 5432 | `auth_db` |
| orders-postgres | 5434 | 5432 | `orders_db` |
| inventory-postgres | 5435 | 5432 | `inventory_db` |
| payments-postgres | 5436 | 5432 | `payments_db` |
| pgAdmin | 5050 | 80 | — |

El mapeo `5434:5432` significa que una aplicación ejecutada en el Mac se conecta a `localhost:5434` y Docker redirige la conexión al puerto `5432` de `orders-postgres`.

## Ejecutar

Desde la raíz del proyecto:

```bash
docker compose -f infra/docker/docker-compose.yml up -d
docker compose -f infra/docker/docker-compose.yml ps
```

Para iniciar únicamente Kafka y la base de pedidos:

```bash
docker compose -f infra/docker/docker-compose.yml up -d kafka orders-postgres
```

Para iniciar Kafka junto con su interfaz gráfica:

```bash
docker compose -f infra/docker/docker-compose.yml up -d kafka-ui
```

`depends_on` inicia Kafka automáticamente y espera a que su health check sea satisfactorio.

## Explorar Kafka con Kafbat UI

Abre [http://localhost:8090](http://localhost:8090) y selecciona el clúster `z4-event-commerce-local`. No se requieren credenciales porque esta interfaz se utiliza solamente en el entorno local.

Desde la interfaz puedes consultar:

- Los topics y sus particiones.
- Los eventos publicados, incluyendo su key, payload, headers y timestamp.
- Los consumer groups de los microservicios.
- Los offsets consumidos y el lag pendiente.

Kafka expone dos listeners porque el host cambia según el lugar desde donde se realiza la conexión:

| Consumidor | Dirección | Motivo |
|---|---|---|
| Backends ejecutados desde IntelliJ | `localhost:9092` | Se conectan desde el Mac al puerto publicado por Docker. |
| Kafbat UI | `kafka:29092` | Se conecta desde otro contenedor utilizando el nombre interno del servicio Kafka. |

`KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS` registra `kafka:29092` como la dirección inicial que Kafbat UI usa para descubrir el broker. Kafbat UI observa Kafka directamente; no se conecta a los controllers ni reemplaza a los consumers de los backends.

La búsqueda de mensajes, el seguimiento mediante `correlationId`, las particiones, los offsets, los consumer groups y el diagnóstico se explican paso a paso en [docs/kafbat-ui-guide.md](docs/kafbat-ui-guide.md).

Los reintentos limitados, los Dead Letter Topics y el procedimiento para revisar un mensaje que agotó sus intentos se explican en [docs/kafka-retries-dlt.md](docs/kafka-retries-dlt.md).

## Consultar logs con Grafana

La primera etapa de observabilidad utiliza tres componentes:

```text
Spring Boot → archivo .log → Alloy → Loki → Grafana
```

- Spring Boot escribe simultáneamente en la consola de IntelliJ y en la carpeta `logs` de cada backend.
- Alloy lee esos archivos, agrupa las líneas de cada stack trace y agrega las etiquetas `service_name` y `environment`.
- Loki almacena e indexa las etiquetas de los logs.
- Grafana consulta Loki mediante un datasource provisionado automáticamente.

Para iniciar solamente esta plataforma:

```bash
docker compose -f infra/docker/docker-compose.yml up -d grafana alloy
```

`depends_on` también inicia Loki. Después reinicia los backends desde IntelliJ para que creen sus archivos de logs.

Cada configuración de ejecución de IntelliJ debe utilizar como **Working directory** la carpeta de su propio backend. Por ejemplo, `backend/order-service` para Order Service. De esta manera, `logs/order-service.log` se crea en la carpeta que Alloy tiene montada.

Abre [http://localhost:3000](http://localhost:3000) e inicia sesión con:

```text
Usuario: admin
Contraseña: admin
```

En Grafana ingresa a **Explore**, selecciona el datasource `Loki` y utiliza LogQL. Para consultar un servicio:

```logql
{service_name="order-service"}
```

Para buscar errores:

```logql
{service_name="payment-service"} |= "ERROR"
```

Para localizar un evento cuando su identificador aparece en el mensaje o stack trace:

```logql
{service_name="payment-service"} |= "event-id-a-buscar"
```

En esta etapa los logs conservan el formato estándar de Spring Boot. Todavía no se configuraron OpenTelemetry, trazas distribuidas, métricas ni campos MDC estructurados; por ello un `eventId` solo será localizable cuando forme parte del texto registrado.

Los archivos de configuración son:

| Archivo | Finalidad |
|---|---|
| `observability/alloy/config.alloy` | Define qué archivos lee Alloy, las etiquetas y el destino Loki. |
| `observability/loki/loki-config.yml` | Configura Loki en modo local de una sola instancia y almacenamiento en filesystem. |
| `observability/grafana/provisioning/datasources/loki.yml` | Registra Loki automáticamente como datasource predeterminado de Grafana. |

La explicación propiedad por propiedad y el recorrido completo entre los componentes se encuentra en [observability/README.md](observability/README.md).

Para iniciar pgAdmin junto con las cuatro bases de datos:

```bash
docker compose -f infra/docker/docker-compose.yml up -d pgadmin
```

`depends_on` inicia automáticamente las bases requeridas por pgAdmin y espera a que sus health checks sean satisfactorios.

## Explorar PostgreSQL con pgAdmin

Abre [http://localhost:5050](http://localhost:5050) e inicia sesión con:

```text
Correo: admin@z4greed.com
Contraseña: admin
```

El grupo `Z4 Event Commerce Local` contiene las conexiones Auth, Orders, Inventory y Payments. Las definiciones se cargan desde `pgadmin/servers.json` y las contraseñas locales desde `pgadmin/pgpass`.

La relación entre estos archivos, Docker y los backends está explicada detalladamente en [pgadmin/README.md](pgadmin/README.md).

pgAdmin utiliza los hosts internos `auth-postgres`, `orders-postgres`, `inventory-postgres` y `payments-postgres` con el puerto `5432` porque se ejecuta dentro de la misma red de Docker Compose. Los backends ejecutados desde el Mac continúan utilizando `localhost` y los puertos `5433` a `5436`.

Para detener la infraestructura conservando los datos:

```bash
docker compose -f infra/docker/docker-compose.yml down
```

Los health checks permiten confirmar cuándo Kafka y las bases están disponibles. Los volúmenes conservan los datos entre reinicios.

La diferencia entre origen, destino, permisos, volúmenes administrados y bind mounts se explica paso a paso en [docs/docker-volumes.md](docs/docker-volumes.md).

## Credenciales locales

```text
Usuario: ecommerce
Contraseña: ecommerce
```

Estas credenciales son exclusivamente para desarrollo local.
