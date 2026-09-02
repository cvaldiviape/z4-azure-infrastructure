# Observabilidad local de logs

Esta carpeta configura la primera etapa de observabilidad de Z4 Event Commerce. Su objetivo es reunir en Grafana los logs que los cuatro backends generan mientras se ejecutan y depuran desde IntelliJ.

## Flujo entre componentes

```text
Backend Spring Boot
  │ escribe logs/*.log
  ▼
Alloy
  │ lee, etiqueta, agrupa stack traces y envía por HTTP
  ▼
Loki
  │ almacena los logs y atiende consultas LogQL
  ▼
Grafana
    consulta Loki y muestra los resultados
```

Grafana no lee directamente los archivos y Alloy no proporciona una pantalla para investigar logs. Cada componente tiene una responsabilidad distinta.

## Archivos involucrados

| Archivo                                                       | Quién lo utiliza | Responsabilidad                                               |
|--------------------------------------------------------------|-------------------|---------------------------------------------------------------|
| `backend/*-service/src/main/resources/application-local.yml` | Spring Boot       | Crea el archivo de log de cada backend.                       |
| `infra/docker-compose.yml`                                   | Docker Compose    | Crea los contenedores, puertos, volúmenes y montajes.         |
| `alloy/config.alloy`                                         | Alloy             | Localiza, etiqueta y procesa los archivos antes de enviarlos. |
| `loki/loki-config.yml`                                       | Loki              | Configura la recepción, indexación y almacenamiento local.    |
| `grafana/provisioning/datasources/loki.yml`                  | Grafana           | Registra automáticamente a Loki como fuente de datos.         |
| `grafana/provisioning/dashboards/dashboards.yml`             | Grafana           | Indica dónde debe buscar dashboards declarados como archivos. |
| `grafana/provisioning/dashboards/json/purchase-saga-flow.json` | Grafana         | Crea el dashboard para seguir una compra por `correlationId`.  |

## 1. Spring Boot crea los archivos

Cada `application-local.yml` contiene una propiedad equivalente a esta:

```yaml
logging:
  file:
    name: ${LOG_FILE:logs/order-service.log}
```

Su lectura es:

- `logging.file.name`: indica a Spring Boot que escriba en un archivo además de mostrar los logs en IntelliJ.
- `LOG_FILE`: variable de entorno opcional que permite cambiar la ubicación sin modificar el YAML.
- `logs/order-service.log`: valor predeterminado cuando `LOG_FILE` no está definida.

La ruta es relativa al **Working directory** configurado en IntelliJ. Para Order Service debe ser `backend/order-service`; así el archivo se crea en `backend/order-service/logs/order-service.log`.

## 2. Docker comparte los archivos con Alloy

Los siguientes montajes de `docker-compose.yml` conectan las carpetas del Mac con el contenedor:

```yaml
volumes:
  - ../backend/order-service/logs:/var/log/z4/order-service:ro
```

La estructura significa:

| Parte | Significado |
|---|---|
| `../backend/order-service/logs` | Carpeta real del backend en el Mac. |
| `/var/log/z4/order-service` | Ruta con la que Alloy ve esa carpeta dentro de su contenedor. |
| `ro` | Montaje de solo lectura; Alloy puede leer los logs, pero no modificarlos. |

Por eso `config.alloy` utiliza rutas `/var/log/z4/...` y no rutas `backend/...`: Alloy se ejecuta dentro de Docker.

## 3. Alloy descubre y procesa los logs

### `local.file_match`

```alloy
local.file_match "order_service" {
  path_targets = [{
    "__path__"     = "/var/log/z4/order-service/*.log",
    "environment"  = "local",
    "service_name" = "order-service",
  }]
}
```

- `local.file_match`: busca archivos que coincidan con un patrón.
- `order_service`: nombre interno del componente dentro de Alloy; no es un topic ni un consumer group.
- `__path__`: ruta y patrón de los archivos que debe encontrar.
- `environment`: etiqueta que permite filtrar por entorno.
- `service_name`: etiqueta que identifica el backend que originó el log.

### `loki.source.file`

```alloy
loki.source.file "order_service" {
  targets    = local.file_match.order_service.targets
  forward_to = [loki.process.spring_boot.receiver]
}
```

- `targets`: recibe los archivos encontrados y sus etiquetas.
- `forward_to`: entrega cada línea al siguiente componente, `loki.process.spring_boot`.
- `receiver`: entrada por la que el siguiente componente recibe información.

### `loki.process` y `stage.multiline`

```alloy
loki.process "spring_boot" {
  stage.multiline {
    firstline     = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}"
    max_wait_time = "3s"
  }

  stage.regex {
    expression = "^(?P<spring_timestamp>...)"
  }

  stage.timestamp {
    source            = "spring_timestamp"
    format            = "RFC3339Nano"
    action_on_failure = "skip"
  }

  forward_to = [loki.write.local.receiver]
}
```

- `firstline`: reconoce una nueva entrada porque los logs de Spring Boot comienzan con una fecha como `2026-08-30T19:30:00`.
- Las líneas sin una nueva fecha se consideran continuación de la entrada anterior. Esto mantiene unido un stack trace Java.
- `max_wait_time`: espera como máximo tres segundos para completar una entrada multilínea.
- `stage.regex`: extrae como `spring_timestamp` la fecha que Spring Boot escribió al inicio del log.
- `stage.timestamp`: convierte esa fecha RFC 3339 en la hora oficial enviada a Loki.
- `action_on_failure: skip`: si una línea no tiene una fecha válida, conserva la hora asignada al leerla.
- Gracias a estas etapas, Grafana ordena por la hora real de ejecución y no por la hora en que Alloy leyó el archivo.
- Después entrega la entrada completa a `loki.write.local`.

### `loki.write`

```alloy
loki.write "local" {
  endpoint {
    url = "http://loki:3100/loki/api/v1/push"
  }
}
```

- `loki.write`: envía los logs procesados a Loki.
- `loki`: nombre del servicio declarado en Docker Compose. Los contenedores se comunican usando ese nombre.
- `3100`: puerto interno en el que escucha Loki.
- `/loki/api/v1/push`: endpoint HTTP utilizado para insertar logs.

## 4. Loki almacena los logs

El archivo `loki/loki-config.yml` configura una instancia local sencilla:

| Propiedad | Utilidad |
|---|---|
| `auth_enabled: false` | Desactiva autenticación y multitenencia únicamente para desarrollo local. |
| `server.http_listen_port: 3100` | Puerto HTTP en el que Loki recibe y consulta logs. |
| `common.path_prefix: /loki` | Carpeta base de los datos dentro del contenedor. |
| `replication_factor: 1` | Conserva una copia porque localmente solo existe una instancia. |
| `kvstore.store: inmemory` | Guarda en memoria el estado de coordinación de esta instancia única. |
| `storage.filesystem` | Guarda chunks y reglas en el filesystem local de Loki. |
| `store: tsdb` | Utiliza TSDB para el índice de los logs. |
| `object_store: filesystem` | Guarda los datos en disco, no en S3, Azure Blob u otro servicio cloud. |
| `schema: v13` | Define el formato de almacenamiento utilizado por Loki. |
| `index.period: 24h` | Organiza el índice en periodos diarios. |
| `allow_structured_metadata: true` | Permite metadata estructurada adicional. |
| `reporting_enabled: false` | Desactiva el envío de estadísticas de uso. |

Docker monta el volumen `loki_data` en `/loki`. Por eso los logs sobreviven a `docker compose down`; se eliminan solamente si también se borra el volumen.

## 5. Grafana consulta Loki

El archivo `grafana/provisioning/datasources/loki.yml` evita configurar el datasource manualmente:

```yaml
apiVersion: 1

datasources:
  - name: Loki
    uid: loki
    type: loki
    access: proxy
    url: http://loki:3100
    isDefault: true
    editable: false
```

| Propiedad | Utilidad |
|---|---|
| `apiVersion: 1` | Versión del formato de provisioning de Grafana. |
| `name` | Nombre mostrado en la interfaz. |
| `uid` | Identificador estable del datasource. |
| `type: loki` | Indica que la fuente de datos es Loki. |
| `access: proxy` | Grafana realiza la consulta desde su servidor. |
| `url: http://loki:3100` | Dirección interna de Loki dentro de la red Docker. |
| `isDefault: true` | Selecciona Loki como datasource predeterminado. |
| `editable: false` | Evita que la configuración declarada como código se cambie desde la interfaz. |

`http://localhost:3100` no se utiliza aquí porque, desde el contenedor Grafana, `localhost` sería el propio Grafana. El nombre `loki` resuelve al contenedor correcto.

## 6. Dashboard del flujo de compra

Grafana carga automáticamente el dashboard **Flujo de compra por Correlation ID** desde el archivo `purchase-saga-flow.json`. Para utilizarlo:

1. Reinicia Alloy y Grafana para aplicar la configuración.
2. Reinicia los backends y crea una nueva orden.
3. Copia el `correlationId` devuelto por Order Service.
4. En Grafana entra a **Dashboards > Z4 Event Commerce > Flujo de compra por Correlation ID**.
5. Pega el valor en el filtro **Correlation ID**.

El dashboard contiene:

| Panel | Información |
|---|---|
| **1. Inicio de la Saga** | Muestra `action=saga_started` y el estado inicial de la orden. |
| **2. Flujo cronológico estructurado** | Presenta una tabla con `Time`, `Service`, `Action`, `Event Type`, `Event ID`, `Topic`, `Order ID`, estados, `Exception Type` y `Error Message` desde antiguo hacia reciente. |
| **3. Fin de la Saga** | Muestra la transición que terminó con `sagaStatus=COMPLETED`. |
| **4. Logs técnicos originales** | Conserva el mensaje completo para revisar IDs, topics, particiones, offsets y stack traces. |

El filtro **Servicio** permite mostrar todos los participantes o limitar la línea temporal a Order, Inventory o Payment Service. El dashboard se actualiza cada cinco segundos.

Los logs ya almacenados conservan el timestamp anterior. La corrección se aplica únicamente a entradas que Alloy procese después de reiniciarse; para comprobarla utiliza una orden nueva.

## 7. Propiedades principales de Docker Compose

| Propiedad | Utilidad |
|---|---|
| `image` | Imagen y versión exacta del componente. |
| `command` | Archivo de configuración y argumentos utilizados al iniciar. |
| `ports` | Publica un puerto del contenedor en el Mac. |
| `depends_on` | Inicia primero la dependencia indicada. No sustituye un health check. |
| `volumes` | Conserva datos o comparte archivos entre el Mac y un contenedor. |
| `environment` | Define variables de entorno del contenedor. |

Los puertos publicados permiten abrir las interfaces desde el Mac:

| Componente | Dirección |
|---|---|
| Grafana | `http://localhost:3000` |
| Loki | `http://localhost:3100` |
| Alloy | `http://localhost:12345` |

## Ejemplo completo

Cuando Order Service lanza una excepción:

1. Spring Boot la escribe en `backend/order-service/logs/order-service.log`.
2. Docker muestra esa carpeta a Alloy como `/var/log/z4/order-service`.
3. `local.file_match.order_service` encuentra el archivo y agrega `service_name="order-service"`.
4. `loki.source.file.order_service` lee las nuevas líneas.
5. `loki.process.spring_boot` agrupa el mensaje y todo su stack trace.
6. `loki.write.local` envía la entrada a `http://loki:3100/loki/api/v1/push`.
7. Loki la almacena dentro del volumen `loki_data`.
8. Grafana consulta Loki cuando se ejecuta:

```logql
{service_name="order-service"} |= "ERROR"
```

Esta etapa centraliza logs y los flujos Kafka registran `eventId`, `correlationId`, `eventType` y `orderId` en sus puntos principales. Todavía no crea trazas distribuidas ni genera automáticamente un `traceId` mediante OpenTelemetry.

## Diagramas del flujo

Los gráficos utilizados para estudiar esta configuración están guardados dentro del proyecto:

1. [Flujo general de observabilidad](docs/images/01-flujo-observabilidad-general.png)
2. [Diagrama técnico original](docs/images/02-flujo-configuracion-original.png)
3. [Diagrama técnico con colores](docs/images/03-flujo-configuracion-colores.png)

Las versiones vectoriales se encuentran en [02-flujo-configuracion-original.svg](docs/images/02-flujo-configuracion-original.svg) y [03-flujo-configuracion-colores.svg](docs/images/03-flujo-configuracion-colores.svg). Sus fuentes Mermaid editables se encuentran en la carpeta [docs/mermaid](docs/mermaid).

Como material adicional, la versión detallada de alto contraste está disponible en [03-flujo-observabilidad-detallado.png](docs/images/03-flujo-observabilidad-detallado.png) y su fuente en [docs/03-flujo-observabilidad-detallado.html](docs/03-flujo-observabilidad-detallado.html).

## Guía de consultas en Grafana

Las consultas para filtrar por backend, nivel, errores, Kafka, PostgreSQL e identificadores de la Saga se encuentran en [docs/grafana-logql-queries.md](docs/grafana-logql-queries.md).
