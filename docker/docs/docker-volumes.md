# Volúmenes de Docker

Un contenedor tiene su propio filesystem, pero ese almacenamiento pertenece al contenedor. Si el contenedor se elimina, también se elimina la información escrita únicamente dentro de él.

Los volúmenes permiten guardar información fuera del filesystem temporal del contenedor y conectarla con una ruta interna.

## Sintaxis fundamental

En Docker Compose, un montaje normalmente utiliza esta estructura:

```text
ORIGEN:DESTINO:PERMISO
```

Ejemplo:

```yaml
services:
  auth-postgres:
    volumes:
      - auth_db_data:/var/lib/postgresql/data
```

Se interpreta de la siguiente manera:

| Parte   | Valor                      | Función                                                 |
|---------|----------------------------|---------------------------------------------------------|
| Origen  | `auth_db_data`             | Lugar persistente administrado por Docker.              |
| Destino | `/var/lib/postgresql/data` | Carpeta dentro del contenedor donde PostgreSQL trabaja. |
| Permiso | No se especifica           | Docker utiliza lectura y escritura (`rw`) por defecto.  |

La configuración anterior equivale a:

```yaml
- auth_db_data:/var/lib/postgresql/data:rw
```

## ¿Qué hace el destino?

El destino siempre es una ruta dentro del contenedor:

```text
/var/lib/postgresql/data
```

La imagen oficial de PostgreSQL utiliza esa carpeta para guardar físicamente:

- Bases de datos.
- Tablas y registros.
- Índices.
- Información de transacciones.
- Metadata interna de PostgreSQL.

Al montar un volumen sobre esa ruta, todo lo que PostgreSQL escriba allí se guarda en el volumen:

```text
PostgreSQL dentro del contenedor
              │ escribe
              ▼
/var/lib/postgresql/data
              │ está montado sobre
              ▼
       auth_db_data
```

La aplicación continúa trabajando con `/var/lib/postgresql/data`. No necesita conocer la ubicación física real del volumen.

## ¿Qué hace el origen?

En este ejemplo el origen es:

```text
auth_db_data
```

Como es un nombre y no una ruta, representa un **volumen administrado por Docker**. Debe declararse en el nivel superior del archivo Compose:

```yaml
volumes:
  auth_db_data:
```

Esta declaración le indica a Docker Compose que debe crear o reutilizar ese volumen.

Compose normalmente combina el nombre del proyecto y el nombre declarado. Si el proyecto se llama `infra`, el nombre real puede ser:

```text
infra_auth_db_data
```

Se puede comprobar con:

```bash
docker volume ls
```

## ¿Dónde se guardan físicamente los datos?

Docker elige y administra la ubicación del volumen.

En Linux suele corresponder a una ruta parecida a:

```text
/var/lib/docker/volumes/infra_auth_db_data/_data
```

En macOS, Docker Desktop ejecuta los contenedores dentro de una máquina virtual Linux. Por eso esa ruta se encuentra dentro de la máquina virtual y no como una carpeta normal visible en Finder.

Se puede consultar la información del volumen con:

```bash
docker volume inspect infra_auth_db_data
```

No se recomienda modificar directamente su `Mountpoint`. Los datos deben manipularse mediante PostgreSQL o el contenedor correspondiente.

## ¿Cómo ocurre la persistencia?

La primera vez que se ejecuta:

```bash
docker compose -f infra/docker-compose.yml up -d auth-postgres
```

Docker Compose realiza conceptualmente estos pasos:

1. Comprueba si existe `infra_auth_db_data`.
2. Si no existe, crea un volumen vacío. Si ya existe, conserva todo su contenido.
3. Crea un contenedor nuevo para `auth-postgres`.
4. Antes de iniciar PostgreSQL, monta el volumen en `/var/lib/postgresql/data`.
5. PostgreSQL inicia y revisa el contenido que ahora está visible en esa carpeta.

A partir del paso 5 existen dos casos.

### Caso A: el volumen es nuevo

```text
auth_db_data vacío
        │ se monta en
        ▼
/var/lib/postgresql/data vacío
        │
        ▼
PostgreSQL inicializa la base de datos
        │ escribe sus archivos
        ▼
auth_db_data queda con información persistente
```

PostgreSQL crea su estructura interna y la base inicial indicada por `POSTGRES_DB`. Todos esos archivos se escriben mediante la ruta destino y terminan almacenados en `auth_db_data`.

### Caso B: el volumen ya contiene información

```text
auth_db_data con tablas y registros anteriores
        │ se monta en
        ▼
/var/lib/postgresql/data muestra esos datos
        │
        ▼
PostgreSQL reconoce una base ya inicializada
        │
        ▼
Inicia utilizando las tablas y registros existentes
```

En este segundo caso PostgreSQL no vuelve a crear la base desde cero. La imagen detecta que `/var/lib/postgresql/data` ya contiene un clúster de PostgreSQL inicializado, omite la inicialización y abre los archivos existentes.

El volumen entra en acción exactamente en el montaje del paso 4. Desde ese momento, `/var/lib/postgresql/data` deja de representar solamente una carpeta temporal del contenedor y pasa a mostrar el contenido persistente de `auth_db_data`.

Si se elimina y vuelve a crear solamente el contenedor:

```text
Contenedor anterior eliminado
            │
            ├─ el volumen permanece
            │
Nuevo contenedor creado
            │ monta el mismo volumen
            ▼
PostgreSQL encuentra los datos anteriores
```

Por eso los datos sobreviven a reinicios y recreaciones del contenedor.

## `down` frente a `down -v`

Este comando elimina contenedores y la red, pero conserva los volúmenes:

```bash
docker compose -f infra/docker-compose.yml down
```

Al ejecutar nuevamente `up`, PostgreSQL utiliza los datos existentes.

Este comando también elimina los volúmenes declarados por el proyecto:

```bash
docker compose -f infra/docker-compose.yml down -v
```

En este proyecto puede eliminar las bases PostgreSQL, los datos de Kafka, Loki, Grafana, pgAdmin y el estado de Alloy. Debe utilizarse únicamente cuando se desea reiniciar toda la infraestructura desde cero.

## Volumen administrado frente a bind mount

Aunque ambos aparecen dentro de `volumes:`, no representan lo mismo.

### Volumen administrado por Docker

```yaml
- auth_db_data:/var/lib/postgresql/data
```

El origen es un nombre:

```text
auth_db_data
```

Docker decide dónde guardar los datos. Es apropiado para bases de datos y estado interno de herramientas.

### Bind mount

```yaml
- ../backend/order-service/logs:/var/log/z4/order-service:ro
```

El origen es una ruta del Mac:

```text
../backend/order-service/logs
```

Docker comparte esa carpeta existente con el contenedor:

```text
Mac                                       Contenedor
backend/order-service/logs       →        /var/log/z4/order-service
```

Ambas rutas muestran los mismos archivos. No se crea una copia adicional.

## Permisos `rw` y `ro`

Si no se especifica permiso, Docker utiliza `rw`:

```yaml
- auth_db_data:/var/lib/postgresql/data
```

Equivale a:

```yaml
- auth_db_data:/var/lib/postgresql/data:rw
```

`rw` significa lectura y escritura. PostgreSQL lo necesita para guardar o modificar información.

El permiso `ro` significa solo lectura:

```yaml
- ./observability/alloy/config.alloy:/etc/alloy/config.alloy:ro
```

Alloy puede leer el archivo, pero no modificarlo ni eliminarlo.

## ¿Qué sucede si el destino ya contiene archivos?

Cuando se monta un volumen en una carpeta interna, el contenido del montaje pasa a ser el contenido visible en esa ruta.

```text
Ruta original del contenedor
/var/lib/postgresql/data
            │
            ▼ se monta el volumen
Contenido visible = contenido de auth_db_data
```

Por eso se debe montar el volumen exactamente en la carpeta de datos recomendada por la imagen utilizada.

## Comandos útiles

Listar los volúmenes existentes:

```bash
docker volume ls
```

Inspeccionar un volumen:

```bash
docker volume inspect infra_auth_db_data
```

Consultar los montajes de un contenedor:

```bash
docker inspect infra-auth-postgres-1
```

Consultar los volúmenes utilizados por este Compose:

```bash
docker compose -f infra/docker-compose.yml config --volumes
```

Detener la infraestructura conservando sus datos:

```bash
docker compose -f infra/docker-compose.yml down
```

Eliminar también todos los datos persistentes del proyecto:

```bash
docker compose -f infra/docker-compose.yml down -v
```

## Resumen mental

Para leer esta línea:

```yaml
- auth_db_data:/var/lib/postgresql/data
```

se puede pensar:

```text
"Conecta el volumen persistente auth_db_data
 con la carpeta /var/lib/postgresql/data del contenedor
 y permite que PostgreSQL lea y escriba en él."
```

El contenedor utiliza el destino. Docker administra el origen. El montaje es el puente entre ambos.
