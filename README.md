# Fintech Reconciliation Batch Engine

![Java](https://img.shields.io/badge/Java-25-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-4.0.7-6DB33F?style=for-the-badge&logo=springboot&logoColor=white)
![Spring Batch](https://img.shields.io/badge/Spring_Batch-6.0-6DB33F?style=for-the-badge&logo=spring&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB_Atlas-Cloud-47A248?style=for-the-badge&logo=mongodb&logoColor=white)
![H2](https://img.shields.io/badge/H2-SQL_Core-004088?style=for-the-badge&logo=databricks&logoColor=white)

Motor de reconciliación financiera de alto rendimiento construido con Spring Batch 6 y persistencia híbrida SQL/NoSQL. Procesa **5.000 transacciones en 20 segundos** resolviendo cinco cuellos de botella de producción reales, partiendo de un tiempo inicial de ~5 minutos.

---

## Tabla de Contenidos

1. [Problema de Negocio y Flujo de Datos](#1-problema-de-negocio-y-flujo-de-datos)
2. [Decisiones de Arquitectura de Alto Rendimiento](#2-decisiones-de-arquitectura-de-alto-rendimiento)
3. [Tabla Comparativa de Métricas](#3-tabla-comparativa-de-métricas)
4. [Principios de Diseño — Clean Architecture](#4-principios-de-diseño--clean-architecture)
5. [Configuración y Ejecución](#5-configuración-y-ejecución)

---

## 1. Problema de Negocio y Flujo de Datos

### ¿Qué es la reconciliación financiera?

En el sector financiero, cada entidad mantiene su propio registro interno de transacciones. La **reconciliación** es el proceso de cruzar el estado de cuenta externo (generalmente un archivo CSV provisto por un banco, procesador de pagos o proveedor) contra el libro mayor interno (la base de datos transaccional de la plataforma) para detectar y clasificar discrepancias:

- **Discrepancias de monto**: el importe registrado externamente difiere del interno.
- **Discrepancias de moneda**: la divisa no coincide entre sistemas.
- **Transacciones huérfanas en archivo**: existen en el CSV externo pero no tienen contraparte en la base SQL interna.
- **Transacciones huérfanas en base**: existen internamente pero no aparecen en el archivo externo.

El resultado de este proceso es la clasificación de cada transacción como `RECONCILED` (conciliada sin errores) o `ERROR` (con discrepancia que requiere revisión manual).

### Flujo de Datos del Sistema

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ARRANQUE DE LA APP                           │
│                                                                     │
│   DataInitializer                                                   │
│   ├── Puebla H2 (SQL) con ~4.750 TransactionEntity                  │
│   └── Genera transactions.csv con 5.000 filas                       │
│       └── ~250 filas con errores deliberados (5%)                   │
│           ├── Discrepancia de monto (+$10.00)                       │
│           ├── Transacción huérfana en CSV                           │
│           └── Transacción huérfana en H2                            │
└────────────────────────────┬────────────────────────────────────────┘
                             │ POST /api/v1/jobs/run-reconciliation
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   SPRING BATCH — reconciliationJob                  │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │               reconciliationStep (multihilo)                  │  │
│  │                                                               │  │
│  │  [beforeStep]                                                 │  │
│  │   └── SELECT * FROM platform_transactions → ConcurrentHashMap │  │
│  │                                                               │  │
│  │  FlatFileItemReader ──► TransactionProcessor ──► ItemWriter   │  │
│  │  (lee CSV por chunks)    (cruza vs. caché)     (Bulk Insert)  │  │
│  │                           ├── RECONCILED                      │  │
│  │                           └── ERROR → failedIds[]             │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  [afterJob]                                                         │
│   └── AuditService → ReconciliationReport → H2 (SQL)                │
└────────────────────────────┬────────────────────────────────────────┘
                             │ Resultados conciliados
                             ▼
               ┌─────────────────────────────┐
               │   MongoDB Atlas (Cloud)     │
               │   Collection: transactions  │
               │   ~5.000 documentos         │
               │   {status: RECONCILED       │
               │    o status: ERROR}         │
               └─────────────────────────────┘
```

### Persistencia Híbrida (Políglota)

El sistema utiliza dos motores de base de datos con roles bien diferenciados:

| Motor | Rol | Justificación |
|---|---|---|
| **PostgreSQL / MySQL** *(H2 en este proyecto)* | Datos transaccionales core y metadatos del batch | Necesita transacciones ACID, esquema fijo, joins y el repositorio de estado de Spring Batch |
| **MongoDB Atlas (NoSQL, nube)** | Documentos conciliados y reportes analíticos | Los resultados son documentos semi-estructurados que pueden variar por entidad bancaria; NoSQL permite escalar sin migraciones de esquema |

> **Nota sobre H2:** En este proyecto se utiliza H2 en modo archivo como base SQL por simplicidad y portabilidad: no requiere instalar ni configurar un servidor externo, lo que facilita clonar y ejecutar el proyecto directamente. En un entorno de producción real, H2 sería reemplazado por **PostgreSQL** (opción recomendada por su robustez en cargas concurrentes) o **MySQL**. El cambio es transparente para la aplicación: basta con reemplazar el driver y la URL de conexión en `application.properties`, ya que toda la capa de persistencia está abstraída mediante JPA/Hibernate.

---

## 2. Decisiones de Arquitectura de Alto Rendimiento

El proceso inicial tardaba cerca de **5 minutos** con fallos de hilos y timeouts. A continuación se detallan los cinco cuellos de botella identificados y las decisiones tomadas para eliminarlos.

---

### Cuello de Botella 1 — Consulta N+1 en SQL

**Problema:** La implementación naive del `ItemProcessor` realizaba una consulta a la base de datos por cada fila del CSV. Con 5.000 registros, eso implicaba 5.000 roundtrips a H2 que saturaban el pool de conexiones de HikariCP y serializaban el procesamiento multihilo.

```
Fila 1 del CSV → SELECT * FROM platform_transactions WHERE reference = 'TXN-001'  ← query
Fila 2 del CSV → SELECT * FROM platform_transactions WHERE reference = 'TXN-002'  ← query
...
Fila 5000 del CSV → SELECT ...                                                     ← query #5000
```

**Solución — Pre-carga con `StepExecutionListener.beforeStep()`:**

`TransactionProcessor` implementa `StepExecutionListener`. En `beforeStep()`, antes de que el lector procese la primera fila, se ejecuta **un único** `SELECT * FROM platform_transactions` y el resultado se indexa en un `ConcurrentHashMap<String, TransactionEntity>` en memoria RAM.

```java
@Override
public void beforeStep(@NonNull StepExecution stepExecution) {
    List<TransactionEntity> platformTransactions = platformTransactionRepository.findAll();
    for (TransactionEntity txn : platformTransactions) {
        dbTransactionsCache.put(txn.getTransactionReference(), txn);
    }
    // Desde aquí, cada lookup del processor cuesta O(1) sin tocar la BD
}
```

El `ConcurrentHashMap` garantiza la seguridad de acceso concurrente cuando los 5–10 hilos del pool leen el caché en paralelo sin condiciones de carrera.

**Resultado:** 5.000 queries SQL → **1 query SQL**.

---

### Cuello de Botella 2 — Ruido Operacional en Logging Masivo

**Contexto técnico:** Spring Boot configura Logback con un `AsyncAppender` por defecto. Esto significa que `log.info()` no escribe directamente en consola: deposita el mensaje en un buffer interno y retorna de inmediato, sin bloquear el hilo llamante. En la práctica, múltiples threads pueden loguear en el mismo milisegundo sin contención observable:

```
09:46:28.831  Batch-Thread-3  → Transaccion exitosa
09:46:28.831  Batch-Thread-4  → Transaccion exitosa   ← mismo milisegundo, sin bloqueo
09:46:28.832  Batch-Thread-2  → Transaccion exitosa
```

El logging asíncrono de Logback absorbe la carga sin impacto en el tiempo de ejecución del job.

**El problema real — Observabilidad y costo operacional:**

Aun sin impacto en tiempo de CPU, loguear una línea por cada transacción exitosa introduce un problema diferente en entornos de producción reales: **señal/ruido en el sistema de logging centralizado**.

En un pipeline financiero productivo, los logs se envían a plataformas como Datadog, ELK Stack o AWS CloudWatch. En esos contextos:

- **5.000 líneas por ejecución** a las 02:00 AM generan ingesta de datos innecesaria con costo directo en dinero.
- Un `log.info("Transaccion exitosa")` repetido 4.830 veces no aporta información accionable: si una transacción es exitosa, no hay nada que revisar ni alertar.
- Cuando sí ocurre un error real, queda sepultado entre miles de líneas de éxito, aumentando el tiempo de diagnóstico.

**Solución — Logging orientado a excepciones:**

Se eliminaron los logs de éxito dentro del loop masivo del processor, conservando únicamente los `log.warn()` para discrepancias reales.

```
Antes: ~5.000 líneas de log por ejecución (una por transacción)
Después:   ~170 líneas de log por ejecución (solo discrepancias reales)
```

La regla aplicada: **en un proceso batch de alto volumen, un log de éxito por registro no es observabilidad, es ruido**. Los sistemas de monitoreo deben alertar sobre lo que falla, no confirmar lo que funciona.

---

### Cuello de Botella 3 — Latencia de Red NoSQL e Inserciones Individuales

**Problema:** El `RepositoryItemWriter` de Spring Batch por defecto itera el chunk y llama a `.save()` de forma individual sobre cada registro. MongoDB Atlas está en la nube (no en localhost), por lo que cada `.save()` incurría en la latencia de red de internet completa. Con chunks de 50 registros y 10 hilos, la cola del executor se desbordaba y el driver de MongoDB arrojaba `TaskRejectedException`, congelando el proceso exactamente 30 segundos (el timeout por defecto del driver).

**Solución — Bulk Insert con `ItemWriter` lambda:**

Se reemplazó el writer estándar por una implementación funcional que empaqueta todos los registros del chunk en **un único viaje de red**:

```java
@Bean
public ItemWriter<TransactionDocument> writer() {
    return chunk -> transactionRepository.insert(chunk.getItems());
}
```

`MongoRepository.insert(Iterable<S>)` ejecuta un `BulkWriteOperation` a nivel de driver, enviando todos los documentos del chunk en una sola operación de red. Combinado con un `chunkSize=500`, el número total de roundtrips a MongoDB Atlas pasó de miles a **decenas**.

---

### Cuello de Botella 4 — Backpressure y Resiliencia del Pool de Hilos

**Problema:** El lector de archivos locales (CSV en disco) opera a velocidad de memoria, órdenes de magnitud más rápido que la escritura en MongoDB Atlas (limitada por la red). Esta asimetría de velocidades desbordaba la cola del `ThreadPoolTaskExecutor` con tareas pendientes de escritura, generando más `TaskRejectedException` y, en el peor caso, pérdida de datos al descartar tareas silenciosamente.

**Solución — `CallerRunsPolicy` como mecanismo de Backpressure nativo:**

```java
@Bean
public ThreadPoolTaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(5);
    executor.setMaxPoolSize(10);
    executor.setQueueCapacity(500);       // Buffer de 500 tareas pendientes
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    executor.setThreadNamePrefix("Batch-Thread-");
    executor.initialize();
    return executor;
}
```

Cuando la cola llega a su capacidad máxima (500 tareas), `CallerRunsPolicy` hace que el **hilo principal ejecute la tarea directamente** en lugar de encolarla. Esto tiene dos efectos:

1. El hilo principal queda ocupado procesando → **deja de leer nuevas filas del CSV** automáticamente.
2. Los worker threads terminan su trabajo (escribir en MongoDB) y liberan espacio en la cola.

El sistema se autoregula sin necesidad de mecanismos externos. El estado final siempre es `COMPLETED`, nunca `FAILED` por desbordamiento.

---

### Cuello de Botella 5 — Desbordamiento de Datos en Reportes (Truncation Error)

**Problema:** Al finalizar el job, el `AuditService` persiste en H2 un `ReconciliationReport` cuyo campo `summaryMessage` contiene la lista completa de los IDs con errores. Con ~170 referencias del formato `TXN-2026-XXXXX`, el string superaba los **2.000 caracteres**. El tipo `VARCHAR(255)` por defecto de JPA arrojaba un `DataException` de truncación, impidiendo guardar el reporte de auditoría.

**Solución:** Anotación `@Column(columnDefinition = "TEXT")` en la entidad:

```java
@Column(name = "summary_message", columnDefinition = "TEXT")
private String summaryMessage;
```

`TEXT` en H2 (equivalente a `CLOB`) almacena strings de longitud arbitraria. Los reportes de auditoría financiera son por naturaleza dinámicos: a mayor volumen de errores, mayor el mensaje. Hardcodear un límite numérico sería una deuda técnica garantizada.

**Nota sobre contadores de auditoría:** El cálculo de `successfulCount` y `failedCount` en `AuditServiceImpl` utiliza contadores atómicos propios (`AtomicLong`) mantenidos directamente en `TransactionProcessor` y expuestos vía `ExecutionContext`, en lugar de inferir los valores desde el `writeCount` de Spring Batch. Esta decisión desacopla la auditoría de los internos del framework: si en el futuro se agrega un `SkipPolicy`, el `writeCount` cambia de semántica mientras que los contadores propios siguen siendo la fuente de verdad exacta.

---

## 3. Tabla Comparativa de Métricas

| Métrica | Antes (Naive) | Después (Optimizado) |
|---|---|---|
| **Tiempo total de ejecución** | ~5 minutos | **20 segundos** |
| **Estado final del Job** | `FAILED` / `ABANDONED` | `COMPLETED` |
| **Queries a SQL (H2)** | 5.000 (una por fila) | **1** (carga masiva en `beforeStep`) |
| **Viajes de red a MongoDB** | ~5.000 (un `.save()` por registro) | **~10** (Bulk Insert por chunk de 500) |
| **Líneas de log generadas** | ~5.000 | **~170** (solo discrepancias reales) |
| **Excepciones de hilos** | `TaskRejectedException` frecuentes | **Ninguna** (Backpressure activo) |
| **Timeouts de MongoDB** | Congelamiento de 30s por bloque | **Eliminados** |
| **Integridad del reporte final** | `DataException` (truncación VARCHAR) | **Persistido íntegro** (`TEXT`) |
| **Chunk Size** | 50 (default) | **500** (óptimo para red WAN) |
| **Pool de hilos** | Sin política de rechazo | `CallerRunsPolicy` (Backpressure) |

---

## 4. Principios de Diseño — Clean Architecture

### Separación de Responsabilidades: `TransactionFieldSetMapper`

La lógica de mapeo entre las columnas del CSV y el modelo de dominio está completamente encapsulada en `TransactionFieldSetMapper`, desacoplada del `TransactionProcessor`:

```java
@Component
public class TransactionFieldSetMapper implements FieldSetMapper<TransactionDocument> {
    @Override
    public TransactionDocument mapFieldSet(FieldSet fieldSet) {
        return TransactionDocument.builder()
                .transactionReference(fieldSet.readString("transactionReference"))
                .amount(fieldSet.readBigDecimal("amount"))
                .currency(CurrencyType.valueOf(fieldSet.readString("currency").toUpperCase()))
                // ...
                .build();
    }
}
```

Esta decisión hace al sistema **Multi-Entidad ready**: para procesar archivos con formato diferente (por ejemplo, un CSV del Banco A con columnas `tx_id,value,iso_currency` vs. el Banco B con `ref,amount,currency_code`), basta con:

1. Crear un nuevo `XxxFieldSetMapper` que implemente `FieldSetMapper<TransactionDocument>`.
2. Inyectarlo en el `FlatFileItemReaderBuilder` de ese job específico.

El `TransactionProcessor` —donde vive la lógica de negocio de reconciliación— **no necesita ninguna modificación**. El contrato entre el mapper y el processor es el modelo `TransactionDocument`, y ese contrato no cambia.

### Idempotencia y Control de Concurrencia: `JobExecutionRegistry`

```java
@Component
public class JobExecutionRegistry {
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public boolean tryLock() { return isRunning.compareAndSet(false, true); }
    public void release()    { isRunning.set(false); }
}
```

La operación `compareAndSet` es atómica a nivel de JVM. Esto garantiza que aunque dos requests HTTP lleguen simultáneamente al endpoint `/run-reconciliation`, solo uno adquirirá el lock. El segundo recibirá un `409 CONFLICT` inmediato. El `release()` se ejecuta siempre en el bloque `finally` del `JobCompletionNotificationListener`, incluso ante fallos, previniendo deadlocks permanentes.

### Seguridad de Credenciales — 12-Factor App

Ninguna credencial está hardcodeada en el código fuente. La URI de MongoDB se construye en runtime inyectando variables de entorno:

```properties
# application.properties
spring.mongodb.uri=mongodb+srv://${MONGO_USER}:${MONGO_PASSWORD}@cluster0.../${MONGO_DATABASE}
```

El repositorio es seguro para hacer público sin riesgo de exponer credenciales de producción.

---

## 5. Configuración y Ejecución

### Prerrequisitos

- Java 25
- Maven 3.9+
- Cuenta en MongoDB Atlas con un cluster activo y un usuario de base de datos creado
- **No se requiere instalar ningún servidor SQL**: este proyecto usa H2 en modo archivo, que se crea automáticamente en `./data/fintech_db` al iniciar la aplicación.

#### Migración a PostgreSQL o MySQL (producción)

Para apuntar a una base SQL real, reemplazar en `application.properties`:

```properties
# PostgreSQL
spring.datasource.url=jdbc:postgresql://localhost:5432/fintech_db
spring.datasource.username=${DB_USER}
spring.datasource.password=${DB_PASSWORD}
spring.datasource.driver-class-name=org.postgresql.Driver

# MySQL
# spring.datasource.url=jdbc:mysql://localhost:3306/fintech_db
# spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
```

Y agregar la dependencia correspondiente en `pom.xml`. El resto de la aplicación no requiere ningún cambio gracias a la abstracción de JPA/Hibernate.

### Variables de Entorno

Antes de iniciar la aplicación, definir las siguientes variables en el entorno de ejecución:

```bash
export MONGO_USER=tu_usuario_mongo
export MONGO_PASSWORD=tu_password_mongo
export MONGO_DATABASE=fintech_reconciliation
```

En Windows (PowerShell):

```powershell
$env:MONGO_USER="tu_usuario_mongo"
$env:MONGO_PASSWORD="tu_password_mongo"
$env:MONGO_DATABASE="fintech_reconciliation"
```

### Compilar y Ejecutar

```bash
# Clonar el repositorio
git clone https://github.com/tu-usuario/fintech-reconciliation-batch.git
cd fintech-reconciliation-batch

# Compilar
mvn clean package -DskipTests

# Ejecutar con las variables de entorno ya definidas en el shell
java -jar target/fintech-reconciliation-batch-0.0.1-SNAPSHOT.jar
```

Al arrancar, el `DataInitializer` generará automáticamente:
- La base H2 en `./data/fintech_db` con ~4.750 transacciones SQL.
- El archivo `transactions.csv` en el directorio raíz con 5.000 filas (incluyendo ~250 con errores deliberados).

### Disparar la Reconciliación Manualmente

```bash
curl -X POST http://localhost:8080/api/v1/jobs/run-reconciliation \
  -H "Content-Type: application/json"
```

Respuesta esperada (`202 Accepted`):

```json
{
  "status": "PROCESSING",
  "message": "El Job de conciliación fue encolado y comenzó su ejecución en segundo plano.",
  "jobExecutionId": 1,
  "batchStatus": "STARTED"
}
```

Si el job ya está en ejecución (`409 Conflict`):

```json
{
  "status": "JOB_ERROR",
  "message": "El proceso de conciliación ya está en ejecución.",
  "details": "No cause details",
  "timestamp": "2026-01-15T02:00:05.123"
}
```

### Consultar Resultados

**H2 Console** (reporte de auditoría SQL):
```
URL:      http://localhost:8080/h2-console
JDBC URL: jdbc:h2:file:./data/fintech_db
Usuario:  sa
Password: password
```

```sql
SELECT * FROM RECONCILIATION_REPORT ORDER BY EXECUTION_DATE DESC;
```

**MongoDB Atlas** (documentos conciliados):

En MongoDB Compass o Atlas UI, conectarse al cluster y consultar la colección `transactions`:

```javascript
// Ver todos los errores
db.transactions.find({ status: "ERROR" })

// Resumen por estado
db.transactions.aggregate([
  { $group: { _id: "$status", total: { $sum: 1 } } }
])
```

### Ejecución Programada (CRON)

El job también se ejecuta automáticamente todos los días a las 02:00 AM. Para pruebas locales, descomentar en `application.properties`:

```properties
# Ejecución cada 10 segundos (solo para testing)
app.batch.reconciliation.cron=*/10 * * * * ?
```

### Health Check

```bash
curl http://localhost:8080/actuator/health
```
