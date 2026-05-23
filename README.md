# 🚀 Pipeline de Big Data para Ventas Minoristas con PySpark y Docker

Proyecto integral de **Ingeniería de Datos** que simula un entorno de ventas minoristas utilizando **Apache Spark**, **Docker** y una **Arquitectura Medallion (Bronce → Plata → Oro)** para procesar un millón de registros de transacciones minoristas en un entorno distribuido.

El proyecto se centra en **conceptos de procesamiento de datos distribuidos**, **funciones internas de Spark** y **modelado de datos para análisis**, al tiempo que expone conjuntos de datos agregados y limpios que posteriormente pueden ser utilizados por herramientas de BI.

---

## 📌 Resumen del proyecto

Este proyecto demuestra cómo diseñar y ejecutar una canalización de datos moderna utilizando:

- **Apache Spark 3.5.1**
- **Clúster distribuido en contenedores Docker**
- **PySpark**
- **Arquitectura Medallion**
- **Formato de almacenamiento de parquet**
- **Power BI (solo demostración)**

El pipeline procesa datos sintéticos de venta minorista, aplica reglas de validación y limpieza, y produce conjuntos de datos analíticos seleccionados y optimizados para la elaboración de informes y análisis posteriores.

---

## Arquitectura

### Clúster Spark distribuido (modelo maestro-trabajador)

El entorno se implementa utilizando Docker y simula un clúster distribuido de Spark compuesto por:

- **1 Spark Master**
- **2 Spark Workers**

```text
                   ┌──────────────────┐
                   │   Spark Master   │
                   │   Port: 7077     │
                   │   UI: 8080       │
                   └────────┬─────────┘
                            │
             ┌──────────────┴──────────────┐
             │                             │
      ┌──────▼──────┐               ┌──────▼──────┐
      │ Spark Worker│               │ Spark Worker│
      │   Worker 1  │               │   Worker 2  │
      │   2 cores   │               │   2 cores   │
      │   2 GB RAM  │               │   2 GB RAM  │
      └─────────────┘               └─────────────┘
```

El **Spark Master** actúa como coordinador del clúster:

- Recibe trabajos
- Elabora planes de ejecución
- Programa tareas
- Distribuye las cargas de trabajo entre los trabajadores

Los trabajadores ejecutan tareas distribuidas en paralelo utilizando particiones del conjunto de datos.

---

## Arquitectura Medallón

El proceso sigue una **arquitectura Medallion multicapa**.

```text
Raw CSV
   │
   ▼
Bronze Layer
(raw ingestion)
   │
   ▼
Silver Layer
(cleaning & validation)
   │
   ▼
Gold Layer
(aggregated business-ready tables)
```

---

## Bronze Layer: ingesta de datos sin procesar

Objetivo:

> Ingiere transacciones minoristas sin procesar en Spark conservando la estructura original.

Responsabilidades principales:

- Aplicación del esquema
- Ingestión de CSV

Ejemplos de tareas:

- Definición de esquema explícito
- Ingesta de 1.000.000 de registros
- Conversión a almacenamiento en columnas (Parquet)

Producción:

```text
data/bronze/retail_sales_bronze/
```

---

## Silver Layer: Limpieza y estandarización de datos

Objetivo:

Mejorar la calidad de los datos y estandarizar la información transaccional.

Normas de limpieza implementadas:

### Eliminación de duplicados

Las transacciones duplicadas se eliminan mediante:

```python
dropDuplicates(["transaction_id"])
```

### Fechas de envío no válidas

Registros donde:

```text
ship_date < order_date
```

se corrigen a nulo.

### Validación de cantidad

Transacciones con:

```text
quantity <= 0
```

se eliminan.

### Estandarización del precio unitario

Los precios negativos se convierten en valores positivos.

### Validación de descuento

Descuentos no válidos:

```text
discount_pct < 0
discount_pct > 100
```

se reemplazan con valores nulos.

### Validación de la edad del cliente

Las edades atípicas se filtran.

### Estandarización de género

Valores como:

```text
Male, Female, M, F
```

se normalizan en:

```text
M / F
```

Producción:

```text
data/silver/retail_silver_clean/
```

---

## Gold Layer: tablas analíticas seleccionadas

Objetivo:

> Generar conjuntos de datos listos para el análisis y optimizados para su consumo.

Conjuntos de datos generados:

### Métricas de ventas diarias

Contiene:

- Ingresos totales
- Pedidos totales
- Valor promedio del pedido

Producción:

```text
gold/daily_sales_metrics/
```

---

### Rendimiento de la categoría de producto

Contiene:

- Ingresos por categoría
- Cantidad vendida
- Recuento de pedidos

Producción:

```text
gold/product_category_performance/
```

---

### Métricas de ingresos de la ciudad

Contiene:

- Ingresos por geografía
- Recuento de pedidos
- Valor promedio del pedido

Producción:

```text
gold/city_revenue_metrics/
```

---

# Conceptos de ingeniería de Spark demostrados

## 1. Procesamiento distribuido maestro-trabajador

Spark ejecuta cargas de trabajo mediante un modelo distribuido.

El nodo maestro:

- Coordina la ejecución
- Crea programación de tareas
- Asigna trabajos

Trabajadores:

- Ejecutar cálculos distribuidos
- Procesar particiones de forma independiente

Esto permite la escalabilidad horizontal y el procesamiento en paralelo.

---

## 2. Transformaciones vs. Acciones

Un concepto fundamental de Spark.

### Transformaciones (Lazy Evaluation)

Las transformaciones definen un cálculo sin ejecutarlo inmediatamente.

Ejemplos en este proyecto:

```python
withColumn()
filter()
groupBy()
agg()
dropDuplicates()
```

Spark almacena estas operaciones en un DAG (grafo acíclico dirigido).

Ejemplo:

```python
silver_df = silver_df.filter(col("quantity") > 0)
```

Aún no se ha producido ninguna ejecución.

---

### Acciones

Las acciones desencadenan la ejecución de cálculos.

Ejemplos:

```python
count()
show()
write.parquet()
```

Cuando se ejecuta una acción, Spark evalúa todo el linaje y elabora el plan de ejecución.

---

## 3. Plan lógico vs. Plan físico

Spark utiliza el **Catalyst Optimizer**.

### Plan lógico

Representa:

> *¿Qué debería pasar?*

Ejemplo:

```python
groupBy("city").agg(sum("total_amount"))
```

Spark primero crea una estrategia de ejecución abstracta.

---

### Plan físico

Representa:

> *cómo Spark ejecutará el trabajo*

El optimizador decide:

- Estrategia de partición
- Etapas de ejecución
- Comportamiento del shuffle (redistribuciòn de datos)

---

## 4. Lazy Evaluation

Spark pospone la ejecución hasta que se llama a una acción.

Beneficios:

- Optimización de consultas
- Coste de cálculo reducido
- Optimización de la etapa

Este proyecto aprovecha al máximo la ejecución diferida mediante transformaciones encadenadas.

---

## 5. Particionamiento de datos

Spark divide los datos en particiones.

En lugar de procesar:

```text
1,000,000 rows sequentially
```

Spark distribuye las particiones entre los nodos de trabajo.

Ejemplo de ejecución conceptual:

```text
Partition 1 → Worker 1
Partition 2 → Worker 2
Partition 3 → Worker 1
Partition 4 → Worker 2
```

Esto permite el paralelismo.

---

## 6. Operaciones de Shuffle

El proceso de redistribución (shuffle) se produce cuando Spark redistribuye los datos entre los nodos.

Operaciones tales como:

```python
groupBy()
agg()
```

accionan el Shuffle ya que es necesario que los datos que comparten el mismo valor de agrupaciòn o agregado se encuentren en la misma particiòn.

Ejemplo de este proyecto:

```python
silver_df.groupBy("product_category").agg(...)
```

Spark debe agrupar los registros que pertenecen a la misma clave antes de la agregación.

Shuffle es caro porque implica:

- E/S de disco
- Transferencia de red
- Serialización

Comprender los costos de la redistribución de datos es esencial para las cargas de trabajo escalables de Spark.

---

## 7. Almacenamiento columnar con parquet

El proyecto almacena los resultados utilizando:

```text
Apache Parquet
```

Beneficios:

- Compresión columnar
- Almacenamiento reducido
- Lecturas analíticas más rápidas
- Optimizado para cargas de trabajo de Spark

---

## Generación de datos sintéticos

El conjunto de datos se genera mediante programación utilizando Python.

Características:

- 1.000.000 de registros
- Transacciones aleatorias
- Valores no válidos introducidos intencionadamente
- Identificadores de transacción duplicados
- Nulos y anomalías

Esto permite crear escenarios realistas de calidad de datos.

Generador:

```text
generate_raw_data.py
```

## Panel de demostración

Se creó un panel de control sencillo en Power BI para validar los datos seleccionados resultantes.

Su propósito es únicamente demostrar el consumo posterior de los conjuntos de datos procesados.

![Panel de control](dashboard.png)

---

## Tech Stack

- Apache Spark 3.5.1
- PySpark
- Docker
- Python
- Apache Parquet
- Power BI

---

