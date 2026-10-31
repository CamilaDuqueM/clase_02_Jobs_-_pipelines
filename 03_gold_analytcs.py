%pip install seaborn

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import seaborn as sns

# =========================
# Configuración
# =========================
CATALOG = "airline_mantenimiento"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# =========================
# 1. Leer Silver
# =========================
vuelos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.vuelos")

# =========================
# 1. Enruquecer tabla de  vuelos
# =========================

print("1. Creando 'vuelos_enriquecidos'...")
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.vuelos_enriquecidos
USING DELTA AS
SELECT 
    v.vuelo_id, 
    v.fecha, 
    CAST(date_trunc('MONTH', v.fecha) AS DATE) AS mes, 
    v.aeronave_id, 
    a.modelo, 
    a.fabricante, 
    a.anio_fabricacion, 
    YEAR(v.fecha) - a.anio_fabricacion AS edad_aeronave, 
    v.origen_id, 
    o.nombre AS origen_nombre, 
    o.ciudad AS origen_ciudad, 
    o.pais AS origen_pais, 
    v.destino_id, 
    d.nombre AS destino_nombre, 
    d.ciudad AS destino_ciudad, 
    d.pais AS destino_pais, 
    v.estado, 
    v.duracion_min, 
    v.estado <> 'cancelado' AS operado
FROM {CATALOG}.{SILVER_SCHEMA}.vuelos v 
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.aeronaves a ON v.aeronave_id = a.aeronave_id 
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.aeropuertos o ON v.origen_id = o.aeropuerto_id 
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.aeropuertos d ON v.destino_id = d.aeropuerto_id;
""")

# -------------------------------------------------------------------------
# 3. Enriquecer Mantenimientos
# -------------------------------------------------------------------------
print("2. Creando 'mantenimientos_enriquecidos'...")
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.mantenimientos_enriquecidos
USING DELTA AS
SELECT 
    m.*, 
    CAST(date_trunc('MONTH', m.fecha) AS DATE) AS mes,
    a.modelo, 
    a.fabricante, 
    a.anio_fabricacion,
    YEAR(m.fecha) - a.anio_fabricacion AS edad_aeronave_en_mantenimiento
FROM {CATALOG}.{SILVER_SCHEMA}.mantenimientos m
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.aeronaves a ON m.aeronave_id = a.aeronave_id;
""")

# -------------------------------------------------------------------------
# 4. KPI 1: Puntualidad por grupo (Genera datos para Gráfico por País)
# -------------------------------------------------------------------------
print("3. Creando KPI 'kpi_puntualidad_vuelos'...")
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.kpi_puntualidad_vuelos
USING DELTA AS
SELECT 
    modelo,
    fabricante,
    origen_pais,
    mes,
    COUNT(*) AS total_vuelos,
    SUM(CASE WHEN operado THEN 1 ELSE 0 END) AS vuelos_operados,
    SUM(CASE WHEN estado = 'a_tiempo' THEN 1 ELSE 0 END) AS vuelos_a_tiempo,
    SUM(CASE WHEN estado = 'retrasado' THEN 1 ELSE 0 END) AS vuelos_retrasados,
    SUM(CASE WHEN estado = 'cancelado' THEN 1 ELSE 0 END) AS vuelos_cancelados,
    ROUND(100.0 * SUM(CASE WHEN estado = 'a_tiempo' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN operado THEN 1 ELSE 0 END), 0), 2) AS puntualidad_operados_pct,
    ROUND(100.0 * SUM(CASE WHEN estado = 'cancelado' THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancelacion_pct,
    ROUND(AVG(CASE WHEN operado THEN duracion_min END), 2) AS duracion_promedio_operados_min
FROM {CATALOG}.{GOLD_SCHEMA}.vuelos_enriquecidos
GROUP BY modelo, fabricante, origen_pais, mes;
""")

# Muestra el resultado (Databricks creará la pestaña para la gráfica de barras)
display(spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.kpi_puntualidad_vuelos ORDER BY modelo, origen_pais"))

# Configuración estética básica
sns.set_theme(style="whitegrid")

df_vuelos_pais = spark.sql(f"""
    SELECT 
        origen_pais, 
        SUM(total_vuelos) AS total_vuelos
    FROM {CATALOG}.{GOLD_SCHEMA}.kpi_puntualidad_vuelos
    GROUP BY origen_pais
    ORDER BY total_vuelos DESC
""").toPandas()

plt.figure(figsize=(8, 5))
sns.barplot(data=df_vuelos_pais, x="origen_pais", y="total_vuelos", palette="Blues_r")
plt.title("Total de Vuelos por País de Origen")
plt.xlabel("País de Origen")
plt.ylabel("Total de Vuelos")
plt.xticks(rotation=0)
plt.tight_layout()

# Muestra la gráfica directamente en la celda del notebook
display(plt.gcf())
plt.clf() # Limpia la figura para evitar superposiciones

# -------------------------------------------------------------------------
# 5. KPI 2: Puntualidad Global
# -------------------------------------------------------------------------
print("4. Calculando Puntualidad Global...")
display(spark.sql(f"""
SELECT 
    SUM(total_vuelos) AS vuelos,
    SUM(vuelos_operados) AS operados,
    SUM(vuelos_a_tiempo) AS a_tiempo,
    SUM(vuelos_cancelados) AS cancelados,
    ROUND(100.0 * SUM(vuelos_a_tiempo) / NULLIF(SUM(vuelos_operados), 0), 2) AS puntualidad_pct,
    ROUND(100.0 * SUM(vuelos_cancelados) / SUM(total_vuelos), 2) AS cancelacion_pct
FROM {CATALOG}.{GOLD_SCHEMA}.kpi_puntualidad_vuelos
"""))

# -------------------------------------------------------------------------
# 6. KPI 3: Costos de Mantenimiento (Genera datos para Gráfico por Tipo)
# -------------------------------------------------------------------------
print("5. Creando KPI 'kpi_costo_mantenimiento'...")
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.kpi_costo_mantenimiento
USING DELTA AS
SELECT 
    aeronave_id,
    modelo,
    fabricante,
    tipo,
    mes,
    COUNT(*) AS total_mantenimientos,
    SUM(costo_usd) AS costo_total_usd,
    ROUND(AVG(costo_usd), 2) AS costo_promedio_usd,
    SUM(duracion_hr) AS duracion_total_hr,
    ROUND(AVG(duracion_hr), 2) AS duracion_promedio_hr
FROM {CATALOG}.{GOLD_SCHEMA}.mantenimientos_enriquecidos
GROUP BY aeronave_id, modelo, fabricante, tipo, mes;
""")

# Muestra el resultado por tipo de mantenimiento
display(spark.sql(f"""
SELECT 
    tipo,
    SUM(total_mantenimientos) AS intervenciones,
    SUM(costo_total_usd) AS costo_total_usd
FROM {CATALOG}.{GOLD_SCHEMA}.kpi_costo_mantenimiento
GROUP BY tipo
ORDER BY costo_total_usd DESC
"""))

# -------------------------------------------------------------------------
# Gráfica 2: Costo Total por Tipo de Mantenimiento (kpi_costo_mantenimiento)
# -------------------------------------------------------------------------
df_costos_tipo = spark.sql(f"""
    SELECT 
        tipo AS tipo_mantenimiento, 
        SUM(costo_total_usd) AS costo_total_usd
    FROM {CATALOG}.{GOLD_SCHEMA}.kpi_costo_mantenimiento
    GROUP BY tipo
    ORDER BY costo_total_usd DESC
""").toPandas()

plt.figure(figsize=(8, 5))
sns.barplot(data=df_costos_tipo, x="tipo_mantenimiento", y="costo_total_usd", palette="Blues_r")
plt.title("Costo Total por Tipo de Mantenimiento (USD)")
plt.xlabel("Tipo de Mantenimiento")
plt.ylabel("Costo Total (USD)")
plt.xticks(rotation=0)
plt.tight_layout()

# Muestra la gráfica directamente en la celda del notebook
display(plt.gcf())
plt.clf()
# -------------------------------------------------------------------------
# 7. Resumen unificado por aeronave y mes
# -------------------------------------------------------------------------
print("6. Creando 'resumen_aeronave_mes'...")
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.resumen_aeronave_mes
USING DELTA AS
WITH actividad AS (
    SELECT 
        aeronave_id,
        mes,
        COUNT(*) AS total_vuelos,
        SUM(CASE WHEN operado THEN 1 ELSE 0 END) AS vuelos_operados
    FROM {CATALOG}.{GOLD_SCHEMA}.vuelos_enriquecidos
    GROUP BY aeronave_id, mes
),
mantenimiento AS (
    SELECT 
        aeronave_id,
        mes,
        COUNT(*) AS total_mantenimientos,
        SUM(costo_usd) AS costo_total_usd
    FROM {CATALOG}.{GOLD_SCHEMA}.mantenimientos_enriquecidos
    GROUP BY aeronave_id, mes
)
SELECT 
    COALESCE(v.aeronave_id, m.aeronave_id) AS aeronave_id,
    COALESCE(v.mes, m.mes) AS mes,
    COALESCE(v.total_vuelos, 0) AS total_vuelos,
    COALESCE(v.vuelos_operados, 0) AS vuelos_operados,
    COALESCE(m.total_mantenimientos, 0) AS total_mantenimientos,
    COALESCE(m.costo_total_usd, 0) AS costo_total_usd
FROM actividad v
FULL OUTER JOIN mantenimiento m
    ON v.aeronave_id = m.aeronave_id AND v.mes = m.mes;
""")
# Muestra la tabla de resumen de aeronaves por mes
display(spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.resumen_aeronave_mes ORDER BY aeronave_id, mes"))


print("--- ¡Capa Gold completada exitosamente! ---")