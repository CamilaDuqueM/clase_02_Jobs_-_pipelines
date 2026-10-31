from pyspark.sql import functions as F
from pyspark.sql import SparkSession

#===================================
# Crear y usar el catálogo principal
#===================================

spark.sql("CREATE CATALOG IF NOT EXISTS airline_mantenimiento;")
spark.sql("USE CATALOG airline_mantenimiento;")

#=================================================
# Crear los esquemas de la arquitectura Medallion
#=================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS airline_mantenimiento.fuentes;")
spark.sql("CREATE SCHEMA IF NOT EXISTS airline_mantenimiento.bronze;")
spark.sql("CREATE SCHEMA IF NOT EXISTS airline_mantenimiento.silver;")
spark.sql("CREATE SCHEMA IF NOT EXISTS airline_mantenimiento.gold;")

#===========================
# Crear el volumen de datos 
#===========================

spark.sql("CREATE VOLUME IF NOT EXISTS airline_mantenimiento.fuentes.landing;")

#======================================
# Verificación de esuqemas y  volumenes
#======================================

spark.sql("SHOW CATALOGS").display()
spark.sql("SHOW SCHEMAS").display()
spark.sql("SHOW VOLUMES").display()

print("¡Entorno preparado con éxito! Catálogos, esquemas y volúmenes creados.")