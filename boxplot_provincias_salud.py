#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 17 12:45:30 2026

@author: estebansant
"""

import seaborn as sns
import duckdb as dd
import matplotlib.pyplot as plt
import pandas as pd
import os

directorio_script = os.path.dirname(os.path.abspath(__file__))

# Cargo los archivos
archivo_prov = os.path.join(directorio_script, "Provincia.csv")
archivo_establecimiento = os.path.join(
    directorio_script, "Establecimiento.csv")
archivo_depto = os.path.join(directorio_script, "Departamento.csv")

provincias = pd.read_csv(archivo_prov)
establecimiento = pd.read_csv(archivo_establecimiento)

# %%

consultaSQL = """
                SELECT DISTINCT est.id_establecimiento, est.id_depto, est.id_prov, pro.nombre AS 'provincia'
                FROM establecimiento AS est
                INNER JOIN provincias AS pro
                ON est.id_prov = pro.id_prov
                ORDER BY est.id_prov, est.id_depto
            """
dataframeResultado = dd.query(consultaSQL).df()

consultaSQL = """
            SELECT provincia,id_prov, id_depto,
            COUNT(id_establecimiento) as cantidad_total
            FROM dataframeResultado
            GROUP BY provincia,id_prov,id_depto
            ORDER BY provincia, id_depto
             """

dataframeResultado = dd.query(consultaSQL).df()

# Solo para cambiarle el nombre a CABA y TDF porque son muy largos para el grafico final

consultaSQL = """
            SELECT 
            CASE WHEN provincia LIKE 'Ciudad Autónoma de Buenos Aires'
                THEN 'CABA'
                WHEN provincia LIKE 'Tierra del Fuego, Antártida e Islas del Atlántico Sur'
                THEN 'Tierra del Fuego'
                ELSE provincia
                END AS provincia,
                
            id_depto,
            cantidad_total
            FROM dataframeResultado
             """

dataframeResultado = dd.query(consultaSQL).df()
dataframeResultado.to_csv(os.path.join(
    directorio_script, "PRUEBA.csv"), index=False)
# %%

orden_provincias = dataframeResultado.groupby(
    "provincia")["cantidad_total"].median().sort_values(ascending=False).index

plt.figure(figsize=(10, 12))

# 3. Crear el Boxplot Horizontal
sns.boxplot(
    data=dataframeResultado,
    x="cantidad_total",
    y="provincia",
    order=orden_provincias,
    palette="viridis",
    hue="provincia",
    legend=False
)

plt.xlabel("Cantidad de Establecimientos")
plt.ylabel("Provincia")
plt.tight_layout()

plt.savefig(os.path.join(directorio_script,
            "distribucion_hospitales.png"), bbox_inches='tight', dpi=300)

plt.show()
