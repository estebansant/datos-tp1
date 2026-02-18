#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 16 22:28:56 2026

@author: estebansant
"""

import pandas as pd
import os
import duckdb as dd

directorio_script = os.path.dirname(os.path.abspath(__file__))

# Cargo los archivos
archivo_prov = os.path.join(directorio_script, "Provincia.csv")
archivo_poblacion = os.path.join(directorio_script, "Poblacion.csv")

provincias = pd.read_csv(archivo_prov)
poblacion = pd.read_csv(archivo_poblacion)

#%%

# Lo que hago es una unica consulta SQL para renombrar los rangos etarios a un valor mas comodo y entendible, sumo por cada caso y agrupo en columnas nuevas
consultaSQL = """
            SELECT pr.nombre AS Provincia,
            
            CASE WHEN p.rango_etario LIKE '01%'
                THEN '0 a 14'
            WHEN p.rango_etario LIKE '02%'
                THEN '15 a 34'
            WHEN p.rango_etario LIKE '03%' 
                THEN '35 a 54'
            WHEN p.rango_etario LIKE '04%' 
                THEN '55 a 74'
            WHEN p.rango_etario LIKE '05%'
                THEN '75 y más'
            ELSE p.rango_etario
            
            END AS "Grupo etario",

            SUM(CASE WHEN p.anio = 2010 AND p.tiene_cobertura = 1 
                THEN p.cantidad
                ELSE 0 
            END) AS "Habitantes con cobertura en 2010",

            SUM(CASE WHEN p.anio = 2010 AND p.tiene_cobertura = 0 
                THEN p.cantidad
                ELSE 0 
            END) AS "Habitantes sin cobertura en 2010",

            SUM(CASE WHEN p.anio = 2022 AND p.tiene_cobertura = 1 
                THEN p.cantidad
                ELSE 0 
            END) AS "Habitantes con cobertura en 2022",

            SUM(CASE WHEN p.anio = 2022 AND p.tiene_cobertura = 0 
                THEN p.cantidad
                ELSE 0 
            END) AS "Habitantes sin cobertura en 2022"

            FROM poblacion AS p

            INNER JOIN provincias AS pr
            ON p.id_prov = pr.id_prov
            GROUP BY pr.nombre, "Grupo etario"
            ORDER BY pr.nombre, "Grupo etario";
"""


dataframeResultado = dd.query(consultaSQL).df()

dataframeResultado.to_csv(os.path.join(directorio_script, "CoberturaDeSalud.csv"), index=False)