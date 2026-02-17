#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 16 23:54:22 2026

@author: estebansant
"""


import pandas as pd
import os
import duckdb as dd

directorio_script = os.path.dirname(os.path.abspath(__file__))

# Cargo los archivos
archivo_prov = os.path.join(directorio_script, "Provincia.csv")
archivo_establecimiento = os.path.join(directorio_script, "Establecimiento.csv")

provincias = pd.read_csv(archivo_prov)
establecimiento = pd.read_csv(archivo_establecimiento)

#%%

consultaSQL = """
                SELECT pr.nombre AS Provincia,
                
                SUM(CASE WHEN tiene_uai=1 AND tipo_financiamiento='Público'
                    THEN 1 
                    ELSE 0
                END) AS 'Establecimientos de Salud con Terapia Intensiva y financiamiento Estatal',
                
                SUM(CASE WHEN tiene_uai=1 AND tipo_financiamiento='Privado'
                    THEN 1 
                    ELSE 0
                END) AS 'Establecimientos de Salud con Terapia Intensiva y financiamiento Privado',
                
                SUM(tiene_uai) AS 'Todos los Establecimientos de Salud con Terapia Intensiva'
                
                FROM establecimiento AS es
                
                INNER JOIN provincias AS pr
                ON es.id_prov = pr.id_prov
                GROUP BY pr.nombre
                ORDER BY pr.nombre;
            """
            
dataframeResultado = dd.query(consultaSQL).df()
dataframeResultado.to_csv(os.path.join(directorio_script, "EstablecimientosTerapiaIntensiva.csv"), index=False)