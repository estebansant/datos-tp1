#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 15 15:21:41 2026

@author: estebansant
"""

import pandas as pd
import duckdb as dd
import os

directorio_script = os.path.dirname(os.path.abspath(__file__))
archivo = os.path.join(directorio_script, "instituciones_de_salud.xlsx")

df = pd.read_excel(archivo)

df.columns = ['id_establecimiento', 'nombre', 'localidad_id', 'localidad_nombre',
                    'id_prov', 'nombre_prov', 'id_depto', 'nombre_depto', 'codloc',
                    'codent', 'tipo_financiamiento', 'tipologia_id', 'tipologia_sigla',
                    'tipologia_nombre', 'cp', 'domicilio', 'sitio_web']

columnas_validas = ['id_establecimiento', 'nombre',
                    'id_depto', 'id_prov', 'tipo_financiamiento',
                    'tipologia_nombre']

df_valido = df[columnas_validas].copy()

# %% PRIMERAS CONSULTAS
# Cambio la columna tipologia_nombre por tiene_uai (UNIDAD DE INTERNACION) determinado por el sistema REFES
# El valor de dicha columna es 1 si tiene, y 0 en caso contrario.

consultaSQL = """
SELECT id_establecimiento, nombre, id_depto, id_prov, tipo_financiamiento,
    CASE 
        WHEN UPPER(tipologia_nombre)
        LIKE '%INTERNACIÓN%'
        OR UPPER(tipologia_nombre) 
        LIKE '%TERAPIA%'
        THEN 1
        ELSE 0
    END AS tiene_uai
FROM df_valido

"""

df_valido = dd.query(consultaSQL).df()

# %% Origen_financiamiento
# Cambio los valores de origen_financiamiento por público y Privado

consultaSQL = """
SELECT id_establecimiento, nombre, id_depto, id_prov, tiene_uai,
    CASE 
        WHEN UPPER(tipo_financiamiento) IN
        ('PRIVADO', 'MUTUAL', 'MIXTA','UNIVERSITARIO PRIVADO', 'OBRA SOCIAL')
        THEN 'Privado'
        ELSE 'Público'
    END AS tipo_financiamiento
FROM df_valido

"""

df_valido = dd.query(consultaSQL).df()

df_valido.to_csv("establecimiento.csv", index=False)
