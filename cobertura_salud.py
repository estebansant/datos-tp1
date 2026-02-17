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

consultaSQL_provId = """
                    SELECT * 
                    FROM poblacion
                    NATURAL JOIN provincias;
                    """
                    
dataframeResultado = dd.query(consultaSQL_provId).df()
print(dataframeResultado)