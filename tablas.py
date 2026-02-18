# -*- coding: utf-8 -*-
"""
Created on Mon Feb 16 18:46:51 2026

@author: ACER
"""

import pandas as pd
import numpy as np 
import duckdb as dd
import os

directorio_script = os.path.dirname(os.path.abspath(__file__))

archivo_prov = os.path.join(directorio_script, "TablasModelo/Provincia.csv")
archivo_def= os.path.join(directorio_script, "TablasModelo/Defuncion.csv")
archivo_depto= os.path.join(directorio_script, "TablasModelo/Departamento.csv")
archivo_est= os.path.join(directorio_script, "TablasModelo/Establecimiento.csv")
archivo_pob= os.path.join(directorio_script, "TablasModelo/Poblacion.csv")

defunciones = pd.read_csv(archivo_def) 
departamento = pd.read_csv(archivo_depto) 
establecimiento = pd.read_csv(archivo_est) 
poblacion = pd.read_csv(archivo_pob) 
provincia      = pd.read_csv(archivo_prov)



#%%
# Frecuencia definitiva
consulta4 = """SELECT categoria, rango_etario, sexo, sum(cantidad) As Cantidad_total
FROM defunciones
GROUP BY rango_etario, categoria, sexo
ORDER BY sexo, rango_etario;
"""


dataframeResultado1 = dd.query(consulta4).df()


consulta14f_menos = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '01.De a 0  a 14 anios' AND sexo = 'femenino' 
                ORDER BY Cantidad_total ASC
                LIMIT 5;
                
"""

consulta15f_menos = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '02.De 15 a 34 anios' AND sexo = 'femenino' 
                ORDER BY Cantidad_total ASC
                LIMIT 5;
                
"""


consulta35f_menos = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '03.De 35 a 54 anios' AND sexo = 'femenino' 
                ORDER BY Cantidad_total ASC
                LIMIT 5;
                
"""


consulta55f_menos = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '04.De 55 a 74 anios' AND sexo = 'femenino' 
                ORDER BY Cantidad_total ASC
                LIMIT 5;
                
"""

consulta75f_menos = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '05.De 75 anios y mas' AND sexo = 'femenino' 
                ORDER BY Cantidad_total ASC
                LIMIT 5;
                
"""

consulta14f_mas = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '01.De a 0  a 14 anios' AND sexo = 'femenino' 
                ORDER BY Cantidad_total DESC
                LIMIT 5;
                
"""

consulta15f_mas = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '02.De 15 a 34 anios' AND sexo = 'femenino' 
                ORDER BY Cantidad_total DESC
                LIMIT 5;
                
"""

consulta35f_mas = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '03.De 35 a 54 anios' AND sexo = 'femenino' 
                ORDER BY Cantidad_total DESC
                LIMIT 5;
                
"""



consulta55f_mas = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '04.De 55 a 74 anios' AND sexo = 'femenino' 
                ORDER BY Cantidad_total DESC
                LIMIT 5;
                
"""

consulta75f_mas = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '05.De 75 anios y mas' AND sexo = 'femenino' 
                ORDER BY Cantidad_total DESC
                LIMIT 5;
                
"""


consulta14M_menos = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '01.De a 0  a 14 anios' AND sexo = 'masculino' 
                ORDER BY Cantidad_total ASC
                LIMIT 5;
                
"""

consulta15M_menos = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '02.De 15 a 34 anios' AND sexo = 'masculino' 
                ORDER BY Cantidad_total ASC
                LIMIT 5;
                
"""

consulta35M_menos = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '03.De 35 a 54 anios' AND sexo = 'masculino' 
                ORDER BY Cantidad_total ASC
                LIMIT 5;
                
"""

consulta55M_menos = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '04.De 55 a 74 anios' AND sexo = 'masculino' 
                ORDER BY Cantidad_total ASC
                LIMIT 5;
                
"""

consulta75M_menos = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '05.De 75 anios y mas' AND sexo = 'masculino' 
                ORDER BY Cantidad_total ASC
                LIMIT 5;
                
"""


consulta14M_mas = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '01.De a 0  a 14 anios' AND sexo = 'masculino' 
                ORDER BY Cantidad_total DESC
                LIMIT 5;
                
"""

consulta15M_mas = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '02.De 15 a 34 anios' AND sexo = 'masculino' 
                ORDER BY Cantidad_total DESC
                LIMIT 5;
                
"""

consulta35M_mas = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '03.De 35 a 54 anios' AND sexo = 'masculino' 
                ORDER BY Cantidad_total DESC
                LIMIT 5;
                
"""

consulta55M_mas = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '04.De 55 a 74 anios' AND sexo = 'masculino' 
                ORDER BY Cantidad_total DESC
                LIMIT 5;
                
"""


consulta75M_mas = """SELECT d.categoria, d.rango_etario, d.sexo, d.Cantidad_total
                from dataframeResultado1 As d
                Where rango_etario = '05.De 75 anios y mas' AND sexo = 'masculino' 
                ORDER BY Cantidad_total DESC
                LIMIT 5;
                
"""





dataframeResultado14femeninosmenos = dd.query(consulta14f_menos).df()

dataframeResultado15femeninosmenos = dd.query(consulta15f_menos).df()

dataframeResultado35femeninosmenos = dd.query(consulta35f_menos).df()

dataframeResultado55femeninosmenos = dd.query(consulta55f_menos).df()

dataframeResultado75femeninosmenos = dd.query(consulta75f_menos).df()


dataframeResultado14femeninosmas = dd.query(consulta14f_mas).df()

dataframeResultado15femeninosmas = dd.query(consulta15f_mas).df()

dataframeResultado35femeninosmas = dd.query(consulta35f_mas).df()

dataframeResultado55femeninosmas = dd.query(consulta55f_mas).df()

dataframeResultado75femeninosmas = dd.query(consulta75f_mas).df()


dataframeResultado14masculinomenos = dd.query(consulta14M_menos).df()

dataframeResultado15masculinomenos = dd.query(consulta15M_menos).df()

dataframeResultado35masculinomenos = dd.query(consulta35M_menos).df()

dataframeResultado55masculinomenos = dd.query(consulta55M_menos).df()

dataframeResultado75masculinomenos = dd.query(consulta75M_menos).df()



dataframeResultado14masculinomas = dd.query(consulta14M_mas).df()

dataframeResultado15masculinomas = dd.query(consulta15M_mas).df()

dataframeResultado35masculinomas = dd.query(consulta35M_mas).df()

dataframeResultado55masculinomas = dd.query(consulta55M_mas).df()

dataframeResultado75masculinomas = dd.query(consulta75M_mas).df()
#%%
# Tasa de mortalidad por provincia


consulta = """ SELECT 
    id_prov,
    rango_etario,
    anio,
    SUM(cantidad) AS total_defunciones
FROM defunciones
WHERE anio = 2022
GROUP BY id_prov, rango_etario, anio
ORDER BY id_prov, rango_etario;
                """
#

consulta2 = """ SELECT 
    id_prov,
    rango_etario,
    anio,
    SUM(cantidad) AS total_defunciones
FROM poblacion
WHERE anio = 2022
GROUP BY id_prov, rango_etario, anio
ORDER BY id_prov, rango_etario;
                """
#
dataframeResultado_mortalidad = dd.query(consulta).df() 



dataframeResultado_mortalidad_provincia = dd.query(consulta2).df() 

consulta3 = """ 
SELECT 
    pr.nombre AS provincia,
    d.rango_etario, d.total_defunciones * 1000.0 / p.total_defunciones AS tasa_mortalidad
FROM dataframeResultado_mortalidad As d
JOIN dataframeResultado_mortalidad_provincia As p
    ON p.id_prov = d.id_prov
    AND p.rango_etario = d.rango_etario
JOIN provincia pr
    ON pr.id_prov = d.id_prov
ORDER BY pr.nombre, d.rango_etario;
"""


dataframeResultado_mortalidad_final = dd.query(consulta3).df()
#%%
# Cambios en las causas de defunción


consulta2 = """SELECT 
    categoria,
    SUM(CASE WHEN anio = 2022 THEN cantidad ELSE 0 END) AS total_2022,
    SUM(CASE WHEN anio = 2010 THEN cantidad ELSE 0 END) AS total_2010,
    SUM(CASE WHEN anio = 2022 THEN cantidad ELSE 0 END) - SUM(CASE WHEN anio = 2010 THEN cantidad ELSE 0 END) 
    AS diferencia
FROM defunciones
WHERE anio IN (2010, 2022)
GROUP BY categoria
ORDER BY diferencia DESC;
"""
dataframeResultado_diferencia = dd.query(consulta2).df()