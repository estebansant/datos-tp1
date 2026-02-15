
import pandas as pd
import os
import numpy as np 
import duckdb as dd

directorio_script = os.path.dirname(os.path.abspath(__file__))
archivo1 = os.path.join(directorio_script, "defunciones.csv")
archivo2 = os.path.join(directorio_script, "categoriasDefunciones.csv")

defunciones = pd.read_csv(archivo1, low_memory=False)
defunciones2 = pd.read_csv(archivo2)



#%%===========================================================================
# Tabla final 
#=============================================================================

print(defunciones)

consultaSQL = """
                Select cie10_causa_id, jurisdiccion_de_residencia_id, anio, sexo_id, cantidad,
                case
                When grupo_edad LIKE '%01.De a 0  a 14%' THEN '0 a 14'
                When grupo_edad LIKE '%02.De 15 a 34%' THEN '15 a 34'
                When grupo_edad LIKE '%03.De 35 a 54 anios%' THEN '35 a 54'
                When grupo_edad LIKE '%04.De 55 a 74 anios%' THEN '55 a 74'
                When grupo_edad LIKE '%05.De 75 anios y mas%' THEN '75 y mas'
                When grupo_edad LIKE '%06.Sin especificar%' THEN '-1'
                END AS grupo_edad
                From defunciones
                WHERE sexo_id NOT IN ('3', '9');
              """

dataframeResultado = dd.sql(consultaSQL).df()

print(dataframeResultado)

consultaSQL_residencia= """
               Select Distinct jurisdicion_residencia_nombre, jurisdiccion_de_residencia_id
               From defunciones

                 """

#jurisdicciones
dataframeResultado_residencia = dd.sql(consultaSQL_residencia).df()

print(dataframeResultado_residencia)

consultaSQL_sexo = """
               Select Distinct sexo_id, Sexo
               From defunciones
               WHERE sexo_id NOT IN ('3', '9') AND Sexo NOT IN ('desconocido', 'indeterminado')

              """

#sexo y sexo_id sacando los casos de desconocido o indeterminado
dataframeResultado_sexo = dd.sql(consultaSQL_sexo).df()

print(dataframeResultado_sexo)

consultaSQL_causas = """
               Select Distinct codigo_def, categorias
               From defunciones2

              """

dataframeResultado_causas = dd.sql(consultaSQL_causas).df()

print(dataframeResultado_causas)



