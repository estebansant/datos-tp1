
import pandas as pd
import os
import numpy as np 
import duckdb as dd

directorio_script = os.path.dirname(os.path.abspath(__file__))
archivo1 = os.path.join(directorio_script, "defunciones.csv")
archivo2 = os.path.join(directorio_script, "categoriasDefunciones.csv")

defunciones = pd.read_csv(archivo1, low_memory=False)
defunciones2 = pd.read_csv(archivo2)

#%%
# Empiezo a filtrar la tabla de defunciones por rango etario
print(defunciones)

consultaSQL = """
                SELECT cie10_causa_id, jurisdiccion_de_residencia_id, anio, sexo_id, cantidad,
                CASE
                WHEN grupo_edad LIKE '%01.De 0 a 14%' THEN '0 a 14'
                WHEN grupo_edad LIKE '%02.De 15 a 34%' THEN '15 a 34'
                WHEN grupo_edad LIKE '%03.De 35 a 54 anios%' THEN '35 a 54'
                WHEN grupo_edad LIKE '%04.De 55 a 74 anios%' THEN '55 a 74'
                WHEN grupo_edad LIKE '%05.De 75 anios y mas%' THEN '75 y mas'
                WHEN grupo_edad LIKE '%06.Sin especificar%' THEN '-1'
                END AS grupo_edad
                From defunciones
                WHERE sexo_id NOT IN ('3', '9');
              """

dataframeResultado = dd.query(consultaSQL).df()

consultaSQL_residencia= """
               SELECT DISTINCT jurisdicion_residencia_nombre, jurisdiccion_de_residencia_id
               FROM defunciones
                 """

#jurisdicciones
dataframeResultado_residencia = dd.query(consultaSQL_residencia).df()

consultaSQL_sexo = """
               SELECT DISTINCT sexo_id, Sexo
               FROM defunciones
               WHERE sexo_id NOT IN ('3', '9') AND Sexo NOT IN ('desconocido', 'indeterminado')

              """

#sexo y sexo_id sacando los casos de desconocido o indeterminado
dataframeResultado_sexo = dd.query(consultaSQL_sexo).df()

consultaSQL_causas = """
               SELECT DISTINCT codigo_def, categorias
               FROM defunciones2

              """

dataframeResultado_causas = dd.query(consultaSQL_causas).df()

#%%
consultaSQL_join_causas = """
                        SELECT *
                        FROM defunciones
                        INNER JOIN defunciones2
                        ON defunciones.cie10_causa_id = defunciones2.codigo_def
                      """

dataframeResultado_join_causas = dd.query(consultaSQL_join_causas).df()
#%%
dataframeResultado_join_causas.columns = ['anio', 'id_prov', 'nombre_prov',
                                'cie10_causa_id', 'cie10_clasificacion', 'sexo_id', 'sexo',
                                'muerte_materna_id', 'muerte_materna_clasificacion', 'rango_etario',
                                'cantidad', 'Unnamed: 0', 'codigo_def', 'categoria']

#%%
# Verifico cuantas apariciones hay de los id 98 y 99 que corresponden a lugares desconocidos de defuncion
# ademas veo cuantas coincidencias hay con sexo y provincia indefinidos
desconocidos1 = (dataframeResultado_join_causas["id_prov"] == 98).sum()
desconocidos2 = (dataframeResultado_join_causas["id_prov"] == 99).sum()
sexo_indef = ((dataframeResultado_join_causas["sexo_id"] == 3) | (dataframeResultado_join_causas["sexo_id"] == 9)).sum()


desconocido_e_indefinido = ((dataframeResultado_join_causas["id_prov"] == 98) & ((dataframeResultado_join_causas["sexo_id"] == 3) | (dataframeResultado_join_causas["sexo_id"] == 9))).sum()
desconocido_e_indefinido2 = ((dataframeResultado_join_causas["id_prov"] == 99) & ((dataframeResultado_join_causas["sexo_id"] == 3) | (dataframeResultado_join_causas["sexo_id"] == 9))).sum()
elementos = len(dataframeResultado_join_causas)

print(desconocidos1)
print(desconocidos2)
print("Total", desconocidos1+desconocidos2)
print("Desconocidos", desconocido_e_indefinido)
print("Desconocidos2", desconocido_e_indefinido2)
print("sexo indefinido:", sexo_indef)

print("filas", elementos)

# Sumando todo junto 5441 + 14353 no llegan a ser el 2.5% (son el 2.34%) del total de las 825765 filas de la tabla. Puedo eliminar esos datos sin perder informacion relevante del modelo.

#%%

# Elimino filas irrelevantes de info

consultaSQL_sin_indefiniciones= """
                        SELECT *
                        FROM dataframeResultado_join_causas
                        WHERE id_prov NOT IN (98, 99)
                        AND sexo_id NOT IN (3, 9)
                      """
                      
dataframeResultado_sin_indefiniciones = dd.query(consultaSQL_sin_indefiniciones).df()

dataframeResultado_sin_indefiniciones.columns = ['anio', 'id_prov', 'nombre_prov',
                                'cie10_causa_id', 'cie10_clasificacion', 'sexo_id', 'sexo',
                                'muerte_materna_id', 'muerte_materna_clasificacion', 'rango_etario',
                                'cantidad', 'Unnamed: 0', 'codigo_def', 'categoria']

columnas_validas = ['categoria', 'anio',
                    'sexo', 'rango_etario', 'id_prov',
                    'cantidad']

# Guardo solo las columnas que me interesan
df_limpio = dataframeResultado_sin_indefiniciones[columnas_validas].copy()
print(df_limpio)

#%%

# Noto que hay rango etario indefinido
rango_etario_desconocido = (df_limpio["rango_etario"] == "06.Sin especificar").sum()
print(rango_etario_desconocido)

# Lo elimino porque sumando todos los datos indefinidos solo representa el 3.1% del total de filas del set de datos para defunciones.

consultaSQL_eliminar_edad_indef = """
                                SELECT * FROM df_limpio
                                WHERE rango_etario != '06.Sin especificar';
                                """
                                
dataframeResultado_filtrado = dd.query(consultaSQL_eliminar_edad_indef).df()

# lo guardo como CSV
ruta_salida = os.path.join(directorio_script, "Defuncion.csv")
dataframeResultado_filtrado.to_csv(ruta_salida, index=False)

#%%

#Filtro para obtener la tabla de Provincia sin las provincias no identificadas

dataframeResultado_residencia.columns = ['nombre', 'id_prov']
cols_validas = ['id_prov', 'nombre']

provincias = dataframeResultado_residencia[cols_validas].copy()

consultaSQL_eliminar_provincias_indet = """
                                        SELECT * FROM provincias
                                        WHERE id_prov NOT IN (98, 99);
                                        """
                                        
dataframeResultado_provincias = dd.query(consultaSQL_eliminar_provincias_indet).df()

#guardo en CSV
ruta_salida = os.path.join(directorio_script, "Provincia.csv")
dataframeResultado_provincias.to_csv(ruta_salida, index=False)

#%%

# Censos

ruta_2010 = os.path.join(directorio_script, "censo2010.xlsX")
ruta_2022 = os.path.join(directorio_script, "censo2022.xlsX")

def procesar_archivo(ruta_entrada, anio):
    # Abro el archivo
    df_raw = pd.read_excel(ruta_entrada, header=None)
    # Inicializo variables
    datos_limpios = []
    provincia_actual = None
    id_prov_actual = None
    cobertura_actual = None
    tipo_cobertura = None
    
    # Itero por cada fila del DF
    
    for i in range(len(df_raw)):
        row = df_raw.iloc[i]
        
        # Extraigo valores de las celdas
        val_texto = str(row[1]) if pd.notna(row[1]) else ""

        val_edad_prov = row[2]
        val_varones = row[3]
        val_mujeres = row[4]
        
        if "RESUMEN" in val_texto.upper():
            provincia_actual = "RESUMEN"
            continue

        if provincia_actual == "RESUMEN":
            continue

        # Provincia
        # Busco "AREA #"
        if "AREA #" in val_texto:
            # Extraigo id de la provincia del texto "AREA #"
            try:
                # El formato es "AREA # id"
                id_prov_actual = int(val_texto.split("#")[1].strip())
            except:
                id_prov_actual = 0
                
            # Agarro el nombre de la provincia
            if pd.notna(val_edad_prov):
                provincia_actual = str(val_edad_prov).strip()
            continue
            
        # Cobertura
        texto_lower = val_texto.lower()
        if "obra social" in texto_lower or "prepaga" in texto_lower or "programas" in texto_lower or "no tiene" in texto_lower:
             cobertura_actual = val_texto.strip()
             continue
        
        if "total" in val_texto.strip().lower():
             cobertura_actual = "Total"
             continue

        # Ignoro el nombre de la columna de cobertura Total
        if "cobertura de salud" in texto_lower:
             continue
            
        # Si la cobertura actual es Total, ignoro todo el bloque de datos
        if cobertura_actual == "Total":
            continue

        # Edad
        if (provincia_actual and cobertura_actual) and (cobertura_actual != "Total"):
            if pd.isna(val_edad_prov):
                continue

            # Convierto a string para verificar contenido
            s_edad = str(val_edad_prov).strip()
            
            # Chequeo si la celda esta vacia
            if not s_edad:
                continue
            
            # Me aseguro de no agarrar la fila con el total de hombres y mujeres con el mismo tipo de cobertura social
            if "total" in s_edad.lower():
                continue

            # Si la edad no es un digito no lo agarro
            if not s_edad.replace('.0', '').isdigit():
                continue
            
            edad = int(float(s_edad))
            
            # Funcion para limpiar el numero de varones y mujeres
            def limpiar_cantidad(val):
                # Filtro para que no agarre la ultima fila del archivo (posible total general)
                if i == len(df_raw) - 1:
                    return 0

                if pd.isna(val): return 0
                s = str(val).strip()
                if s == '-' or s == '': return 0
                # Limpio los - y los renombro como que son un cero
                if not s.replace('.', '').isdigit(): return 0
                return int(float(s))
            
            cant_varones = limpiar_cantidad(val_varones)
            cant_mujeres = limpiar_cantidad(val_mujeres)
            
            # Determino si tiene cobertura, si en el texot hay un "no tiene" le asigno un 0, sino un 1 porque si tiene cobertura medica
            tiene_cobertura = 0 if "no tiene" in cobertura_actual.lower() else 1

            # Defino el tipo de cobertura
            if cobertura_actual == "No tiene obra social, prepaga o plan estatal" or "":
                tipo_cobertura = "No tiene cobertura"

            if cobertura_actual == "Programas o planes estatales de salud":
                tipo_cobertura = "Público"

            if cobertura_actual == "Obra social (incluye PAMI)":
                tipo_cobertura = "Público"

            if cobertura_actual == "Obra social o prepaga (incluye PAMI)":
                tipo_cobertura = "Mixto"
            
            if cobertura_actual == "No tiene obra social, prepaga ni plan estatal":
                tipo_cobertura = "No tiene cobertura"

            if cobertura_actual == "Prepaga sólo por contratación voluntaria":
                tipo_cobertura = "Privado"

            if cobertura_actual == "Prepaga a través de obra social":
                tipo_cobertura = "Privado"
            
            # Agrego a los varones al array
            datos_limpios.append({
                "anio": anio,
                "edad": edad,
                "sexo": "Varón",
                "tiene_cobertura": tiene_cobertura,
                "cantidad": cant_varones,
                "id_prov": id_prov_actual,
                "provincia": provincia_actual,
                "nombre_cobertura": cobertura_actual,
                "tipo_cobertura": tipo_cobertura
            })
            
            # Agrego a las mujeres al array
            datos_limpios.append({
                "anio": anio,
                "edad": edad,
                "sexo": "Mujer",
                "tiene_cobertura": tiene_cobertura,
                "cantidad": cant_mujeres,
                "id_prov": id_prov_actual,
                "provincia": provincia_actual,
                "nombre_cobertura": cobertura_actual,
                "tipo_cobertura": tipo_cobertura
            })

    # Convierto a DF
    df_final = pd.DataFrame(datos_limpios)

    # Selecciono las columnas del DF
    
    # Agrupo por rango etario usando SQL
    consultaSQL_rango_etario = """
        SELECT anio, sexo, tiene_cobertura, tipo_cobertura, id_prov, cantidad,
        CASE
        WHEN edad BETWEEN 0 AND 14 THEN '01.De a 0  a 14 anios'
        WHEN edad BETWEEN 15 AND 34 THEN '02.De 15 a 34 anios'
        WHEN edad BETWEEN 35 AND 54 THEN '03.De 35 a 54 anios'
        WHEN edad BETWEEN 55 AND 74 THEN '04.De 55 a 74 anios'
        WHEN edad >= 75 THEN '05.De 75 anios y mas'
        END AS rango_etario
        FROM df_final
    """
    df_final = dd.query(consultaSQL_rango_etario).df()

    # Selecciono las columnas del DF y devolvemos el DF
    cols = ["anio", "rango_etario", "sexo",  "tiene_cobertura","tipo_cobertura",  "id_prov", "cantidad"]
    df_final = df_final[cols]
    
    return df_final

censo2010 = procesar_archivo(ruta_2010, 2010)
censo2022 = procesar_archivo(ruta_2022, 2022)

# Unimos las tablas censo2010 y censo2022 con SQL usando DuckDB
consultaSQL = """
            SELECT * FROM censo2010 
            UNION ALL 
            SELECT * FROM censo2022
        """

poblacion = dd.query(consultaSQL).df()

# Guardo el resultado final en un CSV
ruta_salida = os.path.join(directorio_script, "Poblacion.csv")
poblacion.to_csv(ruta_salida, index=False)

#%%

# Instituciones de salud

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

df_valido.to_csv("Establecimiento.csv", index=False)

# %%
# Tabla Departamento
# Veo la tabla de provincias para filtrar
ruta_provincia = os.path.join(directorio_script, "Provincia.csv")

df_provincia = pd.read_csv(ruta_provincia)

consultaSQL_depto = """
                    SELECT DISTINCT id_depto, id_prov, nombre_depto as nombre
                    FROM df
                    WHERE id_prov IN (SELECT id_prov FROM df_provincia)
                    ORDER BY id_prov, id_depto
                """

df_depto = dd.query(consultaSQL_depto).df()
df_depto.to_csv(os.path.join(directorio_script, "Departamento.csv"), index=False)
