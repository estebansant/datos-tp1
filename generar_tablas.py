"""
Integrantes: Zoe Carrizo, Tobías Passarelli, Santiago Pizzani Esteban

Este codigo obtiene las tablas de censo2010, censo2022, defunciones, categoriaDefunciones e instituciones_de_salud, las filtra y ordena para que se adapten y adecuen al modelo relacional propuesot por el grupo.
Despues de eso, el codigo se encarga de realizar consultas SQL para generar reportes sobre los datos de dicho modelo relacional.
Se analizan la cobertura de salud por provincia, establecimientos de salud con unidad de terapia intensiva por cada provincia, causes de muerte, tasa de mortalidad del 2022 por provincia, y el cambio en cantidad de muertes para cada categoria de defuncion entre los años 2010 y 2022.
Luego se procede a realizar graficos relevantes que aporten un nuevo punto de vista a los datos. Entre los graficos se ecnuentran cuanta gente vivia en cada provincia en 2010 y 2022, las categorias de defuncion mas comunes, tasas de mortalidad por provincia (2022), defunciones por gruipo etario y sexo (2022), un boxplot con cantidad de establecimientos de salud por departamentos en cada provincia y un ultimo grafico que muestre de forma visual la cantidad de cobertura medica en 2010 vs 2022 por provincia, y a nivel nacional para analizar como evoluciono la cobertura medica con la cantidad de poblacion.

"""
#%%
import pandas as pd
import os
import numpy as np 
import duckdb as dd
import seaborn as sns
import matplotlib.pyplot as plt

directorio_script = os.path.dirname(os.path.abspath(__file__))
archivo1 = os.path.join(directorio_script, "TablasOriginales/defunciones.csv")
archivo2 = os.path.join(directorio_script, "TablasOriginales/categoriasDefunciones.csv")

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
ruta_salida = os.path.join(directorio_script, "TablasModelo/Defuncion.csv")
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
ruta_salida = os.path.join(directorio_script, "TablasModelo/Provincia.csv")
dataframeResultado_provincias.to_csv(ruta_salida, index=False)

#%%

# Censos

ruta_2010 = os.path.join(directorio_script, "TablasOriginales/censo2010.xlsX")
ruta_2022 = os.path.join(directorio_script, "TablasOriginales/censo2022.xlsX")

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
ruta_salida = os.path.join(directorio_script, "TablasModelo/Poblacion.csv")
poblacion.to_csv(ruta_salida, index=False)

#%%

# Instituciones de salud

archivo = os.path.join(directorio_script, "TablasOriginales/instituciones_de_salud.xlsx")

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

df_valido.to_csv("TablasModelo/Establecimiento.csv", index=False)

# %%
# Tabla Departamento
# Veo la tabla de provincias para filtrar
ruta_provincia = os.path.join(directorio_script, "TablasModelo/Provincia.csv")

df_provincia = pd.read_csv(ruta_provincia)

consultaSQL_depto = """
                    SELECT DISTINCT id_depto, id_prov, nombre_depto as nombre
                    FROM df
                    WHERE id_prov IN (SELECT id_prov FROM df_provincia)
                    ORDER BY id_prov, id_depto
                """

df_depto = dd.query(consultaSQL_depto).df()
df_depto.to_csv(os.path.join(directorio_script, "TablasModelo/Departamento.csv"), index=False)
#%%

# TABLAS SQL

#%%

#COBERTURA DE SALUD
# Cargo los archivos
archivo_prov = os.path.join(directorio_script, "TablasModelo/Provincia.csv")
archivo_poblacion = os.path.join(directorio_script, "TablasModelo/Poblacion.csv")

provincias = pd.read_csv(archivo_prov)
poblacion = pd.read_csv(archivo_poblacion)


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

dataframeResultado.to_csv(os.path.join(directorio_script, "Reportes/CoberturaDeSalud.csv"), index=False)

#%%

# ESTABLECIMIENTOS CON TERAPIA INTENSIVA
# Estoy cargando los archivos multiples veces porque desarrollamos el codigo de forma modular
# Cargo los archivos
archivo_prov = os.path.join(directorio_script, "TablasModelo/Provincia.csv")
archivo_establecimiento = os.path.join(directorio_script, "TablasModelo/Establecimiento.csv")

provincias = pd.read_csv(archivo_prov)
establecimiento = pd.read_csv(archivo_establecimiento)

# Hago algo similar a lo realizado en la consulta SQL anterior, pero usando filtros distintos. Decidi agregarle una columna extra para saber la cantidad de establecimientos de salud por provincia con unidad de terapia intensiva
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
dataframeResultado.to_csv(os.path.join(directorio_script, "Reportes/EstablecimientosTerapiaIntensiva.csv"), index=False)

#%%

# CODIGOS ZOEEEE

#%%

# GRAFICOS

# Grafico 1
# Cargo archivos

prov = os.path.join(directorio_script, "TablasModelo/Provincia.csv")
pob = os.path.join(directorio_script, "TablasModelo/Poblacion.csv")

df_pob = pd.read_csv(pob)
df_prov = pd.read_csv(prov)
df_pob = df_pob.rename(columns={'anio': 'Año'})

# Renombramos CABA y Tierra del Fuego con consulta SQL para que se vea mejor en el grafico
consultaSQL = """
            SELECT 
                CASE WHEN nombre LIKE 'Ciudad Autónoma de Buenos Aires'
                    THEN 'CABA'
                    WHEN nombre LIKE 'Tierra del Fuego, Antártida e Islas del Atlántico Sur'
                    THEN 'Tierra del Fuego'
                    ELSE nombre
                END AS nombre,
                id_prov
            FROM df_prov
             """

df_prov = dd.query(consultaSQL).df()

df_unificado = pd.merge(df_pob, df_prov, on='id_prov')

# Agrupo por provincia y año
df_agrupado = df_unificado.groupby(['nombre', 'Año'])['cantidad'].sum().reset_index()

# Divido por 1.000.000 para que el eje X sea legible
df_agrupado['pob_millones'] = df_agrupado['cantidad'] / 1_000_000


fig = plt.figure(figsize=(10, 10))
sns.set_theme(style="whitegrid")

# Ordeno por población el censo 2022
orden = df_agrupado[df_agrupado['Año'] == 2022].sort_values('pob_millones', ascending=False)['nombre']

sns.barplot(
data=df_agrupado,
y='nombre',
x='pob_millones',
hue='Año',
order=orden,
palette='muted'
)

# Ejes del grafico
plt.xlabel('Cantidad de habitantes (en millones)')
plt.ylabel('Provincia')

plt.tight_layout()
#Guardo
fig.savefig('Graficos/poblacion_2010_2022.png')
plt.show()

#%% Grafico 2
df_def = pd.read_csv('TablasModelo/Defuncion.csv')
df_def = df_def.rename(columns={'anio': 'Año'})

# Limpieza de categorías
df_def['categoria'] = df_def['categoria'].str.strip()


nombre_largo = 'Síntomas, signos y hallazgos anormales clínicos y de laboratorio, no clasificados en otra parte'
df_def['categoria'] = df_def['categoria'].replace({nombre_largo: 'Causas mal definidas'})
# Filtramos los años del censo para comparar
df_comparacion = df_def[df_def['Año'].isin([2010, 2022])]


df_agrupado = df_comparacion.groupby(['categoria', 'Año'])['cantidad'].sum().reset_index()

# Selecciono las 10 principales causas busco las que mas muertes tuvieron en 2022
top_10_causas = df_agrupado[df_agrupado['Año'] == 2022].sort_values('cantidad', ascending=False).head(10)['categoria']
df_final = df_agrupado[df_agrupado['categoria'].isin(top_10_causas)]


fig = plt.figure(figsize=(12, 8))
sns.set_theme(style="whitegrid")

# Ordeno de mayor a menor
orden = df_final[df_final['Año'] == 2022].sort_values('cantidad', ascending=False)['categoria']

sns.barplot(
    data=df_final,
    y='categoria',
    x='cantidad',
    hue='Año',
    order=orden,
    palette='flare'
)

# Ejes del grafico
plt.xlabel('Cantidad de Defunciones')
plt.ylabel('Causa de Muerte')

plt.tight_layout()
fig.savefig('Graficos/defunciones_categorias.png')
plt.show()

#%% Grafico 3

# Filtro de seguridad para población 
pob_2022 = df_pob[df_pob['Año'] == 2022].groupby('id_prov')['cantidad'].sum().reset_index()

def_2022 = df_def[df_def['Año'] == 2022].groupby('id_prov')['cantidad'].sum().reset_index()
def_2022 = def_2022.rename(columns={'cantidad': 'muertes'})

df_tasa = pd.merge(pob_2022, def_2022, on='id_prov')
df_tasa = pd.merge(df_tasa, df_prov, on='id_prov')

# Calculo la tasa por cada 1.000 habitantes
df_tasa['tasa_mortalidad'] = (df_tasa['muertes'] / df_tasa['cantidad']) * 1000

fig = plt.figure(figsize=(12, 10))
sns.set_theme(style="whitegrid")

# Ordeno por tasa de mortalidad
orden = df_tasa.sort_values('tasa_mortalidad', ascending=True)['nombre']

sns.barplot(
    data=df_tasa,
    y='nombre',
    x='tasa_mortalidad',
    order=orden,
    palette='viridis',
    edgecolor="0.2"
)

# Ejes del grafico
plt.xlabel('Defunciones por cada 1.000 habitantes')
plt.ylabel('Provincia')

plt.tight_layout()
fig.savefig('Graficos/defunciones_2022.png')
plt.show()

#%% Grafico 3 b) 


# Elijo el sezo para hacer el grafico con este elemento extra diferenciador
df_pob['sexo'] = df_pob['sexo'].replace({'Varón': 'masculino', 'Mujer': 'femenino'})


# Agrupo poblacion por provincia y sexo
pob_sexo = df_pob[df_pob['Año'] == 2022].groupby(['id_prov', 'sexo'])['cantidad'].sum().reset_index()
def_sexo = df_def[df_def['Año'] == 2022].groupby(['id_prov', 'sexo'])['cantidad'].sum().reset_index()


df_merge = pd.merge(pob_sexo, def_sexo, on=['id_prov', 'sexo'], suffixes=('_pob', '_def'))
df_merge = pd.merge(df_merge, df_prov, on='id_prov')
df_merge['tasa'] = (df_merge['cantidad_def'] / df_merge['cantidad_pob']) * 1000

# Pongo en mayuscula para que en la leyenda aparezca Masculino y Femenino
df_merge['sexo'] = df_merge['sexo'].str.capitalize()

fig = plt.figure(figsize=(10, 12))
sns.set_theme(style="whitegrid")

# Uso el mismo orden que el gráfico anterior para que sea facil comparar
orden_provincias = df_merge.groupby('nombre')['tasa'].mean().sort_values().index

sns.barplot(
    data=df_merge,
    y='nombre',
    x='tasa',
    hue='sexo',
    order=orden_provincias,
    palette='muted'
)

# Ejes del grafico
plt.xlabel('Defunciones por cada 1.000 habitantes')
plt.ylabel('Provincia')
plt.legend(title='Sexo')
plt.tight_layout()
fig.savefig('Graficos/defunciones_sexo_2022.png')
plt.show()

#%% Grafico 4

# Reasigno valores a Varón y Mujer por masculino y femenino
df_pob['sexo'] = df_pob['sexo'].replace({'Varón': 'masculino', 'Mujer': 'femenino'})

# Cambio edad por rango etario
df_pob = df_pob.rename(columns={'edad': 'rango_etario'})

# Renombro los rangos etarios
limpieza_edades = {
    '01.De a 0  a 14 anios': 'De 0 a 14 años',
    '02.De 15 a 34 anios': 'De 15 a 34 años',
    '03.De 35 a 54 anios': 'De 35 a 54 años',
    '04.De 55 a 74 anios': 'De 55 a 74 años',
    '05.De 75 anios y mas': 'De 75 años y más'
}

# Limpio los dataframes
df_pob['rango_etario'] = df_pob['rango_etario'].replace(limpieza_edades)
df_def['rango_etario'] = df_def['rango_etario'].replace(limpieza_edades)

# Agrupo población por grupo etario y sexo
pob_grupo = df_pob[df_pob['Año'] == 2022].groupby(['rango_etario', 'sexo'])['cantidad'].sum().reset_index()

# Agrupo defunciones por grupo etario y sexo
def_grupo = df_def[df_def['Año'] == 2022].groupby(['rango_etario', 'sexo'])['cantidad'].sum().reset_index()
def_grupo = def_grupo.rename(columns={'cantidad': 'muertes'})

# Fusiono los dataframes
df_normalizado = pd.merge(pob_grupo, def_grupo, on=['rango_etario', 'sexo'])

# Calculo la tasa por cada 1.000 habitantes para cada grupo
df_normalizado['tasa_grupo'] = (df_normalizado['muertes'] / df_normalizado['cantidad']) * 1000

# Primera letra mayuscula
df_normalizado['sexo'] = df_normalizado['sexo'].str.capitalize()

fig = plt.figure(figsize=(12, 7))
sns.set_theme(style="whitegrid")

# Ordeno los grupos etarios para que queden de menor a mayor edad
orden_edades = sorted(df_normalizado['rango_etario'].unique())

sns.barplot(
    data=df_normalizado,
    x='rango_etario',
    y='tasa_grupo',
    hue='sexo',
    order=orden_edades,
    palette='muted'
)

# Ejes del grafico
plt.xlabel('Grupo Etario')
plt.ylabel('Defunciones cada 1.000 habitantes del grupo')
plt.legend(title='Sexo')

plt.tight_layout()

# guardo el grafico
fig.savefig('Graficos/defunciones_edad_sexo.png')
plt.show()

#%% Grafico 5

archivo_prov = os.path.join(directorio_script, "TablasModelo/Provincia.csv")
archivo_establecimiento = os.path.join(
    directorio_script, "TablasModelo/Establecimiento.csv")

provincias = pd.read_csv(archivo_prov)
establecimiento = pd.read_csv(archivo_establecimiento)

# Uno las tablas para que tengan el nombre de la provincia
consultaSQL = """
                SELECT DISTINCT est.id_establecimiento, est.id_depto, est.id_prov, pro.nombre AS 'provincia'
                FROM establecimiento AS est
                INNER JOIN provincias AS pro
                ON est.id_prov = pro.id_prov
                ORDER BY est.id_prov, est.id_depto
            """
dataframeResultado = dd.query(consultaSQL).df()

# Sumo aquellos elementos que tengas igual id_depto e id_prov para saber cuantas instituciones de salud hay en cada departamento
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

# Aqui hago el grafico ordenado por la mediana
orden_provincias = dataframeResultado.groupby(
    "provincia")["cantidad_total"].median().sort_values(ascending=False).index

plt.figure(figsize=(10, 12))

# Hago el boxplot
sns.boxplot(
    data=dataframeResultado,
    x="cantidad_total",
    y="provincia",
    order=orden_provincias,
    palette="mako",
    legend=False
)

plt.xlabel("Cantidad de Establecimientos")
plt.ylabel("Provincia")
plt.tight_layout()

plt.savefig(os.path.join(directorio_script,
            "Graficos/distribucion_establecimientos.png"), bbox_inches='tight', dpi=300)

plt.show()

#%% Grafico 6 a)

archivo_prov = os.path.join(directorio_script, "TablasModelo/Provincia.csv")
archivo_pob = os.path.join(directorio_script,"TablasModelo/Poblacion.csv")

df_prov = pd.read_csv(archivo_prov)
df_pob = pd.read_csv(archivo_pob)


# Le cambio el nombre a CABA y TDF para que ocupen menos espacio en el grafico
consultaSQL = """
            SELECT 
            CASE WHEN nombre LIKE 'Ciudad Autónoma de Buenos Aires'
                THEN 'CABA'
            WHEN nombre LIKE 'Tierra del Fuego, Antártida e Islas del Atlántico Sur'
                THEN 'Tierra del Fuego'
            ELSE nombre
            END AS nombre,
            id_prov
            
            FROM df_prov
             """
df_prov = dd.query(consultaSQL).df()


# Unimos los datos de población con los nombres de provincia
df_unificado = pd.merge(df_pob, df_prov, on='id_prov')

# Veo poblacion total y con cobertura por provincia y año
consulta_agrupada = """
            SELECT nombre, anio, 
                SUM(cantidad) AS total_poblacion,
                SUM(CASE WHEN tiene_cobertura = 1 THEN cantidad ELSE 0 END) AS con_cobertura
            FROM df_unificado
            GROUP BY nombre, anio
"""

df_agrupado = dd.query(consulta_agrupada).df()

# Calcular porcentaje
df_agrupado['porcentaje_cobertura'] = (df_agrupado['con_cobertura'] / df_agrupado['total_poblacion']) * 100

# Ordeno por porcentaje del 2022
orden = df_agrupado[df_agrupado['anio'] == 2022].sort_values('porcentaje_cobertura', ascending=False)['nombre']

fig1 = plt.figure(figsize=(12, 10))

# Grafico de barras
sns.barplot(
    data=df_agrupado,
    y='nombre',
    x='porcentaje_cobertura',
    hue='anio',
    order=orden,
    palette='muted'
)

plt.xlabel('Porcentaje de población con cobertura médica (%)')
plt.ylabel('Provincia')
plt.legend(title='Año')
plt.tight_layout()
fig1.savefig('Graficos/cobertura_por_provincia.png')
plt.show()

#%% Grafico 6 b)

# Agrupo a nivel nacional por año y cobertura sumando la cantidad
consulta_nacional = """
SELECT 
    anio, 
    SUM(CASE WHEN tiene_cobertura = 1 THEN cantidad ELSE 0 END) AS con_cobertura,
    SUM(CASE WHEN tiene_cobertura = 0 THEN cantidad ELSE 0 END) AS sin_cobertura
FROM df_unificado
GROUP BY anio
"""
df_nacional = dd.query(consulta_nacional).df()

# Configuro subplots: 1 fila, 2 columnas
fig2, axes = plt.subplots(1, 2, figsize=(14, 7))

anios = [2010, 2022]
labels = ['Sin Cobertura', 'Con Cobertura']
colors = ['#a6cee3', '#fdbf6f']

for i, anio in enumerate(anios):
    # Obtenemos los valores sin y con cobertura
    fila = df_nacional[df_nacional['anio'] == anio].iloc[0]
    sin_cob = fila['sin_cobertura']
    con_cob = fila['con_cobertura']
    total_pob = sin_cob + con_cob
    
    sizes = [sin_cob, con_cob]
    
    # pie chart
    patches, texts, autotexts = axes[i].pie(
        sizes, 
        labels=None, 
        colors=colors, 
        autopct='%1.1f%%', 
        startangle=90, 
        pctdistance=0.5, 
        textprops={'weight': 'bold', 'fontsize': 12, 'color': 'black'}
    )
    
    axes[i].set_title(f'Año {anio}', fontsize=14, fontweight='bold', pad=20)
    axes[i].text(0.5, 1, f'Población: {int(total_pob):,}', transform=axes[i].transAxes, ha='center', va='center', fontsize=14, fontweight='medium')

fig2.legend(labels, loc='center right', title='Estado')

plt.tight_layout()
plt.subplots_adjust(top=0.85, right=0.85)

fig2.savefig('Graficos/cobertura_nacional.png')
plt.show()
