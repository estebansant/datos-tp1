import os
import pandas as pd
import duckdb

directorio_script = os.path.dirname(os.path.abspath(__file__))
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
        
        # Ignoro el nombre de la columna
        if "cobertura de salud" in texto_lower and "no tiene" not in texto_lower:
            continue
            
        # Edad
        # Verificamos si es una fila de datos. La edad debe ser un numero
        if provincia_actual and cobertura_actual:
            if pd.isna(val_edad_prov):
                continue

            # Convierto a string para verificar contenido
            s_edad = str(val_edad_prov).strip()
            
            if not s_edad.replace('.0', '').isdigit():
                continue
            
            edad = int(float(s_edad))
            
            # Funcion para limpiar el numero de varones y mujeres
            def limpiar_cantidad(val):
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
    # Selecciono las columnas del DF y devolvemos el DF
    cols = ["anio", "edad", "sexo", "tiene_cobertura", "cantidad", "id_prov", "provincia", "nombre_cobertura", "tipo_cobertura"]
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

poblacion = duckdb.query(consultaSQL).df()

# Guardo el resultado final en un CSV
ruta_salida = os.path.join(directorio_script, "poblacion.csv")
poblacion.to_csv(ruta_salida, index=False)