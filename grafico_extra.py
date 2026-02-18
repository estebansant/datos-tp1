import pandas as pd
import duckdb as dd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Carga de datos
directorio_script = os.path.dirname(os.path.abspath(__file__))
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
fig1.savefig('cobertura_por_provincia.png')
plt.show()

#%% Grafico 2

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
    axes[i].text(0.5, 1.02, f'Población: {int(total_pob):,}', transform=axes[i].transAxes, ha='center', va='center', fontsize=12, fontweight='medium')

fig2.legend(labels, loc='center right', title='Estado')

plt.tight_layout()
plt.subplots_adjust(top=0.85, right=0.85)

fig2.savefig('cobertura_nacional_donut.png')
plt.show()
