import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Carga de datos
df_pob = pd.read_csv('Poblacion.csv')
df_prov = pd.read_csv('Provincia.csv')
#Al contrastar los totales del archivo Poblacion.csv 
#con los datos oficiales del Censo Nacional de Población (INDEC), 
#observamos que Tierra del Fuego (ID 94) 
#presenta una suma de 45 millones en 2022, 
#mientras que el valor oficial es de 190.695 habitantes. 
#Esta discrepancia del 23.500% confirma un grave error de carga 
#de datos
# 2. LIMPIEZA DE DATOS 
error_2010_idx = df_pob[(df_pob['id_prov'] == 94) & (df_pob['anio'] == 2010) & (df_pob['cantidad'] > 100000)].index.min()

# Encontramos el índice donde empieza el error de 2022
error_2022_idx = df_pob[(df_pob['id_prov'] == 94) & (df_pob['anio'] == 2022) & (df_pob['cantidad'] > 100000)].index.min()

# Filtramos: Nos quedamos solo con los datos que están ANTES de esos bloques de error
df_2010_limpio = df_pob[(df_pob['anio'] == 2010) & (df_pob.index < error_2010_idx)]
df_2022_limpio = df_pob[(df_pob['anio'] == 2022) & (df_pob.index < error_2022_idx)]

# Juntamos los dos años ya limpios
df_pob = pd.concat([df_2010_limpio, df_2022_limpio])


# 3. Integración de datos (JOIN)
df_unificado = pd.merge(df_pob, df_prov, on='id_prov')

# 4. Procesamiento
df_agrupado = df_unificado.groupby(['nombre', 'anio'])['cantidad'].sum().reset_index()

# ESCALA: Dividimos por 1.000.000 para que el eje X sea legible (Clase 13)
df_agrupado['pob_millones'] = df_agrupado['cantidad'] / 1_000_000

# 5. Visualización
plt.figure(figsize=(10, 10))
sns.set_theme(style="whitegrid")

# Ordenamos por población del Censo 2022
orden = df_agrupado[df_agrupado['anio'] == 2022].sort_values('pob_millones', ascending=False)['nombre']

sns.barplot(
data=df_agrupado,
y='nombre',
x='pob_millones',
hue='anio',
order=orden,
palette='muted'
)

# 6. Etiquetas finales
plt.title('Población por Provincia: Censos 2010 y 2022', fontsize=14, fontweight='bold')
plt.xlabel('Cantidad de habitantes (en millones)')
plt.ylabel('Provincia')

plt.tight_layout()
plt.show()
