import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# %% Grafico I
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

 
error_2010_idx = df_pob[(df_pob['id_prov'] == 94) & (df_pob['anio'] == 2010) & (df_pob['cantidad'] > 100000)].index.min()

# Encontramos el índice donde empieza el error de 2022
error_2022_idx = df_pob[(df_pob['id_prov'] == 94) & (df_pob['anio'] == 2022) & (df_pob['cantidad'] > 100000)].index.min()

# Filtramos: Nos quedamos solo con los datos que están ANTES de esos bloques de error
df_2010_limpio = df_pob[(df_pob['anio'] == 2010) & (df_pob.index < error_2010_idx)]
df_2022_limpio = df_pob[(df_pob['anio'] == 2022) & (df_pob.index < error_2022_idx)]

# Juntamos los dos años ya limpios
df_pob = pd.concat([df_2010_limpio, df_2022_limpio])


df_unificado = pd.merge(df_pob, df_prov, on='id_prov')


df_agrupado = df_unificado.groupby(['nombre', 'anio'])['cantidad'].sum().reset_index()

# ESCALA: Dividimos por 1.000.000 para que el eje X sea legible (Clase 13)
df_agrupado['pob_millones'] = df_agrupado['cantidad'] / 1_000_000


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


plt.title('Población por Provincia: Censos 2010 y 2022', fontsize=14, fontweight='bold')
plt.xlabel('Cantidad de habitantes (en millones)')
plt.ylabel('Provincia')

plt.tight_layout()
plt.show()

#%% Grafico II
df_def = pd.read_csv('Defuncion.csv')

# Limpieza de categorías (quita espacios invisibles al final)
df_def['categoria'] = df_def['categoria'].str.strip()

# Filtramos los años del censo para comparar
df_comparacion = df_def[df_def['anio'].isin([2010, 2022])]


df_agrupado = df_comparacion.groupby(['categoria', 'anio'])['cantidad'].sum().reset_index()

# Seleccionamos las TOP 10 causas (para que el gráfico sea legible)
# Buscamos las que más muertes tuvieron en 2022
top_10_causas = df_agrupado[df_agrupado['anio'] == 2022].sort_values('cantidad', ascending=False).head(10)['categoria']
df_final = df_agrupado[df_agrupado['categoria'].isin(top_10_causas)]


plt.figure(figsize=(12, 8))
sns.set_theme(style="whitegrid")

# Ordenamos de mayor a menor según 2022
orden = df_final[df_final['anio'] == 2022].sort_values('cantidad', ascending=False)['categoria']

sns.barplot(
    data=df_final,
    y='categoria',
    x='cantidad',
    hue='anio',
    order=orden,
    palette='flare' # Una paleta distinta para diferenciar del gráfico 1
)

plt.title('Principales 10 Causas de Defunción en Argentina (2010 vs 2022)', fontsize=14, fontweight='bold')
plt.xlabel('Cantidad de Defunciones')
plt.ylabel('Causa de Muerte')

plt.tight_layout()
plt.show()

#%% Grafico III

# Filtro de seguridad para población 
df_pob_clean = df_pob[df_pob['cantidad'] <= 100000]

pob_2022 = df_pob_clean[df_pob_clean['anio'] == 2022].groupby('id_prov')['cantidad'].sum().reset_index()

def_2022 = df_def[df_def['anio'] == 2022].groupby('id_prov')['cantidad'].sum().reset_index()
def_2022 = def_2022.rename(columns={'cantidad': 'muertes'})

df_tasa = pd.merge(pob_2022, def_2022, on='id_prov')
df_tasa = pd.merge(df_tasa, df_prov, on='id_prov')

# Calculamos la Tasa por cada 1.000 habitantes
df_tasa['tasa_mortalidad'] = (df_tasa['muertes'] / df_tasa['cantidad']) * 1000

plt.figure(figsize=(12, 10))
sns.set_theme(style="whitegrid")

# Ordenamos por la tasa de mortalidad
orden = df_tasa.sort_values('tasa_mortalidad', ascending=False)['nombre']

sns.barplot(
    data=df_tasa,
    y='nombre',
    x='tasa_mortalidad',
    order=orden,
    palette='Reds_r' # Rojo intenso para mortalidad
)

plt.title('Tasa de Mortalidad por Provincia (Censo 2022)', fontsize=14, fontweight='bold')
plt.xlabel('Defunciones por cada 1.000 habitantes')
plt.ylabel('Provincia')

plt.tight_layout()
plt.show()

#%% Grafico III b) 
df_pob = df_pob[df_pob['cantidad'] <= 100000]

#Se eligió el sexo porque es un factor clave 
#en el análisis epidemiológico. 
#La mortalidad no suele ser uniforme entre hombres
# y mujeres debido a factores biológicos y comportamentales 
#(como la exposición a riesgos o el acceso a controles médicos).


# Población usa 'Varón/Mujer' y Defunciones usa 'masculino/femenino'. 
# Deben ser iguales para poder cruzarlos.
df_pob['sexo'] = df_pob['sexo'].replace({'Varón': 'masculino', 'Mujer': 'femenino'})


# Sumamos población y muertes por PROVINCIA y SEXO
pob_sexo = df_pob[df_pob['anio'] == 2022].groupby(['id_prov', 'sexo'])['cantidad'].sum().reset_index()
def_sexo = df_def[df_def['anio'] == 2022].groupby(['id_prov', 'sexo'])['cantidad'].sum().reset_index()


df_merge = pd.merge(pob_sexo, def_sexo, on=['id_prov', 'sexo'], suffixes=('_pob', '_def'))
df_merge = pd.merge(df_merge, df_prov, on='id_prov')
df_merge['tasa'] = (df_merge['cantidad_def'] / df_merge['cantidad_pob']) * 1000


plt.figure(figsize=(10, 12))
sns.set_theme(style="whitegrid")

# Usamos el mismo orden que el gráfico anterior para que sea fácil comparar
orden_provincias = df_merge.groupby('nombre')['tasa'].mean().sort_values().index

sns.barplot(
    data=df_merge,
    y='nombre',
    x='tasa',
    hue='sexo',
    order=orden_provincias,
    palette='muted'
)

plt.title('Tasa de Mortalidad por Provincia y Sexo (2022)', fontsize=14, fontweight='bold')
plt.xlabel('Defunciones por cada 1.000 habitantes')
plt.ylabel('Provincia')
plt.legend(title='Sexo')
plt.tight_layout()
plt.show()

#%% Grafico iv)




# (Filtramos el error de Tierra del Fuego detectado anteriormente)
df_pob = df_pob[df_pob['cantidad'] <= 100000]


# Cambiamos Varón/Mujer por masculino/femenino y 'edad' por 'rango_etario'
df_pob['sexo'] = df_pob['sexo'].replace({'Varón': 'masculino', 'Mujer': 'femenino'})
df_pob = df_pob.rename(columns={'edad': 'rango_etario'})

# Filtramos año 2022
# Sumamos población por grupo etario y sexo
pob_grupo = df_pob[df_pob['anio'] == 2022].groupby(['rango_etario', 'sexo'])['cantidad'].sum().reset_index()

# Sumamos defunciones por grupo etario y sexo
def_grupo = df_def[df_def['anio'] == 2022].groupby(['rango_etario', 'sexo'])['cantidad'].sum().reset_index()
def_grupo = def_grupo.rename(columns={'cantidad': 'muertes'})

# Definimos un "diccionario" con los cambios. 
limpieza_edades = {
    '01.De a 0  a 14 anios': 'De 0 a 14 años',
    '02.De 15 a 34 anios': 'De 15 a 34 años',
    '03.De 35 a 54 anios': 'De 35 a 54 años',
    '04.De 55 a 74 anios': 'De 55 a 74 años',
    '05.De 75 anios y mas': 'De 75 años y más'
}

# Aplicamos el cambio directamente a la columna de cada tabla
df_pob['rango_etario'] = df_pob['rango_etario'].replace(limpieza_edades)
df_def['rango_etario'] = df_def['rango_etario'].replace(limpieza_edades)

df_normalizado = pd.merge(pob_grupo, def_grupo, on=['rango_etario', 'sexo'])

# Calculamos la Tasa: Muertes por cada 1.000 habitantes de ESE grupo específico
df_normalizado['tasa_grupo'] = (df_normalizado['muertes'] / df_normalizado['cantidad']) * 1000


plt.figure(figsize=(12, 7))
sns.set_theme(style="whitegrid")

# Ordenamos los grupos etarios para que queden de menor a mayor edad
orden_edades = sorted(df_normalizado['rango_etario'].unique())

sns.barplot(
    data=df_normalizado,
    x='rango_etario',
    y='tasa_grupo',
    hue='sexo',
    order=orden_edades,
    palette='muted'
)

plt.title('Gráfico IV: Tasa de Mortalidad por Grupo Etario y Sexo (2022)', fontsize=14, fontweight='bold')
plt.xlabel('Grupo Etario')
plt.ylabel('Defunciones cada 1.000 habitantes del grupo')


plt.tight_layout()
plt.show()

