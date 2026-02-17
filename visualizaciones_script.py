import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Carga de datos (Tablas de tu modelo relacional)
df_pob = pd.read_csv('Poblacion.csv')
df_prov = pd.read_csv('Provincia.csv')

# 2. Relación entre tablas (JOIN)
# Unimos población con provincia usando id_prov para tener los nombres reales
# Esto garantiza consistencia total (Clase 11)
df_unificado = pd.merge(df_pob, df_prov, on='id_prov')

# 3. Procesamiento
# Agrupamos por el nombre de la provincia (que viene de df_prov) y el año
# Como ya corregiste la duplicación, no dividimos por 2
df_agrupado = df_unificado.groupby(['nombre', 'anio'])['cantidad'].sum().reset_index()

# 4. Visualización (Estilo Clase 12 y 13)
plt.figure(figsize=(10, 10))
sns.set_theme(style="whitegrid")

# Ordenamos por población del censo 2022 (Recomendación Clase 12)
orden_provincias = df_agrupado[df_agrupado['anio'] == 2022].sort_values('cantidad', ascending=False)['nombre']

# Gráfico de barras horizontales agrupadas
# Usamos 'nombre' para el eje Y para que sea legible, pero los datos vienen del id_prov
sns.barplot(
data=df_agrupado,
y='nombre',
x='cantidad',
hue='anio',
order=orden_provincias,
palette='muted'
)
# Títulos
plt.title('Población por Provincia: Censos 2010 y 2022', fontsize=14, fontweight='bold')
plt.xlabel('Cantidad de habitantes')
plt.ylabel('Provincia')

# Quitamos tight_layout si no lo viste en clase, el gráfico se verá bien igual
plt.show()

