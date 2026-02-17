# -*- coding: utf-8 -*-
"""
Created on Mon Feb 16 18:06:37 2026


"""
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
archivo= 'poblacion.csv'

df = pd.read_csv(archivo)


# Unificamos los nombres de CABA para que el gráfico no los separe
df['provincia'] = df['provincia'].replace({
'Ciudad Autónoma de Buenos Aires': 'CABA',
'Caba': 'CABA'
})


# Agrupamos por provincia y año para obtener el total de habitantes
# Se aplica un factor de corrección de 0.5 si los datos están duplicados por categoría
df_agrupado = df.groupby(['provincia', 'anio'])['cantidad'].sum().reset_index()



# Configuramos el tamaño de la figura y el estilo
plt.figure(figsize=(10, 10))
sns.set_theme(style="whitegrid")

# Definimos el orden de las provincias de mayor a menor según el último censo
# Este ordenamiento facilita la comparación visual
orden = df_agrupado[df_agrupado['anio'] == 2022].sort_values('cantidad', ascending=False)['provincia']

# Generamos el gráfico de barras horizontales agrupadas por año (hue)
sns.barplot(
data=df_agrupado,
y='provincia',
x='cantidad',
hue='anio',
order=orden,
palette='muted'
)

# 5. Etiquetas y formato final
plt.title('Población por Provincia: Censos 2010 y 2022', fontsize=14, fontweight='bold')
plt.xlabel('Cantidad de habitantes')
plt.ylabel('Provincia')



plt.tight_layout()
plt.show()


