#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
import json
import psycopg2
from sqlalchemy import create_engine
import redshift_connector


# In[2]:


# Leer las credentials desde un archivo de texto
credentials = {}
with open('credentials.txt', 'r') as file:
    for line in file:
        key, value = line.strip().split(': ')
        if key == 'port':
            value = int(value)  # Convertir el valor del puerto a entero
        credentials[key] = value

# Acceder a las credentials individualmente
usuario = credentials['usuario']
password = credentials['password']
host = credentials['host']
port = credentials['port']
database = credentials['database']
api_key = credentials['api_key']
port

# Utilizar las credentials como sea necesario
print(f"Usuario: {usuario} ({type(usuario)})")
print(f"Contraseña: {password} ({type(password)})")
print(f"Host: {host} ({type(host)})")
print(f"Puerto: {port} ({type(port)})")
print(f"Base de datos: {database} ({type(database)})")
print(f"API Key: {api_key} ({type(api_key)})")

print(f"Tipo de Usuario: {type(usuario).__name__}")
print(f"Tipo de Contraseña: {type(password).__name__}")
print(f"Tipo de Host: {type(host).__name__}")
print(f"Tipo de Puerto: {type(port).__name__}")
print(f"Tipo de Base de datos: {type(database).__name__}")
print(f"Tipo de API Key: {type(api_key).__name__}")


# In[3]:


#Extraigo los datos de las películas con el id y un bucle for

movie_list = []

for i in range(1, 500):
    url = f'https://api.themoviedb.org/3/movie/{i}?api_key={api_key}'
    response = requests.get(url)
    if response.status_code == 200:
        movie_json = response.json()
        movie_id = movie_json['id']
        movie_title = movie_json['title']
        movie_desc = movie_json['overview']
        movie_genre = [genre['name'] for genre in movie_json['genres']]
        movie_release = movie_json['release_date']
        movie_runtime = movie_json['runtime']
        movie_revenue = movie_json['revenue']
        movie_budget = movie_json['budget']
        movie_language = movie_json['original_language']
        movie_popularity = movie_json['popularity']
        movie_list.append({'id': movie_id, 'title': movie_title, 'description': movie_desc, 'genre': movie_genre, 
                           'release_date': movie_release, 'runtime': movie_runtime, 'revenue': movie_revenue, 
                           'budget': movie_budget, 'language': movie_language, 'popularity': movie_popularity})

movies_df = pd.DataFrame(movie_list)


# In[4]:


#Reviso el df
movies_df.head()


# In[5]:


#Reviso el tipo de datos y si hay nulos
movies_df.info()


# In[6]:


#Ejemplo de como viene la info estructurada desde la página
response.json()


# In[10]:


# Modifico el tipo de las variables
movies_df = movies_df.astype({'title': 'string', 'description': 'string', 'genre': 'string', 'release_date': 'string', 'language': 'string'})
movies_df = movies_df.drop('description', axis=1)
movies_df.head()


# In[8]:


conn = redshift_connector.connect(
     host=host,
     database=database,
     port=5439,
     user=usuario,
     password=password
  )

#conn.rollback()
conn.autocommit = True


# In[ ]:





# In[11]:


engine = create_engine(f"redshift+psycopg2://{usuario}:{password}@{host}:{port}/{database}")

# Guardar el DataFrame en la tabla 'movies' de Amazon Redshift
movies_df.to_sql('movies', engine, if_exists='replace', index=False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




