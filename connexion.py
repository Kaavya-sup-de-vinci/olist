rom sqlalchemy import create_engine
import urllib.parse
import pandas as pd
 
# Paramètres de connexion PostgreSQL
user = 'postgres'
password = 'it_L3@rning'
host = 'localhost'
port = '5432'
database = 'olist_dwh'
 
# Encoder le mot de passe pour les caractères spéciaux
password_encoded = urllib.parse.quote(password)
 
# Créer l'URL de connexion
connection_url = f'postgresql+psycopg2://{user}:{password_encoded}@{host}:{port}/{database}'
 
# Charger le fichier CSV
csv_file_path = 'C:/Users/Etudiant/Desktop/datalake/fact_sales/dim_product.csv'
 
try:
    # Création de l'objet SQLAlchemy engine
    engine = create_engine(connection_url)
   
    # Tester la connexion
    with engine.connect() as connection:
        print("Connexion réussie à la base de données !")
   
    # Charger le fichier CSV dans un DataFrame
    df = pd.read_csv(csv_file_path, encoding='utf-8')  
 
    # Supprimer la première colonne (par index)
    df = df.iloc[:, 1:]  # Exclut la première colonne
   
    # Importer le DataFrame dans PostgreSQL
    table_name = 'dim_products'  # Nom de la table cible
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
 
    print(f"Table {table_name} importée avec succès dans la base de données.")
 
except Exception as e:
    print("Erreur :", e)
 