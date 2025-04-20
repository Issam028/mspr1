import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text, exc
from retrying import retry
import pymysql
import yaml

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_pandemie.log'),
        logging.StreamHandler()
    ]
)

# Charger la configuration
with open("config.yaml") as f:
    config = yaml.safe_load(f)

DB_NAME = config["database_name"]
DB_CONFIG = {
    'username': 'root',
    'password': '',
    'host': 'localhost',
    'port': 3306
}

def creer_base_et_tables():
    """Créer la base de données et les tables nécessaires"""
    try:
        logging.info("Création/vérification de la base de données et des tables...")
        conn = pymysql.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['username'],
            password=DB_CONFIG['password'],
            port=DB_CONFIG['port']
        )
        
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE {DB_NAME}")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS donnees_covid (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    date DATE,
                    pays VARCHAR(100),
                    cas_totaux FLOAT,
                    deces_totaux FLOAT,
                    patients_en_rea FLOAT,
                    patients_hospitalises FLOAT,
                    taux_reproduction FLOAT,
                    personnes_vaccinees FLOAT,
                    personnes_entierement_vaccinees FLOAT,
                    source VARCHAR(50),
                    traite BOOLEAN
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS donnees_variole (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    date DATE,
                    pays VARCHAR(100),
                    cas_totaux FLOAT,
                    deces_totaux FLOAT,
                    source VARCHAR(50),
                    traite BOOLEAN
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pandemies_combinees (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    date DATE NOT NULL,
                    pays VARCHAR(100) NOT NULL,
                    cas FLOAT,
                    deces FLOAT,
                    patients_en_rea FLOAT,
                    patients_hospitalises FLOAT,
                    taux_reproduction FLOAT,
                    personnes_vaccinees FLOAT,
                    personnes_entierement_vaccinees FLOAT,
                    maladie VARCHAR(20) NOT NULL,
                    cree_le TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
        conn.commit()
        logging.info(f"Base de données '{DB_NAME}' et tables créées avec succès.")
        
    except Exception as e:
        logging.error(f"Erreur lors de la création de la base ou des tables: {e}")
        raise
    finally:
        if 'conn' in locals() and conn.open:
            conn.close()

def get_db_engine():
    try:
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_NAME}",
            pool_pre_ping=True
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except exc.SQLAlchemyError as e:
        logging.error(f"Échec de la connexion à la base de données: {e}")
        raise

@retry(stop_max_attempt_number=3, wait_fixed=3000)
def inserer_donnees(df, table_name, engine, limit=1000):
    try:
        df = df.head(limit)
        lignes = len(df)
        logging.info(f"Insertion de {lignes} lignes dans '{table_name}'...")

        df.to_sql(
            table_name,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=500
        )
        logging.info(f"Insertion réussie dans '{table_name}'")
    except Exception as e:
        logging.error(f"Erreur d'insertion: {e}")
        raise

def telecharger_donnees(url, nom_fichier):
    os.makedirs(config["data_directory"], exist_ok=True)
    chemin = os.path.join(config["data_directory"], nom_fichier)
    logging.info(f"Téléchargement depuis {url}")
    df = pd.read_csv(url)
    df.to_csv(chemin, index=False)
    logging.info(f"Données sauvegardées dans {chemin}")
    return df

def preparer_donnees(df, source):
    maladie = source["disease"]
    df['source'] = maladie
    df['traite'] = True
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    if maladie == "covid19":
        colonnes = {
            'location': 'pays',
            'total_cases': 'cas_totaux',
            'total_deaths': 'deces_totaux',
            'icu_patients': 'patients_en_rea',
            'hosp_patients': 'patients_hospitalises',
            'reproduction_rate': 'taux_reproduction',
            'people_vaccinated': 'personnes_vaccinees',
            'people_fully_vaccinated': 'personnes_entierement_vaccinees'
        }
        df = df.rename(columns=colonnes)
        df = df[list(colonnes.values()) + ['date', 'source', 'traite']]
    elif maladie == "monkeypox":
        colonnes = {
            'location': 'pays',
            'total_cases': 'cas_totaux',
            'total_deaths': 'deces_totaux'
        }
        df = df.rename(columns=colonnes)
        df = df[list(colonnes.values()) + ['date', 'source', 'traite']]
    return df

def creer_table_combinee(engine):
    try:
        logging.info("Création de la table combinée...")
        covid_df = pd.read_sql("SELECT * FROM donnees_covid", engine)
        variole_df = pd.read_sql("SELECT * FROM donnees_variole", engine)

        df_combine = []

        if not covid_df.empty:
            df = covid_df.rename(columns={
                'cas_totaux': 'cas',
                'deces_totaux': 'deces'
            })[['date', 'pays', 'cas', 'deces', 'patients_en_rea', 'patients_hospitalises',
                'taux_reproduction', 'personnes_vaccinees', 'personnes_entierement_vaccinees']]
            df['maladie'] = 'covid19'
            df_combine.append(df)

        if not variole_df.empty:
            df = variole_df.rename(columns={
                'cas_totaux': 'cas',
                'deces_totaux': 'deces'
            })[['date', 'pays', 'cas', 'deces']]
            df['maladie'] = 'monkeypox'
            df = df.assign(
                patients_en_rea=None,
                patients_hospitalises=None,
                taux_reproduction=None,
                personnes_vaccinees=None,
                personnes_entierement_vaccinees=None
            )
            df_combine.append(df)

        if df_combine:
            df_final = pd.concat(df_combine, ignore_index=True)
            df_final['date'] = pd.to_datetime(df_final['date']).dt.date
            df_final.to_sql("pandemies_combinees", engine, if_exists='replace', index=False)
            logging.info("Table 'pandemies_combinees' créée avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de la création de la table combinée: {e}")
        raise

def main():
    try:
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        creer_base_et_tables()
        engine = get_db_engine()

        for source in config["data_sources"]:
            try:
                logging.info(f"Traitement de la source: {source['name']}")
                df = telecharger_donnees(source['url'], f"{source['name']}.csv")
                df = preparer_donnees(df, source)

                table = "donnees_covid" if source["disease"] == "covid19" else "donnees_variole"
                inserer_donnees(df, table, engine)
            except Exception as e:
                logging.error(f"Erreur pendant le traitement de {source['name']}: {e}")

        creer_table_combinee(engine)

        # Affichage final
        with engine.connect() as conn:
            tables = pd.read_sql("SHOW TABLES", conn)
            logging.info(f"Tables présentes dans '{DB_NAME}':\n{tables}")

            for table in tables[f"Tables_in_{DB_NAME}"]:
                count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", conn).iloc[0, 0]
                logging.info(f"Nombre de lignes dans '{table}': {count}")
                
        logging.info("Pipeline ETL terminé avec succès.")

    except Exception as e:
        logging.error(f"Échec du pipeline: {e}", exc_info=True)
    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    main()
