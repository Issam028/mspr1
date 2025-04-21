import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text, exc
from retrying import retry
import pymysql
import yaml
import uuid

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

            # Tables unifiées sans les colonnes source et traite
            schema = """
                id_pays CHAR(36),
                id_trans CHAR(36),
                date DATE,
                total_cases INT,
                total_death INT,
                people_vaccinated INT,
                people_fully_vac INT,
                PRIMARY KEY (id_pays, id_trans, date)
            """

            cursor.execute(f"CREATE TABLE IF NOT EXISTS donnees_covid ({schema})")
            cursor.execute(f"CREATE TABLE IF NOT EXISTS donnees_variole ({schema})")
            cursor.execute(f"CREATE TABLE IF NOT EXISTS donnees_ebola ({schema})")

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
        logging.info(f"Insertion de {len(df)} lignes dans '{table_name}'...")
        df.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi', chunksize=500)
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

def preparer_donnees(df, source, engine):
    maladie = source["disease"]

    # Fetch country mappings (id_pays for each pays_nom)
    country_mapping = {}
    with engine.connect() as conn:
        result = conn.execute(text("SELECT pays_nom, id_pays FROM pays"))
        
        # Log the result to inspect its structure
        rows = result.fetchall()  # Using fetchall to retrieve all rows from the result
        logging.info(f"Fetched rows: {rows[:5]}")  # Log first 5 rows for inspection
        
        # Build the mapping dictionary
        country_mapping = {row[0]: row[1] for row in rows}  # Access by index: row[0] = pays_nom, row[1] = id_pays

    # Map the country names to their corresponding id_pays
    df['id_pays'] = df['country'].map(country_mapping)  # Use 'country' instead of 'nom_pays'

    # Ensure that each row gets a unique id_trans (UUID)
    df['id_trans'] = [str(uuid.uuid4()) for _ in range(len(df))]

    # Convert the 'date' column to a proper datetime format
    df['date'] = pd.to_datetime(df['date'], errors='coerce') if 'date' in df.columns else pd.to_datetime(df.get('Data as of'), errors='coerce')

    # Handle disease-specific column renaming
    if maladie == "covid19":
        df = df.rename(columns={
            'total_cases': 'total_cases',
            'total_deaths': 'total_death',
            'people_vaccinated': 'people_vaccinated',
            'people_fully_vaccinated': 'people_fully_vac'
        })

    elif maladie == "monkeypox":
        df = df.rename(columns={
            'total_cases': 'total_cases',
            'total_deaths': 'total_death'
        })
        df['people_vaccinated'] = None
        df['people_fully_vac'] = None

    elif maladie == "ebola":
        df = df.rename(columns={
            'Numeric': 'total_cases'
        })
        df['total_death'] = None
        df['people_vaccinated'] = None
        df['people_fully_vac'] = None

    # Final unified column set
    final_cols = ['id_pays', 'id_trans', 'date', 'total_cases', 'total_death', 'people_vaccinated', 'people_fully_vac']
    for col in final_cols:
        if col not in df.columns:
            df[col] = None

    return df[final_cols]


def main():
    try:
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        creer_base_et_tables()
        engine = get_db_engine()

        for source in config["data_sources"]:
            try:
                logging.info(f"Traitement de la source: {source['name']}")
                df = telecharger_donnees(source['url'], f"{source['name']}.csv")
                df = preparer_donnees(df, source, engine)

                table = "donnees_covid" if source["disease"] == "covid19" else (
                        "donnees_variole" if source["disease"] == "monkeypox" else "donnees_ebola")

                inserer_donnees(df, table, engine)
            except Exception as e:
                logging.error(f"Erreur pendant le traitement de {source['name']}: {e}")

        logging.info("Pipeline ETL terminé avec succès.")

    except Exception as e:
        logging.error(f"Échec du pipeline: {e}", exc_info=True)
    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    main()
