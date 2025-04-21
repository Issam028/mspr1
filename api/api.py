from fastapi import FastAPI, HTTPException
from typing import List, Optional
from datetime import date
from pydantic import BaseModel
import mysql.connector
import os
from dotenv import load_dotenv
import joblib
import numpy as np

load_dotenv()

app = FastAPI(
    title="Pandemic Analysis API",
    description="API for analyzing COVID-19 and Monkeypox data",
    version="1.0.0"
)

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "pandemic_impact"),
        port=os.getenv("DB_PORT", 3306)
    )

class PandemicRecord(BaseModel):
    id: int
    date: date
    country: str
    cases: Optional[float] = None
    deaths: Optional[float] = None
    icu_patients: Optional[float] = None
    hosp_patients: Optional[float] = None
    reproduction_rate: Optional[float] = None
    people_vaccinated: Optional[float] = None
    people_fully_vaccinated: Optional[float] = None
    disease: str

class CountryStats(BaseModel):
    country: str
    total_cases: float
    total_deaths: float
    mortality_rate: float

class PredictInput(BaseModel):
    date: date
    country: str
    deaths: float
    reproduction_rate: float
    people_vaccinated: float
    disease: str

@app.get("/records", response_model=List[PandemicRecord])
async def get_records(
    disease: Optional[str] = None,
    country: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 1000
):
    """Get pandemic records with filters"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        query = """
            SELECT * FROM combined_pandemics 
            WHERE 1=1
        """
        params = []
        
        if disease:
            query += " AND disease = %s"
            params.append(disease)
        if country:
            query += " AND country = %s"
            params.append(country)
        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND date <= %s"
            params.append(end_date)
            
        query += " ORDER BY date DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/countries", response_model=List[CountryStats])
async def get_country_stats(disease: str):
    """Get statistics by country for a specific disease"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        query = """
            SELECT 
                country,
                MAX(cases) as total_cases,
                MAX(deaths) as total_deaths,
                MAX(deaths)/NULLIF(MAX(cases), 0) as mortality_rate
            FROM combined_pandemics
            WHERE disease = %s
            GROUP BY country
            HAVING total_cases > 0
            ORDER BY total_cases DESC
        """
        cursor.execute(query, (disease,))
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/vaccination/stats")
async def get_vaccination_stats(country: str):
    """Get vaccination statistics for a specific country"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        query = """
            SELECT 
                date,
                people_vaccinated,
                people_fully_vaccinated,
                people_vaccinated_per_hundred,
                people_fully_vaccinated_per_hundred
            FROM donnees_covid
            WHERE location = %s
            ORDER BY date DESC
            LIMIT 1
        """
        cursor.execute(query, (country,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not result:
            raise HTTPException(status_code=404, detail="Country not found")
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/predict")
async def predict_cases(data: PredictInput):
    try:
        # Load the pre-trained model and feature names
        model = joblib.load("model.pkl")
        feature_columns = joblib.load("features.pkl")

        # Prepare input data for prediction
        input_data = {
            'day_of_year': data.date.timetuple().tm_yday,
            'deaths': data.deaths,
            'reproduction_rate': data.reproduction_rate,
            'people_vaccinated': data.people_vaccinated,
        }

        # Dummy encoding for country and disease
        for col in feature_columns:
            if col.startswith("country_"):
                input_data[col] = 1.0 if col == f"country_{data.country}" else 0.0
            elif col.startswith("disease_"):
                input_data[col] = 1.0 if col == f"disease_{data.disease}" else 0.0

        # Fill any missing columns with 0
        for col in feature_columns:
            input_data.setdefault(col, 0.0)

        # Format the input for prediction
        input_array = np.array([input_data[col] for col in feature_columns]).reshape(1, -1)
        prediction = model.predict(input_array)[0]

        return {
            "predicted_cases": round(prediction, 2)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
