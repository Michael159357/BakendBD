from flask import Flask, request, jsonify
import psycopg2
import pandas as pd
import time
import os

app = Flask(__name__)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

@app.route("/consulta", methods=["POST"])
def consulta():
    query = request.json.get("query")
    try:
        conn = get_connection()
        start = time.time()
        df = pd.read_sql_query(query, conn)
        tiempo = round(time.time() - start, 4)
        conn.close()
        return jsonify({
            "data": df.to_dict(orient="records"),
            "tiempo": tiempo
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/")
def inicio():
    return "API lista para recibir consultas"
