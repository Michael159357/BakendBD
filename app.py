from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import pandas as pd
import time
import os

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Permitir CORS para todos los orígenes

import re

if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", schema):
    return {"error": "Nombre de esquema inválido"}


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

@app.post("/consulta")
def consultar_bd(request: Request):
    body = await request.json()
    query = body.get("query")
    schema = body.get("schema", "public")  # por defecto usa public si no se envía

    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Establecer el esquema deseado
        cursor.execute(f"SET search_path TO {schema};")

        # Ejecutar la consulta del usuario
        cursor.execute(query)
        data = cursor.fetchall()

        # Opcional: nombres de columnas
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in data]

        return {"results": results}

    except Exception as e:
        return {"error": str(e)}

@app.route("/")
def inicio():
    return "API lista para recibir consultas"

@app.route("/probar_conexion", methods=["GET"])
def probar_conexion():
    try:
        conn = get_connection()
        conn.close()
        return jsonify({"mensaje": "Conexión exitosa con la base de datos"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

