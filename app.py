from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import pandas as pd
import os
import re

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Permitir CORS para todos los orígenes

# Variables de entorno
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

# Función para conectar a la BD
def get_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

# Endpoint para recibir consultas SQL
@app.route("/consulta", methods=["POST"])
def consultar_bd():
    body = request.get_json()
    query = body.get("query")
    schema = body.get("schema", "public")  # por defecto 'public'

    # Validar el esquema
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", schema):
        return jsonify({"error": "Nombre de esquema inválido"}), 400

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Cambiar esquema
        cursor.execute(f"SET search_path TO {schema};")

        # Ejecutar consulta
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in data]

        cursor.close()
        conn.close()

        return jsonify({"results": results})

    except Exception as e:
        return jsonify({"error": str(e)}), 400

# Página principal
@app.route("/")
def inicio():
    return "API lista para recibir consultas"

# Probar conexión
@app.route("/probar_conexion", methods=["GET"])
def probar_conexion():
    try:
        conn = get_connection()
        conn.close()
        return jsonify({"mensaje": "Conexión exitosa con la base de datos"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
