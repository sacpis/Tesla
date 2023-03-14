from datetime import datetime

from flask import Flask, request, jsonify
from sqlite3 import connect, Connection
from typing import List

from RateLimiter import RateLimiter

# Allow only limit requests in interval seconds
limiter = RateLimiter(limit=10, interval=10)

DATABASE_PATH = "data.db"
MAXIMUM_TEMPERATURE = 90.0

app = Flask(__name__)


def create_db_table() -> None:
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_string TEXT NOT NULL
            );
        """)


def get_db() -> Connection:
    return connect(DATABASE_PATH, check_same_thread=False)


def insert_error(data_string: str) -> None:
    with get_db() as conn:
        conn.execute("""
            INSERT INTO errors (data_string) VALUES (?);
        """, (data_string,))


def get_errors() -> List[str]:
    with get_db() as conn:
        cursor = conn.execute("""
            SELECT data_string FROM errors;
        """)
        return [row[0] for row in cursor.fetchall()]


def delete_errors() -> None:
    with get_db() as conn:
        conn.execute("""
            DELETE FROM errors;
        """)


def parse_data_string(data_string: str) -> dict:
    try:
        device_id, epoch_ms, event_type, temperature = data_string.split(":")
        if event_type != '\'Temperature\'':
            raise ValueError("Invalid event type")
        return {
            "device_id": int(device_id),
            "timestamp": int(epoch_ms),
            "temperature": float(temperature)
        }
    except Exception as e:
        insert_error(data_string)
        raise ValueError("Invalid data string") from e


def is_over_temperature(temperature: float) -> bool:
    return temperature >= MAXIMUM_TEMPERATURE


@app.route('/', methods=["GET"])
def hello_world():
    return 'Hello, World!'


@app.route("/temp", methods=["POST"])
@limiter
def post_temperature():
    try:
        data_string = request.json["data"]
        data = parse_data_string(data_string)
        device_id = data["device_id"]
        epoch_ms = data["timestamp"]
        if is_over_temperature(data["temperature"]):
            formatted_time = datetime.fromtimestamp(epoch_ms / 1000).strftime('%Y/%m/%d %H:%M:%S')
            return jsonify({"overtemp": True, "device_id": device_id, "formatted_time": formatted_time})
        else:
            return jsonify({"overtemp": False})
    except ValueError as e:
        return jsonify({"error": "bad request"}), 400


@app.route("/errors", methods=["GET"])
@limiter
def get_errors_route():
    errors = get_errors()
    return jsonify({"errors": errors})


@app.route("/errors", methods=["DELETE"])
@limiter
def delete_errors_route():
    delete_errors()
    return "", 204


if __name__ == "__main__":
    create_db_table()
    app.run(host='0.0.0.0', port=8080)
