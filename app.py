import os
import json
import requests
from flask import Flask, jsonify
from google.cloud import pubsub_v1
from datetime import datetime

app = Flask(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID")
TOPIC_ID = os.environ.get("PUBSUB_TOPIC")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@app.route("/", methods=["GET"])
def ingest_weather():
    # Public weather API (no API key needed)
    url = "https://api.open-meteo.com/v1/forecast?latitude=28.61&longitude=77.20&current_weather=true"
    response = requests.get(url, timeout=10)
    data = response.json()

    message = {
        "timestamp": datetime.utcnow().isoformat(),
        "city": "Delhi",
        "temperature": data["current_weather"]["temperature"],
        "humidity": 60,
        "source": "open-meteo"
    }

    publisher.publish(
        topic_path,
        json.dumps(message).encode("utf-8")
    )

    return jsonify({
        "status": "published",
        "message": message
    })


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080))
    )
