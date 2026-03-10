from flask import Flask
import threading
import os
import asyncio
from bot import run_bot 

# Flask для того щоб бот не лягав на беслпатному хості, запускаю і стукаюсь на фоні.

app = Flask(__name__)

@app.route('/')
def home():
    return "✅ Бот активний!"

def run_web():
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_web, daemon=True)
    flask_thread.start()

    asyncio.run(run_bot())
