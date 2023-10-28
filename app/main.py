import os
import asyncio
import time
import sched
import threading
from datetime import datetime, timedelta

from binance import Spot, Future, TicketManager
from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from config import Config

from routes import IndexView
from models.ticket import Ticket

# binance = TicketManager(
#     interface=spot,
#     user=USER,
#     password=PASSWORD,
#     host=HOST,
#     port=PORT,
#     database=NAME
# )

app = Flask(__name__)
app.config.from_object(Config)
db = SQLAlchemy(app)
migrate = Migrate(app, db)

spot = Spot()

# def your_task(db, interface):
#     while True:
#         tickets = spot.get_all_tickets()

        
#         time.sleep(24 * 60 * 60)

# task_thread = threading.Thread(target=your_task, args=(db, spot))
# task_thread.start()

def main():
    # tickets = spot.get_all_tickets()
    tickets = Ticket.query.all()

    print(tickets)

    index_view = IndexView.as_view('index', tickets=tickets)
    app.add_url_rule('/', view_func=index_view)
    app.run(host='0.0.0.0', port=5000)

if __name__ == '__main__':
    main()
