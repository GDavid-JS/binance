from datetime import datetime
from models import db

class Tickets(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ticket = db.Column(db.String(20), unique=True, nullable=False)
    time = db.Column(db.TIMESTAMP, nullable=False, default=datetime.utcnow)
