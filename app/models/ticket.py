from datetime import datetime
from models import db

class Ticket(db.Model):
    __tablename__ = 'ticket'
    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('ticket_id_seq'::regclass)"))
    ticket = db.Column(db.String(20), unique=True, nullable=False)
    time = db.Column(db.TIMESTAMP, nullable=False, default=datetime.utcnow)
