import os

from flask_sqlalchemy import SQLAlchemy

USER = os.environ.get('POSTGRES_USER')
PASSWORD = os.environ.get('POSTGRES_PASSWORD')
HOST = os.environ.get('HOST')
PORT = os.environ.get('POSTGRES_PORT')
NAME = os.environ.get('NAME')

class Config:
    SQLALCHEMY_DATABASE_URI = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
