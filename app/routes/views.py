from flask import request, render_template
from flask.views import MethodView
from models import db

class IndexView(MethodView):
    def __init__(self, tickets):
        self.tickets = tickets

    def get(self):
        default_start_date = '2023-01-01'
        default_end_date = '2023-12-31'
        selected_ticket = None
        selected_start_date = None
        selected_end_date = None

        return render_template('index.html', tickets=self.tickets, selected_ticket=selected_ticket,
                               default_start_date=default_start_date, default_end_date=default_end_date,
                               selected_start_date=selected_start_date, selected_end_date=selected_end_date)

    def post(self):
        selected_ticket = request.form.get('ticket')
        selected_start_date = request.form.get('start_date')
        selected_end_date = request.form.get('end_date')
        return self.get()
