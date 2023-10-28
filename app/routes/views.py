from flask import request, render_template
from flask.views import MethodView

class IndexView(MethodView):
    def __init__(self, tickets):
        self.tickets = tickets

    def get(self):

        return render_template('index.html', )

    def post(self):
        return self.get()
