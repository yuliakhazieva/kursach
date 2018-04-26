from flask import Flask
import sys
from wtforms import StringField
from wtforms.validators import DataRequired

app_this = Flask(__name__)
app_this.config.from_object('config')

import views

userid = StringField('userid', validators = [DataRequired()])