from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField
from wtforms.validators import DataRequired
import requests

class LoginForm(FlaskForm):
    userid = IntegerField('userid', validators = [DataRequired()])
