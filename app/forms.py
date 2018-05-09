from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField
from wtforms.validators import DataRequired
import requests

class LoginForm(FlaskForm):
    userid = StringField('userid', validators = [DataRequired()])
    Subscriptionscount = IntegerField('Subscriptionscount', validators = [DataRequired()])
    FriendsCount = IntegerField('FriendsCount', validators = [DataRequired()])
