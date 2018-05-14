# -*- coding: utf-8 -*-
# vim:fileencoding=utf-8
from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField
from wtforms.validators import DataRequired, NumberRange
import requests

class LoginForm(FlaskForm):
    userid = StringField('userid', validators = [DataRequired(message="Введите ссылку на страницу в формате vk.com/<ваша ссылка>")])
    Subscriptionscount = IntegerField('Subscriptionscount', validators = [DataRequired(message="Введите целое положительное число"), NumberRange(min=1, max=None, message="Введите целое положительное число")])
    FriendsCount = IntegerField('FriendsCount', validators = [DataRequired(message="Введите целое положительное число"), NumberRange(min=1, max=None, message="Введите целое положительное число")])
