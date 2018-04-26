# -*- coding: utf-8 -*-
# vim:fileencoding=utf-8
import sys
import pandas as pd
reload(sys)
sys.setdefaultencoding('utf-8')
from flask import render_template, redirect, request
from app import app_this
from forms import LoginForm
from wtforms import Form
import vk

session = vk.Session(access_token='e9adbde0e9adbde0e9adbde0f6e9cf3fa0ee9ade9adbde0b3769eff25ba6ca9496ad29a')
api = vk.API(session)

@app_this.route('/')
@app_this.route('/index', methods=['GET', 'POST'])
def index():
    posts = [ 
        { 
            'author': { 'nickname': 'John' }, 
            'body': 'Beautiful day in Portland!' 
        },
        { 
            'author': { 'nickname': 'Susan' }, 
            'body': 'The Avengers movie was so cool!' 
        }
    ]

    return render_template("index.html",
        title = 'Рекомендации пабликов',
        posts = posts)

@app_this.route('/login', methods = ['GET', 'POST'])
def login():
    form = LoginForm()
    if request.method == 'POST':
        aaa = form.userid.data
        subscriptions = api.users.getSubscriptions(user_id=int(aaa), extended = 1, count = 10, version = 5.0)['items'];
        df = pd.DataFrame({'userid': []})
        for i in subscriptions:
            if api.groups.getMembers(i['id'], "id_asc", 0, 1)['count'] < 1000000:

                sys.stdout.write(i['name'])

    if form.validate_on_submit():
        return redirect('/index')
    return render_template('login.html', 
        title = 'Sign In',
        form = form)