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
        subscriptions = api.users.getSubscriptions(user_id=int(aaa), extended = 1, count = 10, version = 5.0)
        df = pd.DataFrame({'userid': [0]})
        #идём по группам нашего пользователя
        for group in subscriptions:
            memberCount = api.groups.getMembers(group_id=group['gid'], sort='id_asc', offset=0,count=0, version = 5.0)['count']
            if memberCount  < 1000000:
                #для каждого куска по 1000 подписчиков этого паблика
                print(group['name'] + '\n\n\n\n')

                for i in range (1, memberCount/1000):
                    members = api.groups.getMembers(group_id=group['gid'], sort="id_asc", offest=i, version = 5.0)
                    #для каждого пользователя
                    for member in members['users']:
                        print('memderid\n\n\n\n')
                        print(member)
                        memberSubs = api.users.getSubscriptions(user_id=member, extended = 1, count = 10, version = 5.0);
                        df.append([0], ignore_index=True)
                        #идём по всем его подпискам и добавляем в табличку
                        howHighUp = 10;
                        print('membersubs\n\n\n\n\n\n')
                        for memberSub in memberSubs:
                            print(memberSub['gid'])
                            howHighUp -= 1;
                            if memberSub['gid'] in df:
                                df.at[df.shape[0] - 1, memberSub['gid']] = howHighUp;
                            else:
                                df[memberSub['gid']] = 0
                                df.at[df.shape[0] - 1, memberSub['gid']] = howHighUp;

                            for column in df:
                                if not df[column].iloc(df.shape[0] - 1):
                                    df.at[df.shape[0] - 1, column] = 0



    if form.validate_on_submit():
        return redirect('/index')
    return render_template('login.html', 
        title = 'Sign In',
        form = form)