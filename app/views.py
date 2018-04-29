# -*- coding: utf-8 -*-
# vim:fileencoding=utf-8
import sys
import pandas as pd
import time
import vk_api
from vk_api.execute import VkFunction
reload(sys)
sys.setdefaultencoding('utf-8')
from flask import render_template, redirect, request
from app import app_this
from forms import LoginForm
from wtforms import Form
import vk

session = vk.AuthSession(6464246, 'oppasaranhae@gmail.com', 'burnTHEfeel*19')
#session = vk.Session(access_token='e9adbde0e9adbde0e9adbde0f6e9cf3fa0ee9ade9adbde0b3769eff25ba6ca9496ad29a')
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
    memebersearchedcount = 0
    form = LoginForm()
    if request.method == 'POST':
        aaa = form.userid.data

        vk_session = vk_api.VkApi('oppasaranhae@gmail.com', 'burnTHEfeel*19', scope='subscriptions')
        vk_session.auth()

        min = 1000000
        mingid = 0
        ofs = 0
        while min >= 1000000:
            vvv = vk_session.get_api()
            subscriptions = vvv.users.getSubscriptions(user_id=int(aaa), extended=1, count=3, offeset = ofs, version=5.0, timeout=10)
            for group in subscriptions['items']:
                memberCount = api.groups.getMembers(group_id=group['id'], sort='id_asc', offset=0, count=0, version=5.0, timeout=10)['count']
                if memberCount < min:
                    min = memberCount
                    mingid = group['id']
            ofs+=3

        subscriptions = vvv.users.getSubscriptions(user_id=int(aaa), extended=1, version=5.0, timeout=10)
        df = pd.DataFrame({'userid': [0]})
        memberCount = api.groups.getMembers(group_id=mingid, sort='id_asc', offset=0,count=0, version = 5.0, timeout=10)['count']
        #для каждого куска по 1000 подписчиков этого паблика
        print('membercount')
        print(memberCount)
        for i in range (1, memberCount/1000):
            members = api.groups.getMembers(group_id=group['id'], sort="id_asc", offest=i, version = 5.0, timeout=10)
            with vk_api.VkRequestsPool(vk_session) as pool:
                dict = pool.method_one_param(
                    'users.getSubscriptions',
                    key = 'user_id',
                    values = members['users'],
                    default_values={'extended': 1, 'count': 20, 'version': 5.0, 'timeout': 10}
                )
            print('chunk go')
            for member in dict.result:
                if checkForTripleMatch(dict.result[member]['items'], subscriptions['items']):
                    df.loc[df.shape[0]] = [0 for n in range(df.shape[1])]
                    #идём по всем его подпискам и добавляем в табличку
                    howHighUp = 21;
                    for memberSub in dict.result[member]['items']:
                        if 'id' in memberSub:
                            howHighUp -= 1;
                            if memberSub['id'] in df:
                                df.at[df.shape[0] - 1, memberSub['id']] = howHighUp;
                            else:
                                df[memberSub['id']] = 0
                                df.at[df.shape[0] - 1, memberSub['id']] = howHighUp;
            print(df)
    if form.validate_on_submit():
        return redirect('/index')
    return render_template('login.html', 
        title = 'Sign In',
        form = form)

def checkForTripleMatch(membersubs, subscriptions):
    count = 0
    for sub in membersubs:
        if sub in subscriptions:
            count+=1;
            if count == 3:
                return True
    return False
