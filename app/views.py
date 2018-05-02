# -*- coding: utf-8 -*-
# vim:fileencoding=utf-8
import sys
import pandas as pd
import multiprocessing
from itertools import product
import os
import collections
import requests
import time
import vk_api
from vk_api.execute import VkFunction
reload(sys)
from vk_api import exceptions
sys.setdefaultencoding('utf-8')
from flask import render_template, redirect, request
from app import app_this
from forms import LoginForm
import vk
from contextlib import contextmanager

#читаем пароль из файлика
with open('tokenFile.txt', 'r') as f:
    password = f.read()

#стартуем сессии в обоих либах
session = vk.AuthSession(6467194, 'oppasaranhae@gmail.com', password)
api = vk.API(session)
vk_session = vk_api.VkApi('oppasaranhae@gmail.com', password, scope='subscriptions')
vk_session.auth()
vvv = vk_session.get_api()

@app_this.route('/')
@app_this.route('/index', methods=['GET', 'POST'])
def index():

    #заглушка
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

        #парсим значение формы
        aaa = ''
        while aaa == '':
            aaa = form.userid.data
            if aaa[:9] == 'vk.com/id':
                if not RepresentsInt(aaa[9:]):
                    aaa = ''
            if aaa[:7] == 'vk.com/':
                aaa = vvv.users.get(user_ids = aaa[7:])[0]['id']
                print(aaa)
            else:
                aaa = ''

        #находим в подписках любимый паблик с минимальным количеством людей
        min = 1000000
        mingid = 0
        ofs = 0
        while min >= 1000000:
            subscriptions = vvv.users.getSubscriptions(user_id=int(aaa), extended=1, count=3, offeset = ofs, version=5.0, timeout=20)
            for group in subscriptions['items']:
                memberCount = api.groups.getMembers(group_id=group['id'], sort='id_asc', offset=0, count=0, version=5.0, timeout=20)['count']
                if memberCount < min:
                    min = memberCount
                    mingid = group['id']
            ofs+=3

        subscriptions = vvv.users.getSubscriptions(user_id=int(aaa), extended=1, version=5.0, timeout=10)
        df = pd.DataFrame({'userid': [0]})
        dict = {}

        members = api.groups.getMembers(group_id=group['id'], sort="id_asc", version=5.0, timeout=10)
        for i in range(0, 39):
            with vk_api.VkRequestsPool(vk_session) as pool:
                for j in range (0 + 25 * i, 24 + 25 * i):
                    dict[members['users'][j]] = pool.method('users.getSubscriptions', {
                        'user_id': members['users'][j],
                        'extended': 1, 'count': 20, 'version': 5.0, 'timeout': 10})

        od = collections.OrderedDict(sorted(dict.items()))
        for key, value in od.items():
            print(key)
            try:
                dict[key] = value.result
            except:
                dict[key] = {}
                pass

        for member in dict:
            if checkForTripleMatch(dict[member]['items'], subscriptions['items']):
                df.loc[df.shape[0]] = [0 for n in range(df.shape[1])]
                #идём по всем его подпискам и добавляем в табличку
                howHighUp = 21;
                for memberSub in dict[member]['items']:
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

def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def checkForTripleMatch(membersubs, subscriptions):
    count = 0
    for sub in membersubs:
        if sub in subscriptions:
            count+=1;
            if count == 3:
                # print('count')
                # print(count)
                return True
    # print('count')
    # print(count)
    return False
