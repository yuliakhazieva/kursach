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
    memebersearchedcount = 0
    form = LoginForm()
    if request.method == 'POST':
        aaa = form.userid.data
        vk_session = vk_api.VkApi('oppasaranhae@gmail.com', 'burnTHEfeel*19')
        vk_session.auth()
        forExec = vk_session.get_api()

        min = 1000000
        mingid = 0
        ofs = 0
        while min >= 1000000:
            subscriptions = api.users.getSubscriptions(user_id=int(aaa), extended=1, count=3, offeset = ofs, version=5.0, timeout=10)
            for group in subscriptions:
                memberCount = api.groups.getMembers(group_id=group['gid'], sort='id_asc', offset=0, count=0, version=5.0, timeout=10)['count']
                if memberCount < min:
                    min = memberCount
                    mingid = group['gid']
            ofs+=3

        df = pd.DataFrame({'userid': [0]})
        memberCount = api.groups.getMembers(group_id=mingid, sort='id_asc', offset=0,count=0, version = 5.0, timeout=10)['count']
        #для каждого куска по 1000 подписчиков этого паблика
        for i in range (1, memberCount/1000):
            members = api.groups.getMembers(group_id=group['gid'], sort="id_asc", offest=i, version = 5.0, timeout=10)
            #для каждого пользователя
            vk_add = VkFunction(args=('version', 'members'), code='''
                var userToGroups;
                for (var member in members)
                    userToGroups[member] = API.users.getSubscriptions(user_id=member, extended = 1, count = 20, version = 5.0, timeout=10);
                return userToGroups;
            ''')

            with vk_api.VkRequestsPool(vk_session) as pool:
                dict = pool.method_one_param(
                    'users.getSubscriptions',
                    key = 'user_id',
                    values = members['users'],
                    default_values={'extended': 1, 'count': 20, 'version': 5.0, 'timeout': 10}
                )

            print(dict.result['gid'])

            for member in members['users']:
                memebersearchedcount+=1
                print(memebersearchedcount)
                memberSubs = api.users.getSubscriptions(user_id=member, extended = 1, count = 20, version = 5.0, timeout=10);
                if checkForTripleMatch(memberSubs, subscriptions, member):
                    df.loc[df.shape[0]] = [0 for n in range(df.shape[1])]
                    #идём по всем его подпискам и добавляем в табличку
                    howHighUp = 21;
                    for memberSub in memberSubs:
                        if'gid' in memberSub:
                            howHighUp -= 1;
                            if memberSub['gid'] in df:
                                df.at[df.shape[0] - 1, memberSub['gid']] = howHighUp;
                            else:
                                df[memberSub['gid']] = 0
                                df.at[df.shape[0] - 1, memberSub['gid']] = howHighUp;

    if form.validate_on_submit():
        return redirect('/index')
    return render_template('login.html', 
        title = 'Sign In',
        form = form)

def checkForTripleMatch(membersubs, subscriptions, member):
    count = 0
    for sub in membersubs:
        if sub in subscriptions:
            count+=1;
            if count == 3:
                return True
    return False
