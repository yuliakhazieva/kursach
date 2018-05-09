# -*- coding: utf-8 -*-
# vim:fileencoding=utf-8
import sys
import pandas as pd
import multiprocessing
from requests.adapters import HTTPAdapter
import math
import time
import requests
import vk_api
reload(sys)
sys.setdefaultencoding('utf-8')
from flask import render_template, redirect, request
from app import app_this
from forms import LoginForm
import vk

#читаем пароль из файлика
with open('tokenFile.txt', 'r') as f:
    password = f.read()

#стартуем сессии в обоих либах
session = vk.AuthSession(6470661, 'oppasaranhae@gmail.com', password)
session.requests_session.keep_alive = False

api = vk.API(session)
vk_session = vk_api.VkApi('oppasaranhae@gmail.com', password, scope='subscriptions')
vk_session.auth()
vk_session.http.mount('https://', HTTPAdapter(max_retries=10))

vvv = vk_session.get_api()

outputTable = {}
mapUserToLists = {}
mapGroupToSubscribers = {}
mapGroupToSubscribersRes = {}

@app_this.route('/')
@app_this.route('/index', methods=['GET', 'POST'])
def index():

    return render_template("index.html",
        title = 'Рекомендации пабликов',
        outputTable = outputTable)

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

        subscriptions = vvv.users.getSubscriptions(user_id=int(aaa), extended=1, version=5.0, timeout=10, count=200)
        df = pd.DataFrame({'userid': [0.0], 'pearson': [0.0], 'count': [0.0]})

        df.loc[df.shape[0]] = ['rrr' for n in range(df.shape[1])]
        rank = subscriptions['count']
        for sub in subscriptions['items']:
            if 'name' in sub:
                df.at[0, sub['id']] = str(rank)
                df.at[1, sub['id']] = sub['name']
                df.at[1, 'pearson'] = 0.999
            rank-=1
        dict = {}

        #для каждой группы
        # for group in subscriptions['items']:
        for t in range(0, 3):
            group = subscriptions['items'][t]
            if 'name' in group:
                print(group['name'])
                memCount = api.groups.getMembers(group_id=group['id'], sort="id_asc", version=5.0, timeout=10)['count']
                #для каждого куска по 25 тысяч
                if(memCount/1000/25 > 0):
                    firstUpperBound = memCount/1000/25
                else:
                    firstUpperBound = 1

                for m in range (0, firstUpperBound):
                    print(m)
                    time.sleep(0.25)
                    with vk_api.VkRequestsPool(vk_session) as pool:
                        if m != memCount/1000/25:
                            upperbound = 24 + 25 * m
                        else:
                            upperbound = memCount/1000

                        for j in range(0 + 25 * m, upperbound):
                            if group['id'] in mapGroupToSubscribers:
                                mapGroupToSubscribers[group['id']] = mapGroupToSubscribers[group['id']] + [pool.method('groups.getMembers', {
                                    'group_id': group['id'], 'sort': 'id_desc', 'offset': j * 1000})]
                            else:
                                mapGroupToSubscribers[group['id']] = [pool.method('groups.getMembers', {
                                    'group_id': group['id'], 'sort': 'id_desc', 'offset': j * 1000})]

        for element in mapGroupToSubscribers.items():
            try:
                for piece in element[1]:
                    if element[0] in mapGroupToSubscribersRes:
                        mapGroupToSubscribersRes[element[0]] = mapGroupToSubscribersRes[element[0]] + piece.result['items']
                    else:
                        mapGroupToSubscribersRes[element[0]] = [piece.result['items']]
            except:
                pass

        for key in mapGroupToSubscribersRes:
            for val in mapGroupToSubscribersRes[key]:
                if(type(val) == list):
                    for el in val:
                        if el in mapUserToLists:
                            mapUserToLists[el] += 1
                        else:
                            mapUserToLists[el] = 1
                else:
                    if val in mapUserToLists:
                        mapUserToLists[val] += 1
                    else:
                        mapUserToLists[val] = 1

        relativePiece = 1
        mapUserToLists1 = {}
        while len(mapUserToLists1.keys()) == 0:
            print('one')
            mapUserToLists1 = {k: v for k, v in mapUserToLists.items() if v > relativePiece}
            print(len(mapUserToLists1.keys()))
            if len(mapUserToLists1.keys()) > 100:
                print('two')
                relativePiece += 1
                mapUserToLists1 = {}
                print(len(mapUserToLists1.keys()))
            elif len(mapUserToLists1.keys()) < 10:
                print('three')
                relativePiece -=1
                mapUserToLists1 = {k: v for k, v in mapUserToLists.items() if v > relativePiece}
                print(len(mapUserToLists1.keys()))

        print('len mupl1')
        print(len(mapUserToLists1))
        print('rel pie')
        print(relativePiece)

        vk_session.http.close()
        vk_session.auth()
        for j in range(0, len(mapUserToLists1)/25):
            print('beep')
            try:
                with vk_api.VkRequestsPool(vk_session) as pool:
                    if j != len(mapUserToLists1) / 25:
                        upperbound = 24 + 25 * j
                    else:
                        upperbound = len(mapUserToLists1) - 1
                    for g in range(0 + 25 * j, upperbound):
                        dict[mapUserToLists1.keys()[g]] = pool.method('users.getSubscriptions', {'user_id':mapUserToLists1.keys()[g], 'extended':1, 'version':5.0, 'timeout':10, 'count':200})
            except:
                api.http_handler(error=vk_api.exceptions.ApiHttpError)

        for key, value in dict.items():
            try:
                dict[key] = value.result
            except:
                dict.pop(key, None)
                pass

        if aaa in dict.keys():
            dict.pop(aaa)

        print('filling the df')
        for member in dict:
            print(member)
            df.loc[df.shape[0]] = [0 for n in range(df.shape[1])]
            #идём по всем его подпискам и добавляем в табличку
            howHighUp = dict[member]['count'] + 1
            df.at[df.shape[0] - 1, 'userid'] = member
            df.at[df.shape[0] - 1, 'count'] =  dict[member]['count']
            for memberSub in dict[member]['items']:
                if 'name' in memberSub:
                    print('boop')
                    howHighUp -= 1
                    if memberSub['id'] in df:
                        df.at[df.shape[0] - 1, memberSub['id']] = str(howHighUp)
                    else:
                        df[memberSub['id']] = str(0)
                        df.at[1, memberSub['id']] = memberSub['name']
                        df.at[df.shape[0] - 1, memberSub['id']] = str(howHighUp)

        print(df)
        print('calculating pearson')
        df.at[0, 'count'] = subscriptions['count']
        sumOfOurGrades = df.at[0, 'count'] * (df.at[0, 'count'] + 1) / 2

        ourAvr = sumOfOurGrades / (len(df.columns) - 3)
        sumOfAllPearsons = 0.0
        for index, row in df.iterrows():
            if index != 1:
                sumOfgrades = int(df.at[index, 'count'])*(int(df.at[index, 'count']) + 1)/2
                userAvr = sumOfgrades / (len(df.columns) - 3)

                multSum = 0
                ourSquaredSum = 0
                userSquaredSum = 0

                for col in range (3, df.shape[1] - 1):
                    userDiff = int(df.iat[index, col]) - userAvr
                    ourDiff = int(df.iat[0, col]) - ourAvr
                    multSum += userDiff * ourDiff
                    ourSquaredSum += ourDiff * ourDiff
                    userSquaredSum += userDiff * userDiff

                df.at[index, 'pearson'] = abs(multSum / math.sqrt(ourSquaredSum) / math.sqrt(userSquaredSum))
                sumOfAllPearsons += df.at[index, 'pearson']
        print(df)
        df = df.sort_values('pearson', ascending=0)
        print('sorted')
        print(df)
        df.index = range(0, df.shape[0])
        print('reindexed')
        print(df)
        df.drop(df.iloc[:, 3:subscriptions['count']+3], inplace=True, axis=1)
        print('dropped we are already subscribed to')
        print(df)

        dfPart = df.loc[2:, (df).sum(axis=0) > 0]
        print('try drop zeroes')
        print(dfPart)

        df.loc[df.shape[0]] = [0 for n in range(df.shape[1])]
        for col in range(3, df.shape[1] - 1):
            corrTimesRank = 0.0
            for index, row in df.iterrows():
                if (index != df.shape[0] - 1 and index != 1):
                    corrTimesRank += df.at[index, 'pearson'] * int(df.iat[index, col])
            df.iat[df.shape[0] - 1, col] = corrTimesRank/sumOfAllPearsons
        df = df[df.columns[df.ix[df.last_valid_index()].argsort()]]
        print('sorted by rec rank')
        print(df)

        outputCount = 10
        f = df.shape[1]
        while outputCount != 0 and f != 2:
            f-=1
            if api.groups.getMembers(group_id=group['id'], sort="id_asc", version=5.0, timeout=10)['count'] < 1000000:
                outputTable[df.iat[1, f]] = 'vk.com/public' + str(df.columns[f])
                outputCount -= 1

        print(outputTable)


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
            count+=1
            if count == 5:
                return True
    return False
