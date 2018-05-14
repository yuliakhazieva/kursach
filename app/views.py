# -*- coding: utf-8 -*-
# vim:fileencoding=utf-8
import sys
from scipy.sparse.linalg import svds
import pandas as pd
import math
import time
import vk_api
import numpy as np
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
vvv = vk_session.get_api()

#словари для мап редьюс
outputTable = {'':''}
outputTable2 = {'':''}
friends = {'':''}
mapUserToLists = {}
mapGroupToSubscribers = {}
mapGroupToSubscribersRes = {}

@app_this.route('/')
@app_this.route('/index', methods=['GET', 'POST'])
def index():

    return render_template("index.html",
        title = 'Рекомендации пабликов',
        outputTable = outputTable, outputTable2 = outputTable2, friends = friends)

@app_this.route('/login', methods = ['GET', 'POST'])
def login():

    form = LoginForm()
    if request.method == 'POST':

        #парсим значение формы с ссылкой на профиль
        aaa = ''
        aaa = form.userid.data
        if aaa[:9] == 'vk.com/id':
            if not RepresentsInt(aaa[9:]):
                aaa = ''
        elif aaa[:7] == 'vk.com/':
            aaa = vvv.users.get(user_ids = aaa[7:])[0]['id']
            print(aaa)
        else:
            aaa = ''

        #читаем прочие формы
        subscriptionsCountInput = form.Subscriptionscount.data
        friendsCountInput = form.FriendsCount.data

        if form.validate_on_submit():
            if RepresentsInt(subscriptionsCountInput) and RepresentsInt(friendsCountInput):
                if subscriptionsCountInput >= 1 and friendsCountInput >= 1 and aaa[:7] == 'vk.com/':
                    return redirect('/index')
                else:
                    return render_template('login.html',
                                    title='Sign In',
                                    form=form, message="Неверные данные")
            else:
                return render_template('login.html',
                                title='Sign In',
                                form=form, message="Неверные данные")
        else:
            return render_template('login.html',
                            title='Sign In',
                            form=form, message="Неверные данные")

        #читаем список подписок нашего пользоваетеля
        subscriptions = vvv.users.getSubscriptions(user_id=int(aaa), extended=1, version=5.0, timeout=10, count=200)

        #задаем две таблицы данных и заполняем данными нашего пользователя
        df = pd.DataFrame({'userid': [0], 'pearson': [0.0], 'count': [0.0]})
        publics_df = pd.DataFrame({'public_id': [0], 'public_name': ['']})
        rank = subscriptions['count']
        for sub in subscriptions['items']:
            if 'name' in sub:
                df.at[0, sub['id']] = rank
                publics_df.loc[publics_df.shape[0]] = [sub['id'], sub['name']]
            rank-=1
        df.at[0, 'count'] = subscriptions['count']
        sumOfOurGrades = df.shape[1] - 3 * (df.shape[1] - 3 + 1) / 2

        print('init')
        print(df)
        print(publics_df)


        #из указанного количества подписок берем пользователей
        for t in range(0, subscriptionsCountInput):
            group = subscriptions['items'][t]
            if 'name' in group:
                print(group['name'])
                memCount = api.groups.getMembers(group_id=group['id'], sort="id_asc", version=5.0, timeout=10)['count']

                if(memCount/1000/25 > 0):
                    firstUpperBound = memCount/1000/25
                else:
                    firstUpperBound = 1

                for m in range (0, firstUpperBound):
                    print(m)
                    time.sleep(0.18)
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

        print('get res')
        #берем результаты запроса
        for element in mapGroupToSubscribers.items():
            try:
                for piece in element[1]:
                    if element[0] in mapGroupToSubscribersRes:
                        mapGroupToSubscribersRes[element[0]] = mapGroupToSubscribersRes[element[0]] + piece.result['items']
                    else:
                        mapGroupToSubscribersRes[element[0]] = [piece.result['items']]
            except:
                pass

        print('map')
        #МАП пользователь - количество упоминаний
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
        mapUserToLists1 = mapUserToLists

        #уменьшаем размер рассматриваемых пользователей в целях экономии времени
        good_enough = False
        while not good_enough:
            print('reduce')
            print(len(mapUserToLists1.keys()))
            mapUserToLists1 = {k: v for k, v in mapUserToLists1.items() if v > relativePiece}
            if len(mapUserToLists1.keys()) > friendsCountInput:
                relativePiece += 1
            elif len(mapUserToLists1.keys()) < 10:
                relativePiece -=1
                mapUserToLists1 = {k: v for k, v in mapUserToLists.items() if v > relativePiece}
                good_enough = True
            else:
                good_enough = True

        while len(mapUserToLists1.keys()) > friendsCountInput:
            mapUserToLists1.popitem()

        #берем подписки этих пользователей
        dict = {}
        vk_session.http.close()
        vk_session.auth()
        for j in range(0, len(mapUserToLists1)/25):
            print(j)
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

        print('res')
        #записываем результат запроса
        for key, value in dict.items():
            print(key)
            try:
                dict[key] = value.result
            except:
                dict.pop(key, None)
                pass

        #убираем нашего пользователя из словаря
        if aaa in dict.keys():
            dict.pop(aaa)

        print('fill df')
        #заносим данные в таблицы
        for member in dict:
            print(member)
            df.loc[df.shape[0]] = [0 for n in range(df.shape[1])]
            howHighUp = dict[member]['count'] + 1
            df.at[df.shape[0] - 1, 'userid'] = member
            df.at[df.shape[0] - 1, 'count'] =  dict[member]['count']
            for memberSub in dict[member]['items']:
                if 'name' in memberSub:
                    print(memberSub['name'])
                    howHighUp -= 1
                    if memberSub['id'] in df:
                        df.at[df.shape[0] - 1, memberSub['id']] = str(howHighUp)
                    else:
                        df[memberSub['id']] = 0
                        df.at[df.shape[0] - 1, memberSub['id']] = str(howHighUp)
                        publics_df.loc[publics_df.shape[0]] = [memberSub['id'], memberSub['name']]

        print(df)
        print(publics_df)

        ourAvr = sumOfOurGrades / (len(df.columns) - 3)
        sumOfAllPearsons = 0.0

        #считаем коэф пирсона
        print('pearson now')
        for index, row in df.iterrows():
            print(index)
            sumOfgrades = int(df.at[index, 'count'])*(int(df.at[index, 'count']) + 1)/2
            userAvr = sumOfgrades / (len(df.columns) - 3)

            multSum = 0
            ourSquaredSum = 0
            userSquaredSum = 0

            for col in range (3, df.shape[1] - 1):
                print(col)
                userDiff = int(df.iat[index, col]) - userAvr
                ourDiff = int(df.iat[0, col]) - ourAvr
                multSum += userDiff * ourDiff
                ourSquaredSum += ourDiff * ourDiff
                userSquaredSum += userDiff * userDiff

            print (index)
            print (ourSquaredSum)
            print (userSquaredSum)
            df.at[index, 'pearson'] = abs(multSum / math.sqrt(ourSquaredSum) / math.sqrt(userSquaredSum))
            sumOfAllPearsons += df.at[index, 'pearson']

        print('pearson')
        print(df)
        print(publics_df)

        df = df.sort_values('pearson', ascending=0)
        print('sorted')
        print(df)
        print(publics_df)

        print('filling friends')
        session2 = vk.AuthSession(6470661, 'oppasaranhae@gmail.com', password)
        # session.requests_session.keep_alive = False
        api2 = vk.API(session2)
        for f in range(1, 11):
            print(f)
            time.sleep(0.3)
            user = api2.users.get(user_ids = df.at[f, 'userid'],timeout=10, version = 5.0)[0]
            friends['vk.com/id' + str(df.at[f, 'userid'])] = user['first_name'] + " " + user['last_name']

        df.index = range(0, df.shape[0])
        if df.shape[0] > 300:
            df = df.drop(df.index[range(int((df.shape[0]) / 100.0 * 30), df.shape[0])])
        df.index = range(0, df.shape[0])
        print('reindexed')
        print(df)
        print(publics_df)

        df.drop(df.iloc[:, 3:subscriptions['count']+3], inplace=True, axis=1)
        print('dropped we are already subscribed to')
        print(df)
        print(publics_df)

        print('count recommendability')
        #рассчитываем показатель рекоммендованности
        df.loc[df.shape[0]] = [0 for n in range(df.shape[1])]
        for col in range(3, df.shape[1] - 1):
            print(col)
            corrTimesRank = 0.0
            for index, row in df.iterrows():
                print(index)
                if (index != df.shape[0] - 1 and index != 1):
                    corrTimesRank += df.at[index, 'pearson'] * int(df.iat[index, col])
            df.iat[df.shape[0] - 1, col] = corrTimesRank/sumOfAllPearsons
        df = df[df.columns[df.ix[df.last_valid_index()].argsort()]]
        print('sorted by rec rank')
        print(df)
        print(publics_df)

        R_df =  df.drop(['pearson', 'count'], axis=1)
        R_df.set_index('userid')
        R = R_df.as_matrix()
        user_ratings_mean = np.mean(R, axis=1)
        R_demeaned = R - user_ratings_mean.reshape(-1, 1)
        k = 50
        flag = False

        while not flag:
            try:
                U, sigma, Vt = svds(R_demeaned, k=k)
                flag = True
            except:
                k -= 1
                flag = False

        sigma = np.diag(sigma)
        all_user_predicted_ratings = np.dot(np.dot(U, sigma), Vt) + user_ratings_mean.reshape(-1, 1)
        predictions_df = pd.DataFrame(all_user_predicted_ratings, columns=R_df.columns)

        predictions_df = predictions_df.loc[[0]]
        predictions_df = predictions_df[predictions_df.columns[predictions_df.ix[predictions_df.last_valid_index()].argsort()]]
        print (predictions_df)

        outputCount = 15
        f = df.shape[1] - 1
        print(publics_df)
        publics_df.set_index('public_id', inplace=True, drop=True)
        print('index moved')
        print(publics_df)
        session1 = vk.AuthSession(6470661, 'oppasaranhae@gmail.com', password)
        # session.requests_session.keep_alive = False
        api1 = vk.API(session1)
        with pd.option_context('display.max_rows', None, 'display.max_columns', 3):
            print(publics_df)
        while outputCount != 0 and f != 2:
            f-=1
            time.sleep(0.3)
            if api1.groups.getMembers(group_id=df.columns[f], sort="id_asc", version=5.0, timeout=10)['count'] < 1000000:
                print('f')
                print(f)
                print('df.columns[f]')
                print(df.columns[f])
                print('publics_df.at[df.columns[f], public_name')
                print(publics_df.at[df.columns[f], 'public_name'])

                outputTable[publics_df.at[df.columns[f], 'public_name']] = 'vk.com/public' + str(df.columns[f])
                outputCount -= 1

        f = predictions_df.shape[1]
        outputCount = 15
        while outputCount != 0 and f != 0:
            print('lalalla')
            f-=1
            time.sleep(0.3)
            if api.groups.getMembers(group_id=df.columns[f], sort="id_asc", version=5.0, timeout=10)['count'] < 1000000:
                outputTable2[publics_df.at[df.columns[f], 'public_name']] = 'vk.com/public' + str(df.columns[f])
                outputCount -= 1


    return render_template('login.html',
        title = 'Sign In',
        form = form, message = "")

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
