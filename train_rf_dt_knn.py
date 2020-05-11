# encoding: utf-8
import csv
import time
import random
import pymysql
import numpy as np
from joblib import dump
from sklearn import svm
from copy import deepcopy
from utils import get_current_date
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix

# host='10.214.163.179'
# user='dt_yc'
# password='dt_yc123'
# port=3306
# database='dt_yc'

# 定义常数
train_random_forest = 1
train_decision_tree = 2
train_knn = 3

# 使用决策树、随机森林、knn进行训练，并写回结果
def train_rf_dt_knn(test_set_len):
    conn = pymysql.connect(host='10.214.163.179', user='dt_yc', password='dt_yc123', port=3306, database='dt_yc')
    cursor = conn.cursor()

    query = "select * from dt_yc.model_ele_train_feature"
    cursor.execute(query)
    rows = list(deepcopy(cursor.fetchall()))

    for i in range(len(rows)):
        rows[i] = list(rows[i])

    # 将数据库中的date转换为字符串
    for row in rows:
        row[60] = str(row[60])

    whole_set_len = len(rows)
    cnt = 0

    train_X = []
    test_X = []
    train_Y = []
    test_Y = []
    for row in rows:
        cnt = cnt + 1
        if whole_set_len - cnt >= test_set_len:
            train_X.append(row[1:60])
            train_Y.append(row[0])
        else:
            test_X.append(row[1:60])
            test_Y.append(row[0])

    train_X = np.asarray(train_X, dtype=np.float32)
    train_Y = np.asarray(train_Y, dtype=np.float32)
    test_X = np.asarray(test_X, dtype=np.float32)
    test_Y = np.asarray(test_Y, dtype=np.float32)

    train_X = np.nan_to_num(train_X)
    train_Y = np.nan_to_num(train_Y)
    test_X = np.nan_to_num(test_X)
    test_Y = np.nan_to_num(test_Y)

    method = train_random_forest
    # 训练
    clf = RandomForestClassifier(max_depth=2, random_state=0, class_weight={0:0.02, 1:0.98})
    clf.fit(train_X, train_Y)
    # 验证与测试
    print('Random Forest: train score: ' + str(clf.score(train_X, train_Y)))
    print('Random Forest: test score: ' + str(clf.score(test_X, test_Y)))
    pred_test_Y = clf.predict(test_X)
    print(confusion_matrix(test_Y, pred_test_Y))
    print(classification_report(test_Y, pred_test_Y))
    performance(method, confusion_matrix(test_Y, pred_test_Y))
    dump(clf, 'rf.joblib')

    method = train_decision_tree
    # 训练
    clf = DecisionTreeClassifier(class_weight={0:0.02, 1:0.98})
    clf.fit(train_X, train_Y)
    # 验证与测试
    print('Decision Tree: train score: ' + str(clf.score(train_X, train_Y)))
    print('Decision Tree: test score: ' + str(clf.score(test_X, test_Y)))
    pred_test_Y = clf.predict(test_X)
    print(confusion_matrix(test_Y, pred_test_Y))
    print(classification_report(test_Y, pred_test_Y))
    performance(method, confusion_matrix(test_Y, pred_test_Y))
    dump(clf, 'dt.joblib')

    method = train_knn
    # 训练
    knn = KNeighborsClassifier()
    knn.fit(train_X, train_Y)
    # 验证与测试
    print('KNN: train score: ' + str(knn.score(train_X, train_Y)))
    print('KNN: test score: ' + str(knn.score(test_X, test_Y)))
    pred_test_Y = knn.predict(test_X)
    print(confusion_matrix(test_Y, pred_test_Y))
    print(classification_report(test_Y, pred_test_Y))
    performance(method, confusion_matrix(test_Y, pred_test_Y))
    dump(knn, 'knn.joblib')

    cursor.close()
    conn.close()

    print('训练以及模型评估完成，使用模型为random forest, decision tree与knn')
    print('------------------------------------------------------')
    print('------------------------------------------------------')

def performance(method, confusion_matrix):
    conn = pymysql.connect(host='10.214.163.179', user='dt_yc', password='dt_yc123', port=3306, database='dt_yc')
    cursor = conn.cursor()

    tn, fp, fn, tp = confusion_matrix.ravel()
    precision = float(tp / (tp + fp))
    recall = float(tp / (tp + fn))
    f1_score = float(2 * (recall * precision) / (recall + precision))

    if method == train_random_forest:
        method_name = 'random forest'
    elif method == train_decision_tree:
        method_name = 'decision tree'
    elif method == train_knn:
        method_name = 'knn'
    else:
        return

    current_date = get_current_date()
    year = int(current_date[0:4])
    month = int(current_date[5:7])
    insert = "INSERT INTO dt_yc.model_train_performance VALUES(%s, %s, %s, %s, %s, %s)"
    val = [method_name, year, month, precision, recall, f1_score]

    cursor.execute(insert, val)
    conn.commit()

    cursor.close()
    conn.close()
