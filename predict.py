import csv
import time
import pymysql
import numpy as np
import xlearn as xl
from joblib import load
from utils import get_current_date

# host='10.214.163.179'
# user='dt_yc'
# password='dt_yc123'
# port=3306
# database='dt_yc'

def predict(method_type):
    csv_file_name = 'features/' + get_current_date() + '-feature.csv'
    csv_file = open(csv_file_name, 'r')
    reader = csv.reader(csv_file)
    rows = list(reader)

    cnt = 0
    predict_X = []
    tranid_set = []
    for row in rows:
        tranid_set.append(row[0])
        predict_X.append(row[1:])

        cnt = cnt + 1
        if cnt % 10000 == 0:
            print('预测特征读取中，已完成' + str(cnt) + '条')

    predict_X = [[float(i) if i.replace('.', '', 1).isdigit() else 0 for i in j] for j in predict_X]
    predict_X = np.asarray(predict_X, dtype=np.float32)

    if method_type == 'random_forest':
        clf = load('rf.joblib')
        rows = clf.predict_proba(predict_X)
        predict_Y = []
        for row in rows:
            predict_Y.append(row[1])
        write_back(tranid_set, predict_Y)
        print('预测完成，使用模型为random forest')
    elif method_type == 'decision_tree':
        clf = load('dt.joblib')
        rows = clf.predict_proba(predict_X)
        predict_Y = []
        for row in rows:
            predict_Y.append(row[1])
        write_back(tranid_set, predict_Y)
        print('预测完成，使用模型为decision tree')
    elif method_type == 'knn':
        clf = load('knn.joblib')
        rows = clf.predict_proba(predict_X)
        predict_Y = []
        for row in rows:
            predict_Y.append(row[1])
        write_back(tranid_set, predict_Y)
        print('预测完成，使用模型为knn')
    
    print('------------------------------------------------------')
    print('------------------------------------------------------')

# 将预测的结果写回数据库和csv文件中
def write_back(tranid_set, result_set):
    start = time.time()
    conn = pymysql.connect(host='10.214.163.179', user='dt_yc', password='dt_yc123', port=3306, database='dt_yc')
    cursor = conn.cursor()

    query = 'DROP TABLE IF EXISTS dt_yc.model_predictions'
    cursor.execute(query)
    conn.commit()

    query = """
            CREATE TABLE dt_yc.model_predictions(
            TRANID varchar(100),
            PROBABILITY decimal(10, 4)
            )
            """
    cursor.execute(query)
    conn.commit()

    query = 'CREATE INDEX index_predictions ON model_predictions(TRANID)'
    cursor.execute(query)
    conn.commit()

    csv_file_name = 'results/' + get_current_date() + '-result.csv'
    csv_file = open(csv_file_name, 'w', newline='')
    writer = csv.writer(csv_file)
    writer.writerow(['tranid', 'probability'])

    for i in range(0, len(tranid_set)):
        tranid = tranid_set[i]
        prob = result_set[i]

        writer.writerow([tranid, float(prob)])
        insert = 'INSERT INTO dt_yc.model_predictions VALUES(%s, %s)'
        var = [tranid, round(float(prob), 4)]
        cursor.execute(insert, var)
        conn.commit()
        if (i+1) % 10000 == 0:
            print('写回中，已完成' + str(i+1) + '条')

    cursor.close()
    conn.close()
    elapsed = (time.time() - start)
    print('预测结果写回完毕，运行时间：' + str(round(elapsed / 60, 2)) + '分钟')
