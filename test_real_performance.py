import csv
import pymysql
import numpy as np
from joblib import load
from utils import get_previous_diff_date

# host='10.214.163.179'
# user='dt_yc'
# password='dt_yc123'
# port=3306
# database='dt_yc'

def test_real_performance(method_name, date, window=1):
    csv_file_name = 'features/' + date + '-feature.csv'
    csv_file = open(csv_file_name, 'r')
    reader = csv.reader(csv_file)
    rows = list(reader)

    predict_X = []
    tranid_set = []
    for row in rows:
        tranid_set.append(row[0])
        predict_X.append(row[1:])

    predict_X = [[float(i) if i.replace('.', '', 1).isdigit() else 0 for i in j] for j in predict_X]
    predict_X = np.asarray(predict_X, dtype=np.float32)

    joblib_name = method_name + '.joblib'
    clf = load(joblib_name)
    proba = clf.predict_proba(predict_X)
    # res = clf.predict(predict_X)

    pos_list = []
    pos = 0
    for p in proba:
        if p[1] >= 0.5:
            pos_list.append(pos)
        pos = pos + 1

    positive_tranid_set = []
    for pos in pos_list:
        positive_tranid_set.append(tranid_set[pos])
    print('预测过程完成，得到正样本集合')
    
    future_date = get_previous_diff_date(date, -window)

    conn = pymysql.connect(host='10.214.163.179', user='dt_yc', password='dt_yc123', port=3306, database='dt_yc')
    cursor = conn.cursor()

    # 从数据库中得到date到future_date时间段内发生故障电梯的tranid集合
    query = """
            SELECT DISTINCT tranid FROM dt_yc.zt_dt_fault
            WHERE form_create_time >= DATE_FORMAT(%s, '%%Y-%%m-%%d')
            AND form_create_time <= DATE_FORMAT(%s, '%%Y-%%m-%%d')
            """
    val = [date, future_date]
    cursor.execute(query, val)
    temp_fault_sets = list(cursor.fetchall())

    fault_sets = []
    for temp_fault_set in temp_fault_sets:
        fault_sets.append(temp_fault_set[0])
    print('成功从数据库中读取得到故障电梯集合')

    tp = tn = fp = fn = 0
    for tranid in tranid_set:
        if tranid in positive_tranid_set:
            if tranid in fault_sets:
                tp = tp + 1
            else:
                fp = fp + 1
        else:
            if tranid in fault_sets:
                fn = fn + 1
            else:
                tn = tn + 1
    
    precision = float(tp / (tp + fp))
    recall = float(tp / (tp + fn))
    # f1_score = float(2 * (recall * precision) / (recall + precision))

    if method_name == 'rf':
        print('Random Forest:')
    elif method_name == 'dt':
        print('Decision Tree:')
    elif method_name == 'knn':
        print('KNN:')

    print('True Positive: ' + str(tp))
    print('False Positive: ' + str(fp))
    print('False Negative: ' + str(fn))
    print('True Negative: ' + str(tn))
    print('Confusion matrix:')
    print([[tp, fn], [fp, tn]])
    print('Precision: ' + str(precision))
    print('Recall: ' + str(recall))
    # print('F1 Score: ' + str(f1_score))

    print('------------------------------------------------------')
    print('------------------------------------------------------')

    cursor.close()
    conn.close()

date = '2020-03-20'
test_real_performance('rf', date)
# test_real_performance('dt', date)
# test_real_performance('knn', date)
