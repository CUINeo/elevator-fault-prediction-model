import csv
import pymysql
from utils import get_current_date, get_previous_diff_date

# host='10.214.163.179'
# user='dt_yc'
# password='dt_yc123'
# port=3306
# database='dt_yc'

# 使用实际数据与(diff + window - 1)天前的预测数据进行比对，得出模型真实性能，存入数据库中
def get_real_performance(method_type, threshold, diff, window):
    conn = pymysql.connect(host='10.214.163.179', user='dt_yc', password='dt_yc123', port=3306, database='dt_yc')
    cursor = conn.cursor()

    current_date = get_current_date()
    prediction_date = get_previous_diff_date(current_date, diff + window - 1)
    end_date = get_previous_diff_date(prediction_date, -window)

    # 从数据库中得到start_date到end_date时间段内发生故障电梯的tranid集合
    query = """
            SELECT tranid FROM dt_yc.zt_dt_fault
            WHERE form_create_time >= DATE_FORMAT(%s, '%%Y-%%m-%%d')
            AND form_create_time <= DATE_FORMAT(%s, '%%Y-%%m-%%d')
            """
    val = [prediction_date, end_date]
    cursor.execute(query, val)
    temp_fault_sets = list(cursor.fetchall())

    fault_sets = []
    for temp_fault_set in temp_fault_sets:
        fault_sets.append(temp_fault_set[0])

    # 从csv中读取prediction_date的预测结果
    csv_file = open('results/' + str(prediction_date) + '-result.csv', 'r')
    reader = csv.reader(csv_file)
    rows = list(reader)

    tp = tn = fp = fn = 0
    for row in rows:
        if row[0] == 'tranid':
            continue
        tranid = row[0]
        prob = float(row[1])

        if prob >= threshold:
            # positive
            if tranid in fault_sets:
                # true positive
                tp += 1
            else:
                # false positive
                fp += 1
        else:
            # negative
            if tranid in fault_sets:
                # false negative
                fn += 1
            else:
                # true negative
                tn += 1
    
    precision = float(tp / (tp + fp))
    recall = float(tp / (tp + fn))
    f1_score = float(2 * (recall * precision) / (recall + precision))

    insert = "INSERT INTO dt_yc.model_real_performance VALUES(%s, DATE_FORMAT(%s, '%%Y-%%m-%%d'), %s, %s, %s)"
    val = [method_type, prediction_date, precision, recall, f1_score]
    cursor.execute(insert, val)
    conn.commit()

    print('模型真实性能计算完毕')
    print('------------------------------------------------------')
    print('------------------------------------------------------')

    cursor.close()
    conn.close()
