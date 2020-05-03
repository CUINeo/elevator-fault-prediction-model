# encoding: utf-8
import pymysql
import numpy as np
import matplotlib.pyplot as plt
from sklearn.manifold import TSNE
from datetime import date, datetime, timedelta

# 返回当前日期
def get_current_date():
    return datetime.today().strftime('%Y-%m-%d')

# 返回从start_date到current_date之间的月数
def get_use_months(current_date, start_date):
    current_year = int(current_date[0:4])
    current_month = int(current_date[5:7])
    current_day = int(current_date[8:10])
    start_year = int(start_date[0:4])
    start_month = int(start_date[5:7])
    start_day = int(start_date[8:10])

    current = date(current_year, current_month, current_day)
    start = date(start_year, start_month, start_day)
    diff = (current - start).days

    return int(diff / 30) + 1

# 返回day之前30天的日期集合（yyyy-mm-dd格式）
def get_previous_month(day, diff):
    ret = []
    year = int(day[0:4])
    month = int(day[5:7])
    day = int(day[8:10])

    for i in range(1, diff + 1):
        ret.append((date(year, month, day) - timedelta(i)).strftime('%Y-%m-%d'))

    return ret

# 返回day之前30天的日期（yyyy-mm-dd格式）
def get_previous_diff_date(day, diff):
    year = int(day[0:4])
    month = int(day[5:7])
    day = int(day[8:10])

    return (date(year, month, day) - timedelta(diff)).strftime('%Y-%m-%d')

# 对数据仓库进行预处理
def prepare_table():
    conn = pymysql.connect(host='10.214.163.179', user='dt_yc', password='dt_yc123', port=3306, database='dt_yc')
    cursor = conn.cursor()

    # 删除原先的电梯基本表
    drop = 'DROP TABLE IF EXISTS dt_yc.ele_info'
    cursor.execute(drop)
    conn.commit()

    # 建立新的电梯基本表
    create = "CREATE TABLE dt_yc.ele_info " \
             "SELECT * " \
             "FROM dt_dev_info" \
             "WHERE EQU_CATEGORY_L1 = '3000'"
    cursor.excute(create)
    conn.commit()
    
    cursor.close()
    conn.close()

# 使用t-sne对训练数据进行降维并使用图表显示，start与end为表的特征区间，title为标题
def visualize_train_data_tsne(start, end, title, perplexity):
    conn = pymysql.connect(host='10.214.163.179', user='dt_yc', password='dt_yc123',
                           port=3306, database='dt_yc')
    cursor = conn.cursor()

    query = 'select * from dt_yc.model_ele_train_feature'
    cursor.execute(query)
    rows = cursor.fetchall()

    print('数据库读取完成')

    colors = []
    X = []
    for row in rows:
        colors.append(row[0])
        X.append(row[start:end])
    X = np.asarray(X, dtype=np.float32)
    X = np.nan_to_num(X)

    tsne = TSNE(perplexity=perplexity)
    X_embedded = tsne.fit_transform(X)

    x_axis = []
    y_axis = []
    for row in X_embedded:
        x_axis.append(row[0])
        y_axis.append(row[1])

    plt.scatter(x_axis, y_axis, s=3, c=colors)
    plt.suptitle(title + ' (perplexity=' + str(perplexity) + ')')

    plt.show()
    print('特征可视化完成')
