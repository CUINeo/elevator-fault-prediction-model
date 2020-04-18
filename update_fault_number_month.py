# encoding: utf-8
import sys
import time
import pymysql
from datetime import datetime
from utils import get_current_date

#   host='10.214.163.179'
#   user='dt_yc'
#   password='dt_yc123'
#   port=3306
#   database='dt_yc'


def update_fault_number_month():
    start = time.time()
    conn = pymysql.connect(host='10.214.163.179', user='dt_yc', password='dt_yc123',
                           port=3306, database='dt_yc')
    cursor = conn.cursor()

    # -------------------------------- 统计电梯故障数 --------------------------------
    # 删除原先的故障数量表
    query = 'DROP TABLE IF EXISTS dt_yc.model_fault_num'
    cursor.execute(query)
    conn.commit()

    # 建立新的故障数量表
    query = """
            CREATE TABLE dt_yc.model_fault_num(
            TRANID varchar(50),
            MONTH_NUM int,
            FAULT_NUM int,
            primary key(TRANID, MONTH_NUM)
            )
            """
    cursor.execute(query)
    conn.commit()

    # 为新的故障数量表创建索引
    query = 'CREATE INDEX fault_num_index ON dt_yc.model_fault_num(TRANID, MONTH_NUM)'
    cursor.execute(query)
    conn.commit()

    # 统计电梯故障数
    date = get_current_date()
    if date is not None:
        for month_num in range(1, 7):
            query = """
                    SELECT tranid, count(*) fault_num
					FROM dt_yc.zt_dt_fault
					WHERE form_create_time < DATE_ADD(DATE_FORMAT(%s,'%%Y-%%m-%%d'), INTERVAL %s MONTH)
					and form_create_time >= DATE_ADD(DATE_FORMAT(%s,'%%Y-%%m-%%d'), INTERVAL %s MONTH)
					GROUP BY tranid
                    """
            var = [date, str(1-month_num), date, str(-month_num)]
            cursor.execute(query, var)
            rows = cursor.fetchall()
            
            for row in rows:
                reg_code = row[0]
                fault_num = row[1]
                insert = 'INSERT INTO dt_yc.model_fault_num VALUES(%s, %s, %s)'
                var = [reg_code, month_num, fault_num]
                cursor.execute(insert, var)
                conn.commit()

    cursor.close()
    conn.close()
    elapsed = (time.time() - start)
    print('电梯六个月内故障数量统计完毕，总运行时间：' + str(round(elapsed/60, 2)) + '分钟')
    print('------------------------------------------------------')
    print('------------------------------------------------------')
