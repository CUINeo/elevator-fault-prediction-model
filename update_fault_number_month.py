# encoding: utf-8
import sys
import time
import pymysql
from datetime import datetime
from utils import get_current_date
from utils import get_previous_diff_date

# host='10.214.163.179'
# user='dt_yc'
# password='dt_yc123'
# port=3306
# database='dt_yc'

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
    current_date = get_current_date()
    if current_date is not None:
        end_date = get_previous_diff_date(current_date, -1)

        for month_num in range(1, 7):
            start_date = get_previous_diff_date(end_date, 30)
            query = """
                    SELECT tranid, count(*) fault_num
					FROM dt_yc.zt_dt_fault
					WHERE form_create_time >= DATE_FORMAT(%s,'%%Y-%%m-%%d')
					and form_create_time < DATE_FORMAT(%s,'%%Y-%%m-%%d')
                    and use_unit_code is not null
                    and use_unit_code != '-'
                    and use_unit_code != '不详'
                    and make_unit_name is not null
                    and make_unit_name != '-'
                    and make_unit_name != '/'
                    and set_unit_name is not null
                    and set_unit_name != '-'
                    and set_unit_name != '/'
                    and insp_org_name is not null
                    and insp_org_name != '-'
                    and insp_org_name != '/'
                    and wb_unit_name is not null
                    and wb_unit_name != '/'
                    and wb_unit_name != '*'
                    and wb_unit_name != '0'
                    and wb_unit_name != '//'
                    and wb_unit_name != '-'
                    and wb_unit_name != '--'
                    and wb_unit_name != '**'
                    and wb_unit_name != '1'
					GROUP BY tranid
                    """
            var = [start_date, end_date]
            cursor.execute(query, var)
            rows = cursor.fetchall()
            
            for row in rows:
                tranid = row[0]
                fault_num = row[1]
                insert = 'INSERT INTO dt_yc.model_fault_num VALUES(%s, %s, %s)'
                var = [tranid, month_num, fault_num]
                cursor.execute(insert, var)
                conn.commit()

            end_date = start_date

    cursor.close()
    conn.close()
    elapsed = (time.time() - start)
    print('电梯六个月内故障数量统计完毕，总运行时间：' + str(round(elapsed/60, 2)) + '分钟')
    print('------------------------------------------------------')
    print('------------------------------------------------------')
