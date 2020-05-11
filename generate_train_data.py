# encoding: utf-8
import csv
import time
import random
import pymysql
import datetime
from copy import deepcopy
from operator import itemgetter
from utils import get_use_months
from utils import get_previous_month

# host='10.214.163.179'
# user='dt_yc'
# password='dt_yc123'
# port=3306
# database='dt_yc'

def generate_train_data(train_ratio, test_ratio, test_fault_window):
    start = time.time()
    conn = pymysql.connect(host='10.214.163.179', user='dt_yc', password='dt_yc123', port=3306, database='dt_yc')
    cursor = conn.cursor()

    # 删除原先的训练特征表
    query = 'DROP TABLE IF EXISTS dt_yc.model_ele_train_feature'
    cursor.execute(query)
    conn.commit()

    # 建立新的训练特征表
    query = """
			CREATE TABLE dt_yc.model_ele_train_feature(
			LABEL int,
			EQU_SAFE_LEVEL_1 int(11),
			EQU_SAFE_LEVEL_2 int(11),
			EQU_SAFE_LEVEL_3 int(11),

			APPLY_LOCATION_1 int(11),
			APPLY_LOCATION_2 int(11),
			APPLY_LOCATION_3 int(11),
			APPLY_LOCATION_4 int(11),
			APPLY_LOCATION_5 int(11),
			APPLY_LOCATION_6 int(11),
			APPLY_LOCATION_7 int(11),
			APPLY_LOCATION_8 int(11),
			APPLY_LOCATION_9 int(11),
			APPLY_LOCATION_10 int(11),
			APPLY_LOCATION_11 int(11),
			APPLY_LOCATION_12 int(11),
			APPLY_LOCATION_13 int(11),
			APPLY_LOCATION_14 int(11),
			APPLY_LOCATION_15 int(11),
			APPLY_LOCATION_16 int(11),
			APPLY_LOCATION_17 int(11),
			APPLY_LOCATION_18 int(11),

			USE_MONTHS decimal(5, 0),
			EXAM_TYPE decimal(10, 0),

			SAME_SET_UNIT_FAULT_RATE_MONTH_1 decimal(10, 4),
			SAME_SET_UNIT_FAULT_RATE_MONTH_2 decimal(10, 4),
			SAME_SET_UNIT_FAULT_RATE_MONTH_3 decimal(10, 4),
			SAME_SET_UNIT_FAULT_RATE_MONTH_4 decimal(10, 4),
			SAME_SET_UNIT_FAULT_RATE_MONTH_5 decimal(10, 4),
			SAME_SET_UNIT_FAULT_RATE_MONTH_6 decimal(10, 4),

			SAME_MAKE_UNIT_FAULT_RATE_MONTH_1 decimal(10, 4),
			SAME_MAKE_UNIT_FAULT_RATE_MONTH_2 decimal(10, 4),
			SAME_MAKE_UNIT_FAULT_RATE_MONTH_3 decimal(10, 4),
			SAME_MAKE_UNIT_FAULT_RATE_MONTH_4 decimal(10, 4),
			SAME_MAKE_UNIT_FAULT_RATE_MONTH_5 decimal(10, 4),
			SAME_MAKE_UNIT_FAULT_RATE_MONTH_6 decimal(10, 4),

			FAULT_NUMBER_MONTH_1 decimal(5, 0),
			FAULT_NUMBER_MONTH_2 decimal(5, 0),
			FAULT_NUMBER_MONTH_3 decimal(5, 0),
			FAULT_NUMBER_MONTH_4 decimal(5, 0),
			FAULT_NUMBER_MONTH_5 decimal(5, 0),
			FAULT_NUMBER_MONTH_6 decimal(5, 0),

			SAME_INSP_ORG_FAULT_RATE_MONTH_1 decimal(10, 4),
			SAME_INSP_ORG_FAULT_RATE_MONTH_2 decimal(10, 4),
			SAME_INSP_ORG_FAULT_RATE_MONTH_3 decimal(10, 4),
			SAME_INSP_ORG_FAULT_RATE_MONTH_4 decimal(10, 4),
			SAME_INSP_ORG_FAULT_RATE_MONTH_5 decimal(10, 4),
			SAME_INSP_ORG_FAULT_RATE_MONTH_6 decimal(10, 4),

			SAME_USE_UNIT_FAULT_RATE_MONTH_1 decimal(10, 4),
			SAME_USE_UNIT_FAULT_RATE_MONTH_2 decimal(10, 4),
			SAME_USE_UNIT_FAULT_RATE_MONTH_3 decimal(10, 4),
			SAME_USE_UNIT_FAULT_RATE_MONTH_4 decimal(10, 4),
			SAME_USE_UNIT_FAULT_RATE_MONTH_5 decimal(10, 4),
			SAME_USE_UNIT_FAULT_RATE_MONTH_6 decimal(10, 4),

			SAME_WB_UNIT_FAULT_RATE_MONTH_1 decimal(10, 4),
			SAME_WB_UNIT_FAULT_RATE_MONTH_2 decimal(10, 4),
			SAME_WB_UNIT_FAULT_RATE_MONTH_3 decimal(10, 4),
			SAME_WB_UNIT_FAULT_RATE_MONTH_4 decimal(10, 4),
			SAME_WB_UNIT_FAULT_RATE_MONTH_5 decimal(10, 4),
			SAME_WB_UNIT_FAULT_RATE_MONTH_6 decimal(10, 4),

            DATE date
			)
			"""
    cursor.execute(query)
    conn.commit()

    # 统计各单位电梯数量
	# 使用单位电梯数量
    query1 = 'DROP TABLE IF EXISTS dt_yc.model_use_unit_ele_num'
    query2 = """
            CREATE TABLE dt_yc.model_use_unit_ele_num
            SELECT USE_UNIT_CODE, COUNT(*) ELE_NUM
            FROM dt_yc.ele_info
            WHERE USE_UNIT_CODE != '-'
            and USE_UNIT_CODE != '不详'
            GROUP BY USE_UNIT_CODE
            """		
    query3 = 'CREATE INDEX index_use_unit_code ON dt_yc.model_use_unit_ele_num(`USE_UNIT_CODE`)'
    cursor.execute(query1)
    conn.commit()
    cursor.execute(query2)
    conn.commit()	
    cursor.execute(query3)
    conn.commit()

    # 制造单位电梯数量
    query1 = 'DROP TABLE IF EXISTS dt_yc.model_make_unit_ele_num'
    query2 = """
            CREATE TABLE dt_yc.model_make_unit_ele_num
            SELECT MAKE_UNIT_NAME, COUNT(*) ELE_NUM
            FROM dt_yc.ele_info
            WHERE MAKE_UNIT_NAME is not null
            and MAKE_UNIT_NAME != '-'
            and MAKE_UNIT_NAME != '/'
            GROUP BY MAKE_UNIT_NAME
            """
    query3 = 'CREATE INDEX index_make_unit_name ON dt_yc.model_make_unit_ele_num(`MAKE_UNIT_NAME`)'
    cursor.execute(query1)
    conn.commit()
    cursor.execute(query2)
    conn.commit()	
    cursor.execute(query3)
    conn.commit()

    # 安装单位电梯数量
    query1 = 'DROP TABLE IF EXISTS dt_yc.model_set_unit_ele_num'
    query2 = """
            CREATE TABLE dt_yc.model_set_unit_ele_num
            SELECT SET_UNIT_NAME, COUNT(*) ELE_NUM
            FROM dt_yc.ele_info
            WHERE SET_UNIT_NAME is not null
            and SET_UNIT_NAME != '-'
            and SET_UNIT_NAME != '/'
            GROUP BY SET_UNIT_NAME
            """
    query3 = 'CREATE INDEX index_set_unit_name ON dt_yc.model_set_unit_ele_num(`SET_UNIT_NAME`)'
    cursor.execute(query1)
    conn.commit()
    cursor.execute(query2)
    conn.commit()	
    cursor.execute(query3)
    conn.commit()

    # 检验机构电梯数量
    query1 = 'DROP TABLE IF EXISTS dt_yc.model_insp_org_ele_num'
    query2 = """
            CREATE TABLE dt_yc.model_insp_org_ele_num
            SELECT INSP_ORG_NAME, COUNT(*) ELE_NUM
            FROM dt_yc.ele_info
            WHERE INSP_ORG_NAME is not null
            and INSP_ORG_NAME != '-'
            and INSP_ORG_NAME != '/'
            GROUP BY INSP_ORG_NAME
            """
    query3 = 'CREATE INDEX index_insp_org_name ON dt_yc.model_insp_org_ele_num(`INSP_ORG_NAME`)'
    cursor.execute(query1)
    conn.commit()
    cursor.execute(query2)
    conn.commit()	
    cursor.execute(query3)
    conn.commit()

    # 维保单位电梯数量
    query1 = 'DROP TABLE IF EXISTS dt_yc.model_wb_unit_ele_num'
    query2 = """
            CREATE TABLE dt_yc.model_wb_unit_ele_num
            SELECT WB_UNIT_NAME, COUNT(*) ELE_NUM
            FROM dt_yc.ele_info
            WHERE WB_UNIT_NAME is not null
            and WB_UNIT_NAME != '/'
            and WB_UNIT_NAME != '*'
            and WB_UNIT_NAME != '0'
            and WB_UNIT_NAME != '//'
            and WB_UNIT_NAME != '-'
            and WB_UNIT_NAME != '--'
            and WB_UNIT_NAME != '**'
            and WB_UNIT_NAME != '1'
            GROUP BY WB_UNIT_NAME
            """
    query3 = 'CREATE INDEX index_wb_unit_name ON dt_yc.model_wb_unit_ele_num(`WB_UNIT_NAME`)'
    cursor.execute(query1)
    conn.commit()
    cursor.execute(query2)
    conn.commit()	
    cursor.execute(query3)
    conn.commit()

    # 读出各单位电梯数量
    # 读出dt_yc.model_use_unit_ele_num中的数据并存入use_unit_ele_num中
    query = 'SELECT * FROM dt_yc.model_use_unit_ele_num'
    cursor.execute(query)
    rows = cursor.fetchall()
    use_unit_ele_num = {}
    for row in rows:
        use_unit_ele_num[row[0]] = row[1]

    # 读出dt_yc.model_make_unit_ele_num中的数据并存入make_unit_ele_num中
    query = 'SELECT * FROM dt_yc.model_make_unit_ele_num'
    cursor.execute(query)
    rows = cursor.fetchall()
    make_unit_ele_num = {}
    for row in rows:
        make_unit_ele_num[row[0]] = row[1]

    # 读出dt_yc.model_set_unit_ele_num中的数据并存入set_unit_ele_num中
    query = 'SELECT * FROM dt_yc.model_set_unit_ele_num'
    cursor.execute(query)
    rows = cursor.fetchall()
    set_unit_ele_num = {}
    for row in rows:
        set_unit_ele_num[row[0]] = row[1]

    # 读出dt_yc.model_wb_unit_ele_num中的数据并存入wb_unit_ele_num中
    query = 'SELECT * FROM dt_yc.model_wb_unit_ele_num'
    cursor.execute(query)
    rows = cursor.fetchall()
    wb_unit_ele_num = {}
    for row in rows:
        wb_unit_ele_num[row[0]] = row[1]

    # 读出dt_yc.model_insp_org_ele_num中的数据并存入insp_org_ele_num中
    query = 'SELECT * FROM dt_yc.model_insp_org_ele_num'
    cursor.execute(query)
    rows = cursor.fetchall()
    insp_org_ele_num = {}
    for row in rows:
        insp_org_ele_num[row[0]] = row[1]

    # 读出所有电梯特征
    query = """
            SELECT
            equ_safe_level,
            apply_location,
            use_start_date,
            exam_type,
            use_unit_code,
            make_unit_name,
            set_unit_name,
            insp_org_name,
            wb_unit_name,
            tranid
            FROM dt_yc.ele_info
            WHERE use_start_date is not null
            AND use_unit_code is not null
            AND use_unit_code != '-'
            AND use_unit_code != '不详'
            AND make_unit_name is not null
            AND make_unit_name != '-'
            AND make_unit_name != '/'
            AND set_unit_name is not null
            AND set_unit_name != '-'
            AND set_unit_name != '/'
            AND insp_org_name is not null
            AND insp_org_name != '-'
            AND insp_org_name != '/'
            AND wb_unit_name is not null
            AND wb_unit_name != '/'
            AND wb_unit_name != '*'
            AND wb_unit_name != '0'
            AND wb_unit_name != '//'
            AND wb_unit_name != '-'
            AND wb_unit_name != '--'
            AND wb_unit_name != '**'
            AND wb_unit_name != '1'
            """
    cursor.execute(query)
    rows = cursor.fetchall()
    ele_list = deepcopy(rows)

    # 读出2019-08-01后的故障数据
    query = """
            SELECT
            equ_safe_level,
            apply_location,
            use_start_date,
            exam_type,
            a.use_unit_code,
            a.make_unit_name,
            a.set_unit_name,
            a.insp_org_name,
            a.wb_unit_name,
            form_create_time,
            a.tranid
            FROM dt_yc.ele_info a, dt_yc.zt_dt_fault b
            WHERE form_create_time >= '2019-08-01'
            AND use_start_date is not null
            AND a.tranid = b.tranid
            AND a.use_unit_code is not null
            AND a.use_unit_code != '-'
            AND a.use_unit_code != '不详'
            AND a.make_unit_name is not null
            AND a.make_unit_name != '-'
            AND a.make_unit_name != '/'
            AND a.set_unit_name is not null
            AND a.set_unit_name != '-'
            AND a.set_unit_name != '/'
            AND a.insp_org_name is not null
            AND a.insp_org_name != '-'
            AND a.insp_org_name != '/'
            AND a.wb_unit_name is not null
            AND a.wb_unit_name != '/'
            AND a.wb_unit_name != '*'
            AND a.wb_unit_name != '0'
            AND a.wb_unit_name != '//'
            AND a.wb_unit_name != '-'
            AND a.wb_unit_name != '--'
            AND a.wb_unit_name != '**'
            AND a.wb_unit_name != '1'
            """
    cursor.execute(query)
    rows = list(cursor.fetchall())

    # 将2019-08-01后的所有故障数据按照form_create_time字段排序，其位置为9
    rows.sort(key=itemgetter(9))

    # 读出所有故障数据
    query = """
            SELECT
            equ_safe_level,
            apply_location,
            use_start_date,
            exam_type,
            a.use_unit_code,
            a.make_unit_name,
            a.set_unit_name,
            a.insp_org_name,
            a.wb_unit_name,
            form_create_time,
            a.tranid
            FROM dt_yc.ele_info a, dt_yc.zt_dt_fault b
            WHERE use_start_date is not null
            AND a.tranid = b.tranid
            AND a.use_unit_code is not null
            AND a.use_unit_code != '-'
            AND a.use_unit_code != '不详'
            AND a.make_unit_name is not null
            AND a.make_unit_name != '-'
            AND a.make_unit_name != '/'
            AND a.set_unit_name is not null
            AND a.set_unit_name != '-'
            AND a.set_unit_name != '/'
            AND a.insp_org_name is not null
            AND a.insp_org_name != '-'
            AND a.insp_org_name != '/'
            AND a.wb_unit_name is not null
            AND a.wb_unit_name != '/'
            AND a.wb_unit_name != '*'
            AND a.wb_unit_name != '0'
            AND a.wb_unit_name != '//'
            AND a.wb_unit_name != '-'
            AND a.wb_unit_name != '--'
            AND a.wb_unit_name != '**'
            AND a.wb_unit_name != '1'
            """
    cursor.execute(query)
    fault_ele_list = list(cursor.fetchall())
    fault_ele_num = len(fault_ele_list)
    print('故障数据读取完毕')

    # 将所有故障数据tranid与form_create_time存入fault_ele_time_list中
    fault_ele_time_list = []
    for fault_ele in fault_ele_list:
        temp = [fault_ele[10], str(fault_ele[9])[0:10]]
        fault_ele_time_list.append(temp)

    # 插入电梯特征（数据库中只有2019-01-01之后的故障数据，所以生成训练数据时只能是用2019-08-01之后的故障数据)
    # 最后test_fault_window个故障数据用于生成测试数据（正负样本比例为1:test_ratio），其余数据用于生成训练数据（正负样本比例为1:train_ratio）
    cnt = 0
    fault_ele_count = 0
    ratio = train_ratio
    for row in rows:
        fault_ele_count = fault_ele_count + 1
        if fault_ele_num - fault_ele_count < test_fault_window:
            ratio = test_ratio

        equ_safe_level = row[0]
        apply_location = row[1]
        use_start_date = row[2]
        exam_type = row[3]

        use_unit_code = row[4]
        make_unit_name = row[5]
        set_unit_name = row[6]
        insp_org_name = row[7]
        wb_unit_name = row[8]

        form_create_time = str(row[9])[0:10]
        tranid = row[10]

        equ1 = 0
        equ2 = 0
        equ3 = 0
        if equ_safe_level == '高风险':
            equ1 = 1
        elif equ_safe_level == '中风险':
            equ2 = 1
        elif equ_safe_level == '低风险':
            equ3 = 1

        apply1 = 0
        apply2 = 0
        apply3 = 0
        apply4 = 0
        apply5 = 0
        apply6 = 0
        apply7 = 0
        apply8 = 0
        apply9 = 0
        apply10 = 0
        apply11 = 0
        apply12 = 0
        apply13 = 0
        apply14 = 0
        apply15 = 0
        apply16 = 0
        apply17 = 0
        apply18 = 0
        if apply_location == '其他场所':
            apply1 = 1
        elif apply_location == '商场':
            apply2 = 1
        elif apply_location == '宾馆':
            apply3 = 1
        elif apply_location == '餐饮场所':
            apply4 = 1
        elif apply_location == '医疗机构':
            apply5 = 1
        elif apply_location == '学校':
            apply6 = 1
        elif apply_location == '养老机构':
            apply7 = 1
        elif apply_location == '幼儿园':
            apply8 = 1
        elif apply_location == '展览馆':
            apply9 = 1
        elif apply_location == '车站':
            apply10 = 1
        elif apply_location == '公园':
            apply11 = 1
        elif apply_location == '公共浴池':
            apply12 = 1
        elif apply_location == '客运码头':
            apply13 = 1
        elif apply_location == '机场':
            apply14 = 1
        elif apply_location == '儿童活动中心':
            apply15 = 1
        elif apply_location == '影剧院':
            apply16 = 1
        elif apply_location == '图书馆':
            apply17 = 1
        elif apply_location == '体育场馆':
            apply18 = 1

        start_date = str(use_start_date)

        # 计算正样本故障数与故障率数据
        use_months = get_use_months(form_create_time, start_date)

        set_fault = [0, 0, 0, 0, 0, 0]
        make_fault = [0, 0, 0, 0, 0, 0]
        insp_fault = [0, 0, 0, 0, 0, 0]
        use_fault = [0, 0, 0, 0, 0, 0]
        wb_fault = [0, 0, 0, 0, 0, 0]
        fault_num = [0, 0, 0, 0, 0, 0]

        for fault_ele in fault_ele_list:
            use_unit_code_fault = fault_ele[4]
            make_unit_name_fault = fault_ele[5]
            set_unit_name_fault = fault_ele[6]
            insp_org_name_fault = fault_ele[7]
            wb_unit_name_fault = fault_ele[8]

            form_create_time_fault = str(fault_ele[9])[0:10]
            tranid_fault = fault_ele[10]

            # 生成训练样本时不统计该日故障数据（为了与预测特征统一）
            if form_create_time == form_create_time_fault:
                continue

            months_diff = get_use_months(form_create_time, form_create_time_fault)
            if months_diff > 6 or months_diff < 1:
                continue

            if tranid == tranid_fault:
                fault_num[months_diff-1] += 1
                use_fault[months_diff-1] += 1
                make_fault[months_diff-1] += 1
                set_fault[months_diff-1] += 1
                insp_fault[months_diff-1] += 1
                wb_fault[months_diff-1] += 1
                continue
            if use_unit_code == use_unit_code_fault:
                use_fault[months_diff-1] += 1
            if make_unit_name == make_unit_name_fault:
                make_fault[months_diff-1] += 1
            if set_unit_name == set_unit_name_fault:
                set_fault[months_diff-1] += 1
            if insp_org_name == insp_org_name_fault:
                insp_fault[months_diff-1] += 1
            if wb_unit_name == wb_unit_name_fault:
                wb_fault[months_diff-1] += 1

        _set = []
        make = []
        insp = []
        use = []
        wb = []

        for i in range(6):
            temp = round(set_fault[i] / set_unit_ele_num.get(set_unit_name, -1), 4)
            _set.append(temp if temp >= 0 else -1)
            temp = round(make_fault[i] / make_unit_ele_num.get(make_unit_name, -1), 4)
            make.append(temp if temp >= 0 else -1)
            temp = round(insp_fault[i] / insp_org_ele_num.get(insp_org_name, -1), 4)
            insp.append(temp if temp >= 0 else -1)
            temp = round(use_fault[i] / use_unit_ele_num.get(use_unit_code, -1), 4)
            use.append(temp if temp >= 0 else -1)
            temp = round(wb_fault[i] / wb_unit_ele_num.get(wb_unit_name, -1), 4)
            wb.append(temp if temp >= 0 else -1)

        # 插入正样本
        insert = "INSERT INTO dt_yc.model_ele_train_feature VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, DATE_FORMAT(%s, '%%Y-%%m-%%d'))"
        val = [1, equ1, equ2, equ3, apply1, apply2, apply3, apply4, apply5, apply6, apply7, apply8, apply9, 
            apply10, apply11, apply12, apply13, apply14, apply15, apply16, apply17, apply18, use_months,
            exam_type, _set[0], _set[1], _set[2], _set[3], _set[4], _set[5], make[0], make[1], make[2], make[3], make[4], make[5],
            fault_num[0], fault_num[1], fault_num[2], fault_num[3], fault_num[4], fault_num[5], insp[0], insp[1], insp[2], insp[3],
            insp[4], insp[5], use[0], use[1], use[2], use[3], use[4], use[5], wb[0], wb[1], wb[2], wb[3], wb[4], wb[5], form_create_time]
        cursor.execute(insert, val)
        conn.commit()
        cnt = cnt + 1
        if cnt % 1000 == 0:
            print('插入样本中，目前已完成' + str(cnt) + '条')

        # 在所有电梯中随机选取ratio个作为负样本
        for i in range(ratio):
            while True:
                nf_ele = random.choice(ele_list)
                if ([nf_ele[9], form_create_time] not in fault_ele_time_list) and (str(nf_ele[2]) <= form_create_time):
                    # 该电梯在form_create_time未发生故障，且该电梯投用日期在form_create_time前，退出循环
                    break

            nf_equ_safe_level = nf_ele[0]
            nf_apply_location = nf_ele[1]
            nf_use_start_date = nf_ele[2]
            nf_exam_type = nf_ele[3]

            nf_use_unit_code = nf_ele[4]
            nf_make_unit_name = nf_ele[5]
            nf_set_unit_name = nf_ele[6]
            nf_insp_org_name = nf_ele[7]
            nf_wb_unit_name = nf_ele[8]
            nf_tranid = nf_ele[9]

            nf_equ1 = 0
            nf_equ2 = 0
            nf_equ3 = 0
            if nf_equ_safe_level == '高风险':
                nf_equ1 = 1
            elif nf_equ_safe_level == '中风险':
                nf_equ2 = 1
            elif nf_equ_safe_level == '低风险':
                nf_equ3 = 1

            nf_apply1 = 0
            nf_apply2 = 0
            nf_apply3 = 0
            nf_apply4 = 0
            nf_apply5 = 0
            nf_apply6 = 0
            nf_apply7 = 0
            nf_apply8 = 0
            nf_apply9 = 0
            nf_apply10 = 0
            nf_apply11 = 0
            nf_apply12 = 0
            nf_apply13 = 0
            nf_apply14 = 0
            nf_apply15 = 0
            nf_apply16 = 0
            nf_apply17 = 0
            nf_apply18 = 0
            if nf_apply_location == '其他场所':
                nf_apply1 = 1
            elif nf_apply_location == '商场':
                nf_apply2 = 1
            elif nf_apply_location == '宾馆':
                nf_apply3 = 1
            elif nf_apply_location == '餐饮场所':
                nf_apply4 = 1
            elif nf_apply_location == '医疗机构':
                nf_apply5 = 1
            elif nf_apply_location == '学校':
                nf_apply6 = 1
            elif nf_apply_location == '养老机构':
                nf_apply7 = 1
            elif nf_apply_location == '幼儿园':
                nf_apply8 = 1
            elif nf_apply_location == '展览馆':
                nf_apply9 = 1
            elif nf_apply_location == '车站':
                nf_apply10 = 1
            elif nf_apply_location == '公园':
                nf_apply11 = 1
            elif nf_apply_location == '公共浴池':
                nf_apply12 = 1
            elif nf_apply_location == '客运码头':
                nf_apply13 = 1
            elif nf_apply_location == '机场':
                nf_apply14 = 1
            elif nf_apply_location == '儿童活动中心':
                nf_apply15 = 1
            elif nf_apply_location == '影剧院':
                nf_apply16 = 1
            elif nf_apply_location == '图书馆':
                nf_apply17 = 1
            elif nf_apply_location == '体育场馆':
                nf_apply18 = 1

            nf_start_date = str(nf_use_start_date)

            # 计算负样本故障数与故障率数据
            nf_use_months = get_use_months(form_create_time, nf_start_date)

            nf_set_fault = [0, 0, 0, 0, 0, 0]
            nf_make_fault = [0, 0, 0, 0, 0, 0]
            nf_insp_fault = [0, 0, 0, 0, 0, 0]
            nf_use_fault = [0, 0, 0, 0, 0, 0]
            nf_wb_fault = [0, 0, 0, 0, 0, 0]
            nf_fault_num = [0, 0, 0, 0, 0, 0]

            for fault_ele in fault_ele_list:
                use_unit_code_fault = fault_ele[4]
                make_unit_name_fault = fault_ele[5]
                set_unit_name_fault = fault_ele[6]
                insp_org_name_fault = fault_ele[7]
                wb_unit_name_fault = fault_ele[8]

                form_create_time_fault = str(fault_ele[9])[0:10]
                tranid_fault = fault_ele[10]

                # 生成训练样本时不统计该日故障数据（为了与预测特征统一）
                if form_create_time == form_create_time_fault:
                    continue

                months_diff = get_use_months(form_create_time, form_create_time_fault)
                if months_diff > 6 or months_diff < 1:
                    continue

                if nf_tranid == tranid_fault:
                    nf_fault_num[months_diff-1] += 1
                    nf_use_fault[months_diff-1] += 1
                    nf_make_fault[months_diff-1] += 1
                    nf_set_fault[months_diff-1] += 1
                    nf_insp_fault[months_diff-1] += 1
                    nf_wb_fault[months_diff-1] += 1
                    continue
                if nf_use_unit_code == use_unit_code_fault:
                    nf_use_fault[months_diff-1] += 1
                if nf_make_unit_name == make_unit_name_fault:
                    nf_make_fault[months_diff-1] += 1
                if nf_set_unit_name == set_unit_name_fault:
                    nf_set_fault[months_diff-1] += 1
                if nf_insp_org_name == insp_org_name_fault:
                    nf_insp_fault[months_diff-1] += 1
                if nf_wb_unit_name == wb_unit_name_fault:
                    nf_wb_fault[months_diff-1] += 1

            nf_set = []
            nf_make = []
            nf_insp = []
            nf_use = []
            nf_wb = []

            for i in range(6):
                temp = round(nf_set_fault[i] / set_unit_ele_num.get(nf_set_unit_name, -1), 4)
                nf_set.append(temp if temp >= 0 else -1)
                temp = round(nf_make_fault[i] / make_unit_ele_num.get(nf_make_unit_name, -1), 4)
                nf_make.append(temp if temp >= 0 else -1)
                temp = round(nf_insp_fault[i] / insp_org_ele_num.get(nf_insp_org_name, -1), 4)
                nf_insp.append(temp if temp >= 0 else -1)
                temp = round(nf_use_fault[i] / use_unit_ele_num.get(nf_use_unit_code, -1), 4)
                nf_use.append(temp if temp >= 0 else -1)
                temp = round(nf_wb_fault[i] / wb_unit_ele_num.get(nf_wb_unit_name, -1), 4)
                nf_wb.append(temp if temp >= 0 else -1)

            # 插入负样本
            insert = "INSERT INTO dt_yc.model_ele_train_feature VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, DATE_FORMAT(%s, '%%Y-%%m-%%d'))"
            val = [0, nf_equ1, nf_equ2, nf_equ3, nf_apply1, nf_apply2, nf_apply3, nf_apply4, nf_apply5, nf_apply6, nf_apply7, nf_apply8, nf_apply9, 
                nf_apply10, nf_apply11, nf_apply12, nf_apply13, nf_apply14, nf_apply15, nf_apply16, nf_apply17, nf_apply18, nf_use_months,
                nf_exam_type, nf_set[0], nf_set[1], nf_set[2], nf_set[3], nf_set[4], nf_set[5], nf_make[0], nf_make[1], nf_make[2], nf_make[3], nf_make[4], nf_make[5],
                nf_fault_num[0], nf_fault_num[1], nf_fault_num[2], nf_fault_num[3], nf_fault_num[4], nf_fault_num[5], nf_insp[0], nf_insp[1], nf_insp[2], nf_insp[3],
                nf_insp[4], nf_insp[5], nf_use[0], nf_use[1], nf_use[2], nf_use[3], nf_use[4], nf_use[5], nf_wb[0], nf_wb[1], nf_wb[2], nf_wb[3], nf_wb[4], nf_wb[5], form_create_time]
            cursor.execute(insert, val)
            conn.commit()
            cnt = cnt + 1
            if cnt % 1000 == 0:
                print('插入样本中，目前已完成' + str(cnt) + '条')

    cursor.close()
    conn.close()
    elapsed = (time.time() - start)
    print('训练特征提取完毕，总运行时间：' + str(round(elapsed/60, 2)) + '分钟')
    print('------------------------------------------------------')
    print('------------------------------------------------------')
