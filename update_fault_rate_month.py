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

def update_fault_rate_month():
	# 作为dict访问的默认值
	default = -1

	start = time.time()
	flag = start
	conn = pymysql.connect(host='10.214.163.179', user='dt_yc', password='dt_yc123', port=3306, database='dt_yc')
	cursor = conn.cursor()

	# -------------------------------- 统计电梯数量表格 --------------------------------
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

	temp = time.time()
	print('电梯数量统计完成，运行时间：' + str(temp - flag))
	flag = temp

	# -------------------------------- 统计故障电梯数量 --------------------------------
	# 删除原先错误信息表格
	query1 = 'DROP TABLE IF EXISTS dt_yc.model_use_unit_fault_num'
	query2 = 'DROP TABLE IF EXISTS dt_yc.model_make_unit_fault_num'
	query3 = 'DROP TABLE IF EXISTS dt_yc.model_set_unit_fault_num'
	query4 = 'DROP TABLE IF EXISTS dt_yc.model_insp_org_fault_num'
	query5 = 'DROP TABLE IF EXISTS dt_yc.model_wb_unit_fault_num'
	cursor.execute(query1)
	conn.commit()
	cursor.execute(query2)
	conn.commit()
	cursor.execute(query3)
	conn.commit()
	cursor.execute(query4)
	conn.commit()
	cursor.execute(query5)
	conn.commit()

	# 建立错误信息表格
	query1 = """
			CREATE TABLE dt_yc.model_use_unit_fault_num(
			USE_UNIT_CODE varchar(50),
			MONTH_NUM int,
			FAULT_NUM int
			)
			"""
	query2 = """
			CREATE TABLE dt_yc.model_make_unit_fault_num(
			MAKE_UNIT_NAME varchar(50),
			MONTH_NUM int,
			FAULT_NUM int
			)
			"""
	query3 = """
			CREATE TABLE dt_yc.model_set_unit_fault_num(
			SET_UNIT_NAME varchar(50),
			MONTH_NUM int,
			FAULT_NUM int
			)
			"""
	query4 = """
			CREATE TABLE dt_yc.model_insp_org_fault_num(
			INSP_ORG_NAME varchar(50),
			MONTH_NUM int,
			FAULT_NUM int
			)
			"""
	query5 = """
			CREATE TABLE dt_yc.model_wb_unit_fault_num(
			WB_UNIT_NAME varchar(50),
			MONTH_NUM int,
			FAULT_NUM int
			)
			"""
	cursor.execute(query1)
	conn.commit()
	cursor.execute(query2)
	conn.commit()
	cursor.execute(query3)
	conn.commit()
	cursor.execute(query4)
	conn.commit()
	cursor.execute(query5)
	conn.commit()

	# 为新建表格创建索引
	query1 = 'CREATE INDEX use_unit_fault_num_index ON dt_yc.model_use_unit_fault_num(USE_UNIT_CODE, MONTH_NUM)'
	query2 = 'CREATE INDEX make_unit_fault_num_index ON dt_yc.model_make_unit_fault_num(MAKE_UNIT_NAME, MONTH_NUM)'
	query3 = 'CREATE INDEX set_unit_fault_num_index ON dt_yc.model_set_unit_fault_num(SET_UNIT_NAME, MONTH_NUM)'
	query4 = 'CREATE INDEX insp_org_fault_num_index ON dt_yc.model_insp_org_fault_num(INSP_ORG_NAME, MONTH_NUM)'
	query5 = 'CREATE INDEX wb_unit_fault_num_index ON dt_yc.model_wb_unit_fault_num(WB_UNIT_NAME, MONTH_NUM)'
	cursor.execute(query1)
	conn.commit()
	cursor.execute(query2)
	conn.commit()
	cursor.execute(query3)
	conn.commit()
	cursor.execute(query4)
	conn.commit()
	cursor.execute(query5)
	conn.commit()

	# 统计故障电梯数量
	current_date = get_current_date()
	if current_date is not None:
		end_date = get_previous_diff_date(current_date, -1)
		for i in range(1, 7):
			start_date = get_previous_diff_date(end_date, 30)

			# 使用单位错误信息聚合
			query = """
					SELECT use_unit_code, count(*) fault_num
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
					GROUP BY use_unit_code
					"""
			var = [start_date, end_date]
			cursor.execute(query, var)
			rows = cursor.fetchall()
			for row in rows:
				use_unit_code = row[0]
				if use_unit_code is None:
					continue
				fault_num = row[1]
				insert = 'INSERT INTO dt_yc.model_use_unit_fault_num VALUES(%s, %s, %s)'
				var = [use_unit_code, i, fault_num]
				cursor.execute(insert, var)
				conn.commit()
			
			# 制造单位错误信息聚合
			query = """
					SELECT make_unit_name, count(*) fault_num
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
					GROUP BY make_unit_name
					"""
			var = [start_date, end_date]
			cursor.execute(query, var)
			rows = cursor.fetchall()
			for row in rows:
				make_unit_name = row[0]
				if make_unit_name is None:
					continue
				fault_num = row[1]
				insert = 'INSERT INTO dt_yc.model_make_unit_fault_num VALUES(%s, %s, %s)'
				var = [make_unit_name, i, fault_num]
				cursor.execute(insert, var)
				conn.commit()

			# 安装单位错误信息聚合
			query = """
					SELECT set_unit_name, count(*) fault_num
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
					GROUP BY set_unit_name
					"""
			var = [start_date, end_date]
			cursor.execute(query, var)
			rows = cursor.fetchall()
			for row in rows:
				set_unit_name = row[0]
				if set_unit_name is None:
					continue
				fault_num = row[1]
				insert = 'INSERT INTO dt_yc.model_set_unit_fault_num VALUES(%s, %s, %s)'
				var = [set_unit_name, i, fault_num]
				cursor.execute(insert, var)
				conn.commit()

			# 检验机构错误信息聚合
			query = """
					SELECT insp_org_name, count(*) fault_num
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
					GROUP BY insp_org_name
					"""
			var = [start_date, end_date]
			cursor.execute(query, var)
			rows = cursor.fetchall()
			for row in rows:
				insp_org_name = row[0]
				if insp_org_name is None:
					continue
				fault_num = row[1]
				insert = 'INSERT INTO dt_yc.model_insp_org_fault_num VALUES(%s, %s, %s)'
				var = [insp_org_name, i, fault_num]
				cursor.execute(insert, var)
				conn.commit()

			# 维保单位错误信息聚合
			query = """
					SELECT wb_unit_name, count(*) fault_num
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
					GROUP BY wb_unit_name
					"""
			var = [start_date, end_date]
			cursor.execute(query, var)
			rows = cursor.fetchall()
			for row in rows:
				wb_unit_name = row[0]
				if wb_unit_name is None:
					continue
				fault_num = row[1]
				insert = 'INSERT INTO dt_yc.model_wb_unit_fault_num VALUES(%s, %s, %s)'
				var = [wb_unit_name, i, fault_num]
				cursor.execute(insert, var)
				conn.commit()

			end_date = start_date
			print('第' + str(i) + '个月数据处理完成')

	temp = time.time()
	print('故障电梯数量统计完毕，运行时间：' + str(temp - flag))
	flag = temp

	# -------------------------------- 将电梯数量读入内存 --------------------------------
	# 读出dt_yc.model_use_unit_ele_num中的数据并存入use_unit_ele_num中
	query = 'SELECT * FROM dt_yc.model_use_unit_ele_num'
	cursor.execute(query)
	use_unit_ele_num = cursor.fetchall()

	# 读出dt_yc.model_make_unit_ele_num中的数据并存入make_unit_ele_num中
	query = 'SELECT * FROM dt_yc.model_make_unit_ele_num'
	cursor.execute(query)
	make_unit_ele_num = cursor.fetchall()

	# 读出dt_yc.model_set_unit_ele_num中的数据并存入set_unit_ele_num中
	query = 'SELECT * FROM dt_yc.model_set_unit_ele_num'
	cursor.execute(query)
	set_unit_ele_num = cursor.fetchall()

	# 读出dt_yc.model_wb_unit_ele_num中的数据并存入wb_unit_ele_num中
	query = 'SELECT * FROM dt_yc.model_wb_unit_ele_num'
	cursor.execute(query)
	wb_unit_ele_num = cursor.fetchall()

	# 读出dt_yc.model_insp_org_ele_num中的数据并存入insp_org_ele_num中
	query = 'SELECT * FROM dt_yc.model_insp_org_ele_num'
	cursor.execute(query)
	insp_org_ele_num = cursor.fetchall()

	temp = time.time()
	print('电梯数量信息读取完毕，运行时间：' + str(temp - flag))
	flag = temp

	# -------------------------------- 故障率计算 --------------------------------
	# 删除原先故障率表格
	query1 = 'DROP TABLE IF EXISTS dt_yc.model_use_unit_fault_rate'
	query2 = 'DROP TABLE IF EXISTS dt_yc.model_make_unit_fault_rate'
	query3 = 'DROP TABLE IF EXISTS dt_yc.model_set_unit_fault_rate'
	query4 = 'DROP TABLE IF EXISTS dt_yc.model_insp_org_fault_rate'
	query5 = 'DROP TABLE IF EXISTS dt_yc.model_wb_unit_fault_rate'
	cursor.execute(query1)
	conn.commit()
	cursor.execute(query2)
	conn.commit()
	cursor.execute(query3)
	conn.commit()
	cursor.execute(query4)
	conn.commit()
	cursor.execute(query5)
	conn.commit()

	# 建立故障率信息表格
	query1 = """
			CREATE TABLE dt_yc.model_use_unit_fault_rate(
			USE_UNIT_CODE varchar(50),
			FAULT_RATE_MONTH_1 decimal(10, 4),
			FAULT_RATE_MONTH_2 decimal(10, 4),
			FAULT_RATE_MONTH_3 decimal(10, 4),
			FAULT_RATE_MONTH_4 decimal(10, 4),
			FAULT_RATE_MONTH_5 decimal(10, 4),
			FAULT_RATE_MONTH_6 decimal(10, 4)
			)
			"""
	query2 = """
			CREATE TABLE dt_yc.model_make_unit_fault_rate(
			MAKE_UNIT_NAME varchar(50),
			FAULT_RATE_MONTH_1 decimal(10, 4),
			FAULT_RATE_MONTH_2 decimal(10, 4),
			FAULT_RATE_MONTH_3 decimal(10, 4),
			FAULT_RATE_MONTH_4 decimal(10, 4),
			FAULT_RATE_MONTH_5 decimal(10, 4),
			FAULT_RATE_MONTH_6 decimal(10, 4)
			)
			"""
	query3 = """
			CREATE TABLE dt_yc.model_set_unit_fault_rate(
			SET_UNIT_NAME varchar(50),
			FAULT_RATE_MONTH_1 decimal(10, 4),
			FAULT_RATE_MONTH_2 decimal(10, 4),
			FAULT_RATE_MONTH_3 decimal(10, 4),
			FAULT_RATE_MONTH_4 decimal(10, 4),
			FAULT_RATE_MONTH_5 decimal(10, 4),
			FAULT_RATE_MONTH_6 decimal(10, 4)
			)
			"""
	query4 = """
			CREATE TABLE dt_yc.model_insp_org_fault_rate(
			INSP_ORG_NAME varchar(50),
			FAULT_RATE_MONTH_1 decimal(10, 4),
			FAULT_RATE_MONTH_2 decimal(10, 4),
			FAULT_RATE_MONTH_3 decimal(10, 4),
			FAULT_RATE_MONTH_4 decimal(10, 4),
			FAULT_RATE_MONTH_5 decimal(10, 4),
			FAULT_RATE_MONTH_6 decimal(10, 4)
			)
			"""
	query5 = """
			CREATE TABLE dt_yc.model_wb_unit_fault_rate(
			WB_UNIT_NAME varchar(50),
			FAULT_RATE_MONTH_1 decimal(10, 4),
			FAULT_RATE_MONTH_2 decimal(10, 4),
			FAULT_RATE_MONTH_3 decimal(10, 4),
			FAULT_RATE_MONTH_4 decimal(10, 4),
			FAULT_RATE_MONTH_5 decimal(10, 4),
			FAULT_RATE_MONTH_6 decimal(10, 4)
			)
			"""
	cursor.execute(query1)
	conn.commit()
	cursor.execute(query2)
	conn.commit()
	cursor.execute(query3)
	conn.commit()
	cursor.execute(query4)
	conn.commit()
	cursor.execute(query5)
	conn.commit()

	# 为新建表格创建索引
	query1 = 'CREATE INDEX use_unit_fault_rate_index ON dt_yc.model_use_unit_fault_rate(use_unit_code)'
	query2 = 'CREATE INDEX make_unit_fault_rate_index ON dt_yc.model_make_unit_fault_rate(make_unit_name)'
	query3 = 'CREATE INDEX set_unit_fault_rate_index ON dt_yc.model_set_unit_fault_rate(set_unit_name)'
	query4 = 'CREATE INDEX insp_org_fault_rate_index ON dt_yc.model_insp_org_fault_rate(insp_org_name)'
	query5 = 'CREATE INDEX wb_unit_fault_rate_index ON dt_yc.model_wb_unit_fault_rate(wb_unit_name)'
	cursor.execute(query1)
	conn.commit()
	cursor.execute(query2)
	conn.commit()
	cursor.execute(query3)
	conn.commit()
	cursor.execute(query4)
	conn.commit()
	cursor.execute(query5)
	conn.commit()
	temp = time.time()

	# 读出dt_yc.model_use_unit_fault_num中的数据并存入use_unit_fault_num中
	use_unit_fault_num = {}
	query = 'SELECT * FROM dt_yc.model_use_unit_fault_num'
	cursor.execute(query)
	rows = cursor.fetchall()
	for row in rows:
		use_unit_code = row[0]
		month_num = row[1]
		fault_num = row[2]
		if use_unit_fault_num.get(use_unit_code, default) == -1:
			use_unit_fault_num[use_unit_code] = {}
		use_unit_fault_num[use_unit_code][month_num] = fault_num

	# 读出dt_yc.model_make_unit_fault_num中的数据并存入make_unit_fault_num中
	make_unit_fault_num = {}
	query = 'SELECT * FROM dt_yc.model_make_unit_fault_num'
	cursor.execute(query)
	rows = cursor.fetchall()
	for row in rows:
		make_unit_name = row[0]
		month_num = row[1]
		fault_num = row[2]
		if make_unit_fault_num.get(make_unit_name, default) == -1:
			make_unit_fault_num[make_unit_name] = {}
		make_unit_fault_num[make_unit_name][month_num] = fault_num

	# 读出dt_yc.model_set_unit_fault_num中的数据并存入set_unit_fault_num中
	set_unit_fault_num = {}
	query = 'SELECT * FROM dt_yc.model_set_unit_fault_num'
	cursor.execute(query)
	rows = cursor.fetchall()
	for row in rows:
		set_unit_name = row[0]
		month_num = row[1]
		fault_num = row[2]
		if set_unit_fault_num.get(set_unit_name, default) == -1:
			set_unit_fault_num[set_unit_name] = {}
		set_unit_fault_num[set_unit_name][month_num] = fault_num

	# 读出dt_yc.model_insp_org_fault_num中的数据并存入insp_org_fault_num中
	insp_org_fault_num = {}
	query = 'SELECT * FROM dt_yc.model_insp_org_fault_num'
	cursor.execute(query)
	rows = cursor.fetchall()
	for row in rows:
		insp_org_name = row[0]
		month_num = row[1]
		fault_num = row[2]
		if insp_org_fault_num.get(insp_org_name, default) == -1:
			insp_org_fault_num[insp_org_name] = {}
		insp_org_fault_num[insp_org_name][month_num] = fault_num

	# 读出dt_yc.model_wb_unit_fault_num中的数据并存入wb_unit_fault_num中
	wb_unit_fault_num = {}
	query = 'SELECT * FROM dt_yc.model_wb_unit_fault_num'
	cursor.execute(query)
	rows = cursor.fetchall()
	for row in rows:
		wb_unit_name = row[0]
		month_num = row[1]
		fault_num = row[2]
		if wb_unit_fault_num.get(wb_unit_name, default) == -1:
			wb_unit_fault_num[wb_unit_name] = {}
		wb_unit_fault_num[wb_unit_name][month_num] = fault_num

	temp = time.time()
	print('电梯故障信息读取完毕，运行时间：' + str(temp - flag))
	flag = temp

	# 使用单位故障率计算
	for use_unit in use_unit_ele_num:
		use_unit_code = use_unit[0]
		ele_num = use_unit[1]
		fault_info = use_unit_fault_num.get(use_unit_code, None)

		var = []
		var.append(use_unit_code)
		for month_num in range(1, 7):
			if fault_info is None:
				fault_num = 0
			else:
				fault_num = fault_info.get(month_num, 0)
			fault_rate = fault_num / ele_num
			fault_rate = round(fault_rate, 4)
			var.append(fault_rate)

		# 提高效率，六个月内无故障的单位不进行插入
		if var[1:] == [0, 0, 0, 0, 0, 0]:
			continue

		insert = 'INSERT INTO dt_yc.model_use_unit_fault_rate VALUES(%s, %s, %s, %s, %s, %s, %s)'
		cursor.execute(insert, var)
		conn.commit()
	temp = time.time()
	print('使用单位故障率计算完毕，运行时间：' + str(temp - flag))
	flag = temp

	# 制造单位故障率计算
	for make_unit in make_unit_ele_num:
		make_unit_name = make_unit[0]
		ele_num = make_unit[1]
		fault_info = make_unit_fault_num.get(make_unit_name, None)

		var = []
		var.append(make_unit_name)
		for month_num in range(1, 7):
			if fault_info is None:
				fault_num = 0
			else:
				fault_num = fault_info.get(month_num, 0)
			fault_rate = fault_num / ele_num
			fault_rate = round(fault_rate, 4)
			var.append(fault_rate)

		# 提高效率，六个月内无故障的单位不进行插入
		if var[1:] == [0, 0, 0, 0, 0, 0]:
			continue

		insert = 'INSERT INTO dt_yc.model_make_unit_fault_rate VALUES(%s, %s, %s, %s, %s, %s, %s)'
		cursor.execute(insert, var)
		conn.commit()
	temp = time.time()
	print('制造单位故障率计算完毕，运行时间：' + str(temp - flag))
	flag = temp

	# 安装单位故障率计算
	for set_unit in set_unit_ele_num:
		set_unit_name = set_unit[0]
		ele_num = set_unit[1]
		fault_info = set_unit_fault_num.get(set_unit_name, None)

		var = []
		var.append(set_unit_name)
		for month_num in range(1, 7):
			if fault_info is None:
				fault_num = 0
			else:
				fault_num = fault_info.get(month_num, 0)
			fault_rate = fault_num / ele_num
			fault_rate = round(fault_rate, 4)
			var.append(fault_rate)

		# 提高效率，六个月内无故障的单位不进行插入
		if var[1:] == [0, 0, 0, 0, 0, 0]:
			continue

		insert = 'INSERT INTO dt_yc.model_set_unit_fault_rate VALUES(%s, %s, %s, %s, %s, %s, %s)'
		cursor.execute(insert, var)
		conn.commit()
	temp = time.time()
	print('安装单位故障率计算完毕，运行时间：' + str(temp - flag))
	flag = temp

	# 检验机构故障率计算
	for insp_org in insp_org_ele_num:
		insp_org_name = insp_org[0]
		ele_num = insp_org[1]
		fault_info = insp_org_fault_num.get(insp_org_name, None)

		var = []
		var.append(insp_org_name)
		for month_num in range(1, 7):
			if fault_info is None:
				fault_num = 0
			else:
				fault_num = fault_info.get(month_num, 0)
			fault_rate = fault_num / ele_num
			fault_rate = round(fault_rate, 4)
			var.append(fault_rate)

		# 提高效率，六个月内无故障的单位不进行插入
		if var[1:] == [0, 0, 0, 0, 0, 0]:
			continue

		insert = 'INSERT INTO dt_yc.model_insp_org_fault_rate VALUES(%s, %s, %s, %s, %s, %s, %s)'
		cursor.execute(insert, var)
		conn.commit()
	temp = time.time()
	print('检验机构故障率计算完毕，运行时间：' + str(temp - flag))
	flag = temp

	# 维保单位故障率计算
	for wb_unit in wb_unit_ele_num:
		wb_unit_name = wb_unit[0]
		ele_num = wb_unit[1]
		fault_info = wb_unit_fault_num.get(wb_unit_name, None)

		var = []
		var.append(wb_unit_name)
		for month_num in range(1, 7):
			if fault_info is None:
				fault_num = 0
			else:
				fault_num = fault_info.get(month_num, 0)
			fault_rate = fault_num / ele_num
			fault_rate = round(fault_rate, 4)
			var.append(fault_rate)

		# 提高效率，六个月内无故障的单位不进行插入
		if var[1:] == [0, 0, 0, 0, 0, 0]:
			continue

		insert = 'INSERT INTO dt_yc.model_wb_unit_fault_rate VALUES(%s, %s, %s, %s, %s, %s, %s)'
		cursor.execute(insert, var)
		conn.commit()
	temp = time.time()
	print('维保单位故障率计算完毕，运行时间：' + str(temp - flag))
	flag = temp

	cursor.close()
	conn.close()
	elapsed = (time.time() - start)
	print('使用、制造、安装、维保单位与检验机构故障率统计完毕，总运行时间：' + str(round(elapsed/60, 2)) + '分钟')
	print('------------------------------------------------------')
	print('------------------------------------------------------')

update_fault_rate_month()
