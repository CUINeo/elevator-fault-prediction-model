# encoding: utf-8
import sys
import csv
import time
import pymysql
from datetime import datetime
from utils import get_use_months
from utils import get_current_date

#   host='10.214.163.179'
#   user='dt_yc'
#   password='dt_yc123'
#   port=3306
#   database='dt_yc'

def feature_extraction():
	# 作为dict访问的默认值
	default = -1
	current_date = get_current_date()
	start = time.time()
	conn = pymysql.connect(host='10.214.163.179', user='dt_yc', password='dt_yc123', port=3306, database='dt_yc')
	cursor = conn.cursor()

	# 本来需要将提取好的特征存到数据库中，现在只需要存到csv文件中即可
	# query = 'DROP TABLE IF EXISTS dt_yc.model_ele_info_feature'
	# cursor.execute(query)
	# conn.commit()

	# query = """
	# 		CREATE TABLE dt_yc.model_ele_info_feature(
	# 		PROBABILITY decimal(10, 4),
	# 		TRANID varchar(50),
	# 		EQU_SAFE_LEVEL_1 int(11),
	# 		EQU_SAFE_LEVEL_2 int(11),
	# 		EQU_SAFE_LEVEL_3 int(11),

	# 		APPLY_LOCATION_1 int(11),
	# 		APPLY_LOCATION_2 int(11),
	# 		APPLY_LOCATION_3 int(11),
	# 		APPLY_LOCATION_4 int(11),
	# 		APPLY_LOCATION_5 int(11),
	# 		APPLY_LOCATION_6 int(11),
	# 		APPLY_LOCATION_7 int(11),
	# 		APPLY_LOCATION_8 int(11),
	# 		APPLY_LOCATION_9 int(11),
	# 		APPLY_LOCATION_10 int(11),
	# 		APPLY_LOCATION_11 int(11),
	# 		APPLY_LOCATION_12 int(11),
	# 		APPLY_LOCATION_13 int(11),
	# 		APPLY_LOCATION_14 int(11),
	# 		APPLY_LOCATION_15 int(11),
	# 		APPLY_LOCATION_16 int(11),
	# 		APPLY_LOCATION_17 int(11),
	# 		APPLY_LOCATION_18 int(11),

	# 		USE_MONTHS decimal(5, 0),
	# 		EXAM_TYPE decimal(10, 0),

	# 		SAME_SET_UNIT_FAULT_RATE_MONTH_1 decimal(10, 4),
	# 		SAME_SET_UNIT_FAULT_RATE_MONTH_2 decimal(10, 4),
	# 		SAME_SET_UNIT_FAULT_RATE_MONTH_3 decimal(10, 4),
	# 		SAME_SET_UNIT_FAULT_RATE_MONTH_4 decimal(10, 4),
	# 		SAME_SET_UNIT_FAULT_RATE_MONTH_5 decimal(10, 4),
	# 		SAME_SET_UNIT_FAULT_RATE_MONTH_6 decimal(10, 4),

	# 		SAME_MAKE_UNIT_FAULT_RATE_MONTH_1 decimal(10, 4),
	# 		SAME_MAKE_UNIT_FAULT_RATE_MONTH_2 decimal(10, 4),
	# 		SAME_MAKE_UNIT_FAULT_RATE_MONTH_3 decimal(10, 4),
	# 		SAME_MAKE_UNIT_FAULT_RATE_MONTH_4 decimal(10, 4),
	# 		SAME_MAKE_UNIT_FAULT_RATE_MONTH_5 decimal(10, 4),
	# 		SAME_MAKE_UNIT_FAULT_RATE_MONTH_6 decimal(10, 4),

	# 		FAULT_NUMBER_MONTH_1 decimal(5, 0),
	# 		FAULT_NUMBER_MONTH_2 decimal(5, 0),
	# 		FAULT_NUMBER_MONTH_3 decimal(5, 0),
	# 		FAULT_NUMBER_MONTH_4 decimal(5, 0),
	# 		FAULT_NUMBER_MONTH_5 decimal(5, 0),
	# 		FAULT_NUMBER_MONTH_6 decimal(5, 0),

	# 		SAME_INSP_ORG_FAULT_RATE_MONTH_1 decimal(10, 4),
	# 		SAME_INSP_ORG_FAULT_RATE_MONTH_2 decimal(10, 4),
	# 		SAME_INSP_ORG_FAULT_RATE_MONTH_3 decimal(10, 4),
	# 		SAME_INSP_ORG_FAULT_RATE_MONTH_4 decimal(10, 4),
	# 		SAME_INSP_ORG_FAULT_RATE_MONTH_5 decimal(10, 4),
	# 		SAME_INSP_ORG_FAULT_RATE_MONTH_6 decimal(10, 4),

	# 		SAME_USE_UNIT_FAULT_RATE_MONTH_1 decimal(10, 4),
	# 		SAME_USE_UNIT_FAULT_RATE_MONTH_2 decimal(10, 4),
	# 		SAME_USE_UNIT_FAULT_RATE_MONTH_3 decimal(10, 4),
	# 		SAME_USE_UNIT_FAULT_RATE_MONTH_4 decimal(10, 4),
	# 		SAME_USE_UNIT_FAULT_RATE_MONTH_5 decimal(10, 4),
	# 		SAME_USE_UNIT_FAULT_RATE_MONTH_6 decimal(10, 4),

	# 		SAME_WB_UNIT_FAULT_RATE_MONTH_1 decimal(10, 4),
	# 		SAME_WB_UNIT_FAULT_RATE_MONTH_2 decimal(10, 4),
	# 		SAME_WB_UNIT_FAULT_RATE_MONTH_3 decimal(10, 4),
	# 		SAME_WB_UNIT_FAULT_RATE_MONTH_4 decimal(10, 4),
	# 		SAME_WB_UNIT_FAULT_RATE_MONTH_5 decimal(10, 4),
	# 		SAME_WB_UNIT_FAULT_RATE_MONTH_6 decimal(10, 4)
	# 		)
	# 		"""
	# cursor.execute(query)
	# conn.commit()

	# query = 'CREATE INDEX index_ele_info_feature ON dt_yc.model_ele_info_feature(TRANID)'
	# cursor.execute(query)
	# conn.commit()

	query = 'SELECT * FROM dt_yc.model_use_unit_fault_rate'
	cursor.execute(query)
	rows = cursor.fetchall()
	use_unit_fault_rate = {}
	for row in rows:
		use_unit_fault_rate[row[0]] = [row[1], row[2], row[3], row[4], row[5], row[6]]
	print('model_use_unit_fault_rate读取完毕')

	query = 'SELECT * FROM dt_yc.model_make_unit_fault_rate'
	cursor.execute(query)
	rows = cursor.fetchall()
	make_unit_fault_rate = {}
	for row in rows:
		make_unit_fault_rate[row[0]] = [row[1], row[2], row[3], row[4], row[5], row[6]]
	print('model_make_unit_fault_rate读取完毕')

	query = 'SELECT * FROM dt_yc.model_set_unit_fault_rate'
	cursor.execute(query)
	rows = cursor.fetchall()
	set_unit_fault_rate = {}
	for row in rows:
		set_unit_fault_rate[row[0]] = [row[1], row[2], row[3], row[4], row[5], row[6]]
	print('model_set_unit_fault_rate读取完毕')

	query = 'SELECT * FROM dt_yc.model_insp_org_fault_rate'
	cursor.execute(query)
	rows = cursor.fetchall()
	insp_org_fault_rate = {}
	for row in rows:
		insp_org_fault_rate[row[0]] = [row[1], row[2], row[3], row[4], row[5], row[6]]
	print('model_insp_org_fault_rate读取完毕')

	query = 'SELECT * FROM dt_yc.model_wb_unit_fault_rate'
	cursor.execute(query)
	rows = cursor.fetchall()
	wb_unit_fault_rate = {}
	for row in rows:
		wb_unit_fault_rate[row[0]] = [row[1], row[2], row[3], row[4], row[5], row[6]]
	print('model_wb_unit_fault_rate读取完毕')

	query = 'SELECT * FROM dt_yc.model_fault_num'
	cursor.execute(query)
	rows = cursor.fetchall()
	model_fault_num = {}
	for row in rows:
		tranid = row[0]
		month_num = row[1]
		fault_num = row[2]
		if model_fault_num.get(tranid, default) == default:
			model_fault_num[tranid] = []
		model_fault_num[tranid].append([month_num, fault_num])
	print('model_fault_num读取完毕')

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
			"""
	cursor.execute(query)
	rows = cursor.fetchall()
	print('ele_info读取完毕')

	csv_file_name = 'features/' + get_current_date() + '-feature.csv'
	csv_file = open(csv_file_name, 'w', newline='')
	writer = csv.writer(csv_file)

	cnt = 0
	for row in rows:
		equ_safe_level = row[0]
		apply_location = row[1]
		use_start_date = row[2]
		exam_type = row[3]

		use_unit_code = row[4]
		make_unit_name = row[5]
		set_unit_name = row[6]
		insp_org_name = row[7]
		wb_unit_name = row[8]

		tranid = row[9]

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

		use = use_unit_fault_rate.get(use_unit_code, None)
		make = make_unit_fault_rate.get(make_unit_name, None)
		_set = set_unit_fault_rate.get(set_unit_name, None)
		insp = insp_org_fault_rate.get(insp_org_name, None)
		wb = wb_unit_fault_rate.get(wb_unit_name, None)

		if use is not None:
			use1 = use[0]
			use2 = use[1]
			use3 = use[2]
			use4 = use[3]
			use5 = use[4]
			use6 = use[5]
		else:
			use1 = use2 = use3 = use4 = use5 = use6 = -1
		
		if make is not None:
			make1 = make[0]
			make2 = make[1]
			make3 = make[2]
			make4 = make[3]
			make5 = make[4]
			make6 = make[5]
		else:
			make1 = make2 = make3 = make4 = make5 = make6 = -1

		if _set is not None:
			set1 = _set[0]
			set2 = _set[1]
			set3 = _set[2]
			set4 = _set[3]
			set5 = _set[4]
			set6 = _set[5]
		else:
			set1 = set2 = set3 = set4 = set5 = set6 = -1
		
		if insp is not None:
			insp1 = insp[0]
			insp2 = insp[1]
			insp3 = insp[2]
			insp4 = insp[3]
			insp5 = insp[4]
			insp6 = insp[5]
		else:
			insp1 = insp2 = insp3 = insp4 = insp5 = insp6 = -1

		if wb is not None:
			wb1 = wb[0]
			wb2 = wb[1]
			wb3 = wb[2]
			wb4 = wb[3]
			wb5 = wb[4]
			wb6 = wb[5]
		else:
			wb1 = wb2 = wb2 = wb3 = wb4 = wb5 = wb6 = -1

		start_date = str(use_start_date)
		use_months = get_use_months(current_date, start_date)

		fault_nums = [0, 0, 0, 0, 0, 0]
		temp_fault_num = model_fault_num.get(tranid, default)
		if temp_fault_num != default:
			for row in temp_fault_num:
				month_num = row[0]
				fault_num = row[1]
				fault_nums[month_num-1] = fault_num

		# insert = 'INSERT INTO dt_yc.model_ele_info_feature VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
		val = [tranid, equ1, equ2, equ3, apply1, apply2, apply3, apply4, apply5, apply6, apply7, apply8, apply9,
			apply10, apply11, apply12, apply13, apply14, apply15, apply16, apply17, apply18, use_months,
			exam_type, set1, set2, set3, set4, set5, set6, make1, make2, make3, make4, make5, make6, fault_nums[0],
			fault_nums[1], fault_nums[2], fault_nums[3], fault_nums[4], fault_nums[5], insp1, insp2, insp3, insp4, insp5, insp6,
			use1, use2, use3, use4, use5, use6, wb1, wb2, wb3, wb4, wb5, wb6]
		# cursor.execute(insert, val)
		# conn.commit()

		writer.writerow(val)

		cnt = cnt + 1
		if cnt % 10000 == 0:
			print('处理中，目前已完成' + str(cnt) + '条')

	cursor.close()
	conn.close()
	elapsed = (time.time() - start)
	print('电梯特征提取完毕，总运行时间：' + str(round(elapsed/60, 2)) + '分钟')
	print('------------------------------------------------------')
	print('------------------------------------------------------')
