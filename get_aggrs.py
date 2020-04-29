import csv
import pymysql
from utils import get_use_months
from utils import get_current_date

# host='10.214.163.179'
# user='dt_yc'
# password='dt_yc123'
# port=3306
# database='dt_yc'

# 根据预测结果进行数据聚合
def get_aggrs(threshold):
    conn = pymysql.connect(host='10.214.163.179', user='dt_yc', password='dt_yc123', port=3306, database='dt_yc')
    cursor = conn.cursor()

    current_date = get_current_date()

    # 从csv中读取得到current_date的预测结果
    csv_file = open('results/' + str(current_date) + '-result.csv', 'r')
    reader = csv.reader(csv_file)
    rows = list(reader)

    # 得到预测为高危的电梯集合
    high_risk_tranid_set = []
    for row in rows:
        if row[0] == 'tranid':
            continue
        if float(row[1]) >= threshold:
            high_risk_tranid_set.append(row[0])

    # 读取ele_info中的内容
    query = """
            SELECT
            apply_location,
            use_start_date,
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

    ele_info = {}
    for row in rows:
        apply_location = row[0]
        use_start_date = str(row[1])
        tranid = row[2]

        use_months = get_use_months(current_date, use_start_date)
        ele_info[tranid] = [apply_location, use_months]

    # 高危电梯地区信息聚合（信息缺失，暂时无法聚合）

    # 高危电梯使用月数信息聚合（分别存储0-50，50-100，100-150，150-200，200-250，250-300，300以上的数据）
    use_months_aggr = [0, 0, 0, 0, 0, 0, 0]
    for high_risk_tranid in high_risk_tranid_set:
        info = ele_info.get(high_risk_tranid, [])
        if len(info) == 0:
            continue
        else:
            use_months = info[1]
            if use_months in range(0, 50):
                use_months_aggr[0] += 1
            elif use_months in range(50, 100):
                use_months_aggr[1] += 1
            elif use_months in range(100, 150):
                use_months_aggr[2] += 1
            elif use_months in range(150, 200):
                use_months_aggr[3] += 1
            elif use_months in range(200, 250):
                use_months_aggr[4] += 1
            elif use_months in range(250, 300):
                use_months_aggr[5] += 1
            elif use_months >= 300:
                use_months_aggr[6] += 1
    
    # 将聚合结果插入数据库中
    for i in range(len(use_months_aggr)):
        amount = use_months_aggr[i]
        lower_use_month = i * 50
        upper_use_month = (i + 1) * 50
        if i == 6:
            upper_use_month = 100000000

        insert = "INSERT INTO dt_yc.model_use_months_aggr VALUES(DATE_FORMAT(%s, '%%Y-%%m-%%d'), %s, %s, %s)"
        val = [current_date, lower_use_month, upper_use_month, amount]
        cursor.execute(insert, val)
        conn.commit()
    print('高危电梯使用月数信息聚合完毕')

    # 高危电梯投用场所信息聚合
    apply_loc_aggr = {}
    for high_risk_tranid in high_risk_tranid_set:
        info = ele_info.get(high_risk_tranid, [])
        if len(info) == 0:
            continue
        else:
            apply_location = info[0]
            if apply_loc_aggr.get(apply_location, -1) == -1:
                apply_loc_aggr[apply_location] = 1
            else:
                apply_loc_aggr[apply_location] += 1

    # 将聚合结果插入数据库中
    for apply_location in apply_loc_aggr:
        amount = apply_loc_aggr[apply_location]

        insert = "INSERT INTO dt_yc.model_apply_loc_aggr VALUES(DATE_FORMAT(%s, '%%Y-%%m-%%d'), %s, %s)"
        val = [current_date, apply_location, amount]
        cursor.execute(insert, val)
        conn.commit()
    print('高危电梯投用场所信息聚合完毕')

    print('高危电梯信息聚合完毕')
    print('------------------------------------------------------')
    print('------------------------------------------------------')

    cursor.close()
    conn.close()
