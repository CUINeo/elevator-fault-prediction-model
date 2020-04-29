# encoding: utf-8
from train_rf_dt_knn import train_rf_dt_knn
from generate_train_data import generate_train_data

# 训练数据正负样本比例为1:train_ratio
train_ratio = 50
# 测试数据正负样本比例为1:test_ratio
test_ratio = 600
# 测试数据使用故障数据条数为test_fault_window
test_fault_window = 0
# 测试样本数量
test_data_num = (test_ratio + 1) * test_fault_window

generate_train_data(train_ratio, test_ratio, test_fault_window)
train_rf_dt_knn(test_data_num)
