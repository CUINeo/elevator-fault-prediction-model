from train_rf_dt_knn import train_rf_dt_knn
from generate_train_data import generate_train_data

# 训练数据正负样本比例为1:train_ratio
train_ratio = 50
# 测试数据正负样本比例为1:test_ratio
test_ratio = 540
# 测试数据使用故障数据条数为test_fault_window（生成的测试样本数量为(test_ratio + 1) * test_fault_window）
test_fault_window = 200

generate_train_data(train_ratio, test_ratio, test_fault_window)
train_rf_dt_knn((test_ratio + 1) * test_fault_window)
