from predict import predict
from utils import prepare_table
from get_aggrs import get_aggrs
from feature_extraction import feature_extraction
from get_real_performance import get_real_performance
from update_fault_rate_month import update_fault_rate_month
from update_fault_number_month import update_fault_number_month

# 决定使用哪种模型进行预测
method_type = 'random forest'
# method_type = 'decision tree'
# method_type = 'knn'

# 数据库更新时间为diff天
diff = 2

# 性能评估的时间窗口为window天
window = 1

# prepare_table()
update_fault_number_month()
update_fault_rate_month()
feature_extraction()
predict(method_type)
get_real_performance(method_type, diff, window)
get_aggrs()
