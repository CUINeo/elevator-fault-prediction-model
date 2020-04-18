from predict import predict
from utils import prepare_table
from get_aggrs import get_aggrs
from feature_extraction import feature_extraction
from get_real_performance import get_real_performance
from update_fault_rate_month import update_fault_rate_month
from update_fault_number_month import update_fault_number_month


# 决定使用哪种模型进行预测
method = 1
method_type = 'fm'
# method = 2
# method_type = 'random forest'
# method = 3
# method_type = 'decision tree'
# method = 4
# method_type = 'knn'

# 预测值大于threshold时判定为高危电梯（用于四种模型的真实性能评估）
threshold = 0.5

# 预测结果为从今天起向后推diff天内是否会发生故障
diff = 30

# prepare_table()
update_fault_number_month()
update_fault_rate_month()
feature_extraction()
predict(method)
get_real_performance(method_type, threshold, diff)
get_aggrs(threshold)
