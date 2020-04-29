from joblib import load

clf = load('rf.joblib')
features = ['equ_safe_level1', 'equ_safe_level2', 'equ_safe_level3', 'apply_location1', 'apply_location2', 'apply_location3',
            'apply_location4', 'apply_location5', 'apply_location6', 'apply_location7', 'apply_location8', 'apply_location9',
            'apply_location10', 'apply_location11', 'apply_location12', 'apply_location13', 'apply_location14', 'apply_location15',
            'apply_location16', 'apply_location17', 'apply_location18', 'use_months', 'exam_type', 'same_set_unit_fault_rate1', 
            'same_set_unit_fault_rate2', 'same_set_unit_fault_rate3', 'same_set_unit_fault_rate4', 'same_set_unit_fault_rate5',
            'same_set_unit_fault_rate6', 'same_make_unit_fault_rate1', 'same_make_unit_fault_rate2', 'same_make_unit_fault_rate3',
            'same_make_unit_fault_rate4', 'same_make_unit_fault_rate5', 'same_make_unit_fault_rate6', 'fault_number_month1',
            'fault_number_month2', 'fault_number_month3', 'fault_number_month4', 'fault_number_month5', 'fault_number_month6',
            'same_insp_org_fault_rate1', 'same_insp_org_fault_rate2', 'same_insp_org_fault_rate3', 'same_insp_org_fault_rate4',
            'same_insp_org_fault_rate5', 'same_insp_org_fault_rate6', 'same_use_unit_fault_rate1', 'same_use_unit_fault_rate2',
            'same_use_unit_fault_rate3', 'same_use_unit_fault_rate4', 'same_use_unit_fault_rate5', 'same_use_unit_fault_rate6',
            'same_wb_unit_fault_rate1', 'same_wb_unit_fault_rate2', 'same_wb_unit_fault_rate3', 'same_wb_unit_fault_rate4',
            'same_wb_unit_fault_rate5', 'same_wb_unit_fault_rate6']
importances = []
for importance in clf.feature_importances_:
    importances.append(float(importance))

for i in range(len(features)):
    if importances[i] != 0:
        print(features[i] + ' : ' + str(importances[i]))
