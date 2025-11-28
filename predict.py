# ==============================================================================
# 0. 环境准备与数据加载
# ==============================================================================
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix

# 设置显示选项
pd.set_option('display.float_format', lambda x: '%.4f' % x)
pd.set_option('display.max_columns', None)

FILE_NAME = "model_training_data.csv"
RANDOM_STATE = 42
PAID_RATIO_THRESHOLD = 0.20 # 判断是否需要SMOTE的付费用户比例阈值

try:
    df = pd.read_csv(FILE_NAME)
    print("数据加载成功！")
    print(f"数据形状：{df.shape}\n")
except FileNotFoundError:
    print(f"错误：文件 `{FILE_NAME}` 未找到。请确保文件在当前目录下。")
    exit()

# ==============================================================================
# 1. 数据预处理 (承接实验一)
# ==============================================================================

# (一) 读取数据：分离特征 (X) 和目标变量 (y)，剔除 user_id
y = df['is_paid']
X = df.drop(columns=['user_id', 'is_paid'])

paid_count = y.sum()
total_count = len(y)
paid_ratio = paid_count / total_count
is_imbalanced = paid_ratio < PAID_RATIO_THRESHOLD

print("--- 1. 数据预处理 ---")
print(f"付费用户占比: {paid_ratio:.4f}")
print(f"是否考虑样本平衡 (SMOTE): {is_imbalanced}")

# (二) 特征缩放：对连续型特征用 StandardScaler 标准化
# 假设的连续型特征列表（需根据实际数据情况调整）
continuous_features = [
    'registration_days', 'learning_sessions', 'learning_days',
    'avg_sessions_per_day', 'total_duration', 'avg_session_duration',
    'avg_score', 'max_score', 'score_std', 'word_accuracy',
    'sentence_accuracy', 'total_practices', 'course_completion_rate',
    'immersive_ratio', 'total_visits', 'active_days', 'visit_frequency',
    'unique_pages', 'unique_events'
]
# 筛选出实际存在的连续型特征
continuous_features = [col for col in continuous_features if col in X.columns]

scaler = StandardScaler()
X[continuous_features] = scaler.fit_transform(X[continuous_features])
print("连续型特征标准化完成。\n")

# ==============================================================================
# 2. 数据集划分
# ==============================================================================
print("--- 2. 数据集划分 (7:1:2 分层抽样) ---")

# 步骤 1: 划分 全量数据 = 训练集+验证集 (80%) + 测试集 (20%)
X_train_val, X_test, y_train_val, y_test = train_test_split(
    X, y,
    test_size=0.2,
    random_state=RANDOM_STATE,
    stratify=y
)

# 步骤 2: 划分 训练集+验证集 (80%) = 训练集 (70%) + 验证集 (10%)
# 验证集占比 (test_size) = 0.1 / 0.8 = 0.125
X_train, X_val, y_train, y_val = train_test_split(
    X_train_val, y_train_val,
    test_size=0.125,
    random_state=RANDOM_STATE,
    stratify=y_train_val
)

print(f"训练集 (70%): {len(X_train)} 样本, 付费率: {y_train.sum()/len(y_train):.4f}")
print(f"验证集 (10%): {len(X_val)} 样本, 付费率: {y_val.sum()/len(y_val):.4f}")
print(f"测试集 (20%): {len(X_test)} 样本, 付费率: {y_test.sum()/len(y_test):.4f}\n")


# ==============================================================================
# 2.5 样本平衡 (可选：SMOTE)
# ==============================================================================
if is_imbalanced:
    print("--- 2.5 样本平衡 (SMOTE) ---")
    smote = SMOTE(random_state=RANDOM_STATE)

    # 仅在训练集上应用 SMOTE
    X_train_smote, y_train_smote = smote.fit_resample(X_train, y_train)

    print(f"SMOTE 前训练集样本数: {len(X_train)}")
    print(f"SMOTE 后训练集样本数: {len(X_train_smote)}")
    print(f"SMOTE 后付费率: {y_train_smote.sum() / len(y_train_smote):.4f}")

    # 更新训练集变量
    X_train = X_train_smote
    y_train = y_train_smote
    print("SMOTE 样本平衡完成。\n")
else:
    print("--- 2.5 样本平衡 ---")
    print("付费率高于阈值，跳过 SMOTE 步骤。\n")

# ==============================================================================
# 3. 模型选择与超参数调整 (Logistic Regression + Grid Search)
# ==============================================================================
print("--- 3. 模型选择与超参数调整 (Logistic Regression) ---")

# 待优化的参数空间
param_grid = {
    'C': [0.01, 0.1, 1, 10, 100],  # 正则化强度倒数
    'penalty': ['l2'],
    'solver': ['lbfgs']
}

# 初始化逻辑回归模型
log_reg = LogisticRegression(max_iter=1000, random_state=RANDOM_STATE)

# 初始化 GridSearchCV
grid_search = GridSearchCV(
    estimator=log_reg,
    param_grid=param_grid,
    scoring='roc_auc',  # 使用 ROC AUC 作为调优指标
    cv=5,
    verbose=0,
    n_jobs=-1
)

# 在训练集上进行搜索
grid_search.fit(X_train, y_train)

best_model = grid_search.best_estimator_
best_params = grid_search.best_params_
best_score = grid_search.best_score_

print(f"最佳超参数: {best_params}")
print(f"最佳 ROC AUC (交叉验证): {best_score:.4f}\n")


# ==============================================================================
# 4. 模型训练与预测
# (注: 最优模型已通过 grid_search.fit() 完成训练)
# ==============================================================================
print("--- 4. 模型预测 ---")

# --- 验证集预测 ---
y_val_pred = best_model.predict(X_val)
y_val_proba = best_model.predict_proba(X_val)[:, 1] # 获取正类（付费）的概率

# --- 测试集预测 ---
y_test_pred = best_model.predict(X_test)
y_test_proba = best_model.predict_proba(X_test)[:, 1] # 获取正类（付费）的概率

print("预测标签和概率输出完成。\n")

# ==============================================================================
# 5. 模型评估 (核心关注准确率 + 业务指标)
# ==============================================================================

def evaluate_model(y_true, y_pred, y_proba, dataset_name):
    """计算并打印二分类模型的关键评估指标和业务解读"""
    print(f"\n--- 5. 模型评估: {dataset_name} ---")

    # 计算核心指标
    accuracy = accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    f1 = f1_score(y_true, y_pred, zero_division=0)
    roc_auc = roc_auc_score(y_true, y_proba)

    results = pd.DataFrame({
        '指标': ['准确率 (Accuracy)', '精确率 (Precision)', '召回率 (Recall)', 'F1 Score', 'AUC'],
        '结果': [accuracy, precision, recall, f1, roc_auc]
    }).set_index('指标')
    print(results.to_markdown(floatfmt=".4f"))

    # 混淆矩阵 (Confusion Matrix)
    cm = confusion_matrix(y_true, y_pred)
    tn, fp, fn, tp = cm.ravel()
    print("\n[混淆矩阵]")
    print(f"  实际未付费 | 实际已付费")
    print(f"预测未付费| {tn} (TN) | {fn} (FN)")
    print(f"预测已付费| {fp} (FP) | {tp} (TP)")

    # 业务指标解读
    print("\n[业务指标解读]")
    print(f"  - **召回率 ({recall:.4f})**: 模型捕捉到的真实付费用户的比例。高召回率有助于锁定所有潜在付费用户。")
    print(f"  - **精确率 ({precision:.4f})**: 预测为付费的用户中，真正付费的比例。高精确率有助于降低营销成本。")


# (一) 验证集评估
evaluate_model(y_val, y_val_pred, y_val_proba, "验证集 (Validation Set)")

# (二) 测试集评估 (最终结果)
evaluate_model(y_test, y_test_pred, y_test_proba, "测试集 (Test Set)")