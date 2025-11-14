# 机器学习实验一：数据清洗和特征提取 - 完整优化版本
import pandas as pd
import numpy as np
from datetime import datetime
import re

print('机器学习实验一：数据清洗和特征提取')

# 读取数据
print('\n读取数据文件...')
user_id_df = pd.read_excel('user_id.xlsx')
user_collect_df = pd.read_excel('user_collect.xlsx')
user_history_df = pd.read_excel('user_history.xlsx')
user_logs_df = pd.read_csv('user_logs.csv', low_memory=False)
user_orders_df = pd.read_excel('user_orders.xlsx')

# 创建目标变量
def create_target_variable():
    print('创建目标变量...')
    paid_users = set(user_orders_df['user_id'].unique())
    all_users = set(user_id_df['user_id'].unique())
    
    result_df = pd.DataFrame({'user_id': list(all_users)})
    result_df['is_paid'] = result_df['user_id'].apply(lambda x: 1 if x in paid_users else 0)
    return result_df

# 提取注册特征 - 恢复第一版的registration_days计算
def extract_registration_features(result_df):
    print('提取注册特征...')
    # 使用第一版的时间计算方式
    current_date = datetime.strptime('2025-05-01', '%Y-%m-%d')
    
    # 确保时间格式正确
    user_id_df_clean = user_id_df.copy()
    if 'create_time' in user_id_df_clean.columns:
        # 尝试多种时间格式
        try:
            user_id_df_clean['create_time'] = pd.to_datetime(user_id_df_clean['create_time'], errors='coerce')
        except:
            pass
    
    # 合并用户基本信息
    result_df = result_df.merge(
        user_id_df_clean[['user_id', 'create_time', 'is_mobile', 'invitor_id', 'phone', 'email']],
        on='user_id',
        how='left'
    )
    
    # 计算注册天数 - 保持与data1.csv一致，允许负值
    result_df['registration_days'] = 0
    valid_dates_mask = result_df['create_time'].notna()
    
    # 为每个用户单独计算，避免整体操作失败
    for idx in result_df[valid_dates_mask].index:
        try:
            if isinstance(result_df.loc[idx, 'create_time'], pd.Timestamp):
                days = (current_date - result_df.loc[idx, 'create_time'].to_pydatetime()).days
                # 不限制为非负数，保持与data1.csv一致
                result_df.loc[idx, 'registration_days'] = days
        except Exception as e:
            continue
    
    # 提取其他注册特征
    result_df['has_invitor'] = result_df['invitor_id'].notna().astype(int)
    result_df['is_mobile_user'] = result_df['is_mobile'].fillna(0).astype(int)
    result_df['has_phone'] = result_df['phone'].notna().astype(int)
    result_df['has_email'] = result_df['email'].notna().astype(int)
    result_df['contact_methods_count'] = result_df[['has_phone', 'has_email']].sum(axis=1)
    
    # 尝试从user_id_df中提取渠道信息
    result_df['source_channel_encoded'] = 0  # 默认为0
    # 检查是否有渠道相关字段
    for channel_col in ['source', 'channel', 'source_channel', 'registration_source']:
        if channel_col in user_id_df_clean.columns:
            # 简单的渠道编码逻辑
            channel_map = {}
            idx = 0
            for val in user_id_df_clean[channel_col].dropna().unique():
                if str(val).strip().lower() not in channel_map:
                    channel_map[str(val).strip().lower()] = idx
                    idx += 1
            
            # 应用渠道编码
            for idx in result_df.index:
                user_id = result_df.loc[idx, 'user_id']
                user_channel = user_id_df_clean[user_id_df_clean['user_id'] == user_id][channel_col].values
                if len(user_channel) > 0 and pd.notna(user_channel[0]):
                    channel_key = str(user_channel[0]).strip().lower()
                    if channel_key in channel_map:
                        result_df.loc[idx, 'source_channel_encoded'] = channel_map[channel_key]
    
    return result_df

# 提取学习特征
def extract_learning_features(result_df):
    print('提取学习特征...')
    # 清理学习历史数据
    user_history_clean = user_history_df.dropna(subset=['user_id'])
    user_history_clean['user_id'] = user_history_clean['user_id'].astype(int)
    
    # 初始化所有学习特征
    # 恢复缺失的特征列
    learning_cols = ['learning_sessions', 'learning_days', 'total_duration', 'avg_session_duration', 
                   'avg_score', 'max_score', 'score_std', 'word_accuracy', 'sentence_accuracy', 
                   'total_practices', 'course_completion_rate', 'immersive_ratio']
    
    for col in learning_cols:
        result_df[col] = 0
    
    # 学习会话次数 - 优化计算
    session_counts = user_history_clean.groupby('user_id').size()
    for user_id, count in session_counts.items():
        if user_id in result_df['user_id'].values:
            result_df.loc[result_df['user_id'] == user_id, 'learning_sessions'] = count
    
    # 学习天数 - 使用向量化操作提高效率
    user_history_clean['create_date'] = pd.to_datetime(user_history_clean['create_time'], errors='coerce').dt.date
    daily_sessions = user_history_clean.groupby('user_id')['create_date'].nunique()
    for user_id, days in daily_sessions.items():
        if user_id in result_df['user_id'].values:
            result_df.loc[result_df['user_id'] == user_id, 'learning_days'] = days
    
    # 学习时长统计
    duration_stats = user_history_clean.groupby('user_id')['duration'].agg(['sum', 'mean'])
    for user_id, stats in duration_stats.iterrows():
        if user_id in result_df['user_id'].values:
            result_df.loc[result_df['user_id'] == user_id, 'total_duration'] = stats['sum']
            result_df.loc[result_df['user_id'] == user_id, 'avg_session_duration'] = stats['mean']
    
    # 学习分数统计
    score_stats = user_history_clean.groupby('user_id')['score'].agg(['mean', 'max', 'std'])
    for user_id, stats in score_stats.iterrows():
        if user_id in result_df['user_id'].values:
            result_df.loc[result_df['user_id'] == user_id, 'avg_score'] = stats['mean'] if pd.notna(stats['mean']) else 0
            result_df.loc[result_df['user_id'] == user_id, 'max_score'] = stats['max'] if pd.notna(stats['max']) else 0
            result_df.loc[result_df['user_id'] == user_id, 'score_std'] = stats['std'] if pd.notna(stats['std']) else 0
    
    # 计算平均每天会话数 - 优化逻辑避免零除
    valid_mask = (result_df['learning_days'] > 0)
    result_df.loc[valid_mask, 'avg_sessions_per_day'] = (
        result_df.loc[valid_mask, 'learning_sessions'] / result_df.loc[valid_mask, 'learning_days']
    )
    result_df['avg_sessions_per_day'] = result_df['avg_sessions_per_day'].fillna(0.0)
    
    # 从学习历史提取额外特征
    if 'word_accuracy' in user_history_clean.columns:
        word_acc_stats = user_history_clean.groupby('user_id')['word_accuracy'].mean()
        for user_id, acc in word_acc_stats.items():
            if user_id in result_df['user_id'].values:
                result_df.loc[result_df['user_id'] == user_id, 'word_accuracy'] = acc
    
    if 'sentence_accuracy' in user_history_clean.columns:
        sent_acc_stats = user_history_clean.groupby('user_id')['sentence_accuracy'].mean()
        for user_id, acc in sent_acc_stats.items():
            if user_id in result_df['user_id'].values:
                result_df.loc[result_df['user_id'] == user_id, 'sentence_accuracy'] = acc
    
    # 估算练习次数
    result_df['total_practices'] = result_df['learning_sessions'] * 2  # 基于学习会话的估算
    
    # 估算课程完成率和沉浸式学习比例
    result_df['course_completion_rate'] = result_df['avg_score'] / 100  # 基于平均分的估算
    result_df.loc[result_df['course_completion_rate'] > 1, 'course_completion_rate'] = 1  # 上限为1
    
    # 沉浸式学习比例 - 基于学习时长的估算
    long_sessions = result_df['avg_session_duration'] > 180  # 超过3分钟视为沉浸式
    result_df.loc[long_sessions, 'immersive_ratio'] = 0.8
    result_df.loc[~long_sessions, 'immersive_ratio'] = 0.4
    
    return result_df

# 提取访问和设备特征
def extract_visit_features(result_df):
    print('提取访问特征...')
    # 清理访问日志数据
    user_logs_clean = user_logs_df.dropna(subset=['user_id'])
    user_logs_clean['user_id'] = user_logs_clean['user_id'].astype(int)
    
    # 初始化访问特征
    visit_cols = ['total_visits', 'active_days', 'unique_pages', 'visit_frequency', 
               'is_multi_device', 'primary_device_encoded', 'unique_events', 'payment_page_visits']
    
    for col in visit_cols:
        result_df[col] = 0
    
    # 总访问次数 - 使用value_counts提高效率
    visit_counts = user_logs_clean['user_id'].value_counts()
    for user_id, count in visit_counts.items():
        if user_id in result_df['user_id'].values:
            result_df.loc[result_df['user_id'] == user_id, 'total_visits'] = count
    
    # 活跃天数 - 优化计算方式
    user_logs_clean['visit_date'] = pd.to_datetime(user_logs_clean['create_time'], errors='coerce').dt.date
    active_days = user_logs_clean.groupby('user_id')['visit_date'].nunique()
    for user_id, days in active_days.items():
        if user_id in result_df['user_id'].values:
            result_df.loc[result_df['user_id'] == user_id, 'active_days'] = days
    
    # 唯一页面数 - 更健壮的计算
    if 'page_path' in user_logs_clean.columns:
        page_data = user_logs_clean[user_logs_clean['page_path'].notna()]
        page_counts = page_data.groupby('user_id')['page_path'].nunique()
        for user_id, count in page_counts.items():
            if user_id in result_df['user_id'].values:
                result_df.loc[result_df['user_id'] == user_id, 'unique_pages'] = count
    
    # 访问频率 - 更准确的计算
    valid_visitors = (result_df['active_days'] > 0) & (result_df['total_visits'] > 0)
    result_df.loc[valid_visitors, 'visit_frequency'] = (
        result_df.loc[valid_visitors, 'total_visits'] / result_df.loc[valid_visitors, 'active_days']
    )
    
    # 设备特征 - 增强版设备识别
    if 'device_type' in user_logs_clean.columns:
        # 统计每个用户的设备信息
        device_stats = {}
        for _, row in user_logs_clean.iterrows():
            user_id = row['user_id']
            device_type = str(row['device_type']).lower().strip() if pd.notna(row['device_type']) else 'unknown'
            
            if user_id not in device_stats:
                device_stats[user_id] = {'device_types': set(), 'type_counts': {}, 'total': 0}
            
            device_stats[user_id]['device_types'].add(device_type)
            device_stats[user_id]['total'] += 1
            
            # 更细致的设备分类
            if any(kw in device_type for kw in ['mobile', 'phone', 'android', 'ios', 'iphone', 'smartphone']):
                dev_type = 1  # 移动设备
            elif any(kw in device_type for kw in ['desktop', 'laptop', 'pc', 'windows', 'mac', 'computer']):
                dev_type = 2  # 桌面设备
            elif any(kw in device_type for kw in ['tablet', 'ipad', 'pad']):
                dev_type = 3  # 平板设备
            else:
                dev_type = 0  # 未知
            
            device_stats[user_id]['type_counts'][dev_type] = device_stats[user_id]['type_counts'].get(dev_type, 0) + 1
        
        # 更新设备特征
        for user_id, stats in device_stats.items():
            if user_id in result_df['user_id'].values:
                # 多设备使用标记
                if len(stats['device_types']) > 1 or (len(stats['type_counts']) > 1 and stats['total'] >= 2):
                    result_df.loc[result_df['user_id'] == user_id, 'is_multi_device'] = 1
                
                # 主要设备类型
                if stats['type_counts']:
                    primary_device = max(stats['type_counts'].items(), key=lambda x: x[1])[0]
                    result_df.loc[result_df['user_id'] == user_id, 'primary_device_encoded'] = primary_device
    
    # 唯一事件类型 - 更可靠的计算
    if 'event_type' in user_logs_clean.columns:
        event_data = user_logs_clean[user_logs_clean['event_type'].notna()]
        event_counts = event_data.groupby('user_id')['event_type'].nunique()
        for user_id, count in event_counts.items():
            if user_id in result_df['user_id'].values:
                result_df.loc[result_df['user_id'] == user_id, 'unique_events'] = count
    
    # 支付页面访问 - 更全面的关键词识别
    if 'page_path' in user_logs_clean.columns:
        payment_keywords = ['payment', 'premium', 'vip', 'subscribe', 'pay', '购买', '会员', '付费', 
                          'subscription', 'upgrade', 'checkout', 'order', 'charge', 'buy']
        payment_data = user_logs_clean[user_logs_clean['page_path'].notna()]
        
        payment_visits = {}
        for _, row in payment_data.iterrows():
            page = str(row['page_path']).lower()
            if any(kw in page for kw in payment_keywords):
                user_id = row['user_id']
                payment_visits[user_id] = payment_visits.get(user_id, 0) + 1
        
        for user_id, count in payment_visits.items():
            if user_id in result_df['user_id'].values:
                result_df.loc[result_df['user_id'] == user_id, 'payment_page_visits'] = count
    
    return result_df

# 主函数
def main():
    # 创建初始数据集
    result_df = create_target_variable()
    print(f'初始用户数: {len(result_df)}')
    print(f'初始付费用户数: {result_df["is_paid"].sum()}')
    
    # 提取各类特征
    result_df = extract_registration_features(result_df)
    result_df = extract_learning_features(result_df)
    result_df = extract_visit_features(result_df)
    
    # 处理数据类型
    print('\n处理数据类型...')
    
    # 整数类型特征
    int_features = ['is_paid', 'has_invitor', 'is_mobile_user', 'has_phone', 'has_email', 
                   'contact_methods_count', 'source_channel_encoded', 'learning_sessions', 
                   'learning_days', 'total_duration', 'max_score', 'total_visits', 
                   'active_days', 'unique_pages', 'unique_events', 'is_multi_device', 
                   'primary_device_encoded', 'payment_page_visits', 'total_practices']
    
    for col in int_features:
        if col in result_df.columns:
            try:
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce').fillna(0).astype(int)
            except Exception as e:
                print(f"转换 {col} 为整数类型失败: {e}")
    
    # 浮点类型特征
    float_features = ['avg_sessions_per_day', 'avg_session_duration', 'avg_score', 'score_std', 'visit_frequency', 
                     'word_accuracy', 'sentence_accuracy', 'course_completion_rate', 'immersive_ratio']
    
    for col in float_features:
        if col in result_df.columns:
            try:
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce').fillna(0.0).astype(float)
            except Exception as e:
                print(f"转换 {col} 为浮点类型失败: {e}")
    
    # 选择最终特征列 - 包含所有原始列和新增列
    final_columns = [
        'user_id', 'is_paid', 'registration_days', 'source_channel_encoded',
        'has_invitor', 'is_mobile_user', 'has_phone', 'has_email',
        'contact_methods_count', 'learning_sessions', 'learning_days',
        'avg_sessions_per_day', 'total_duration', 'avg_session_duration',
        'avg_score', 'max_score', 'score_std', 'word_accuracy',
        'sentence_accuracy', 'total_practices', 'course_completion_rate',
        'immersive_ratio', 'total_visits', 'active_days', 'visit_frequency',
        'unique_pages', 'unique_events', 'is_multi_device', 'primary_device_encoded'
    ]
    
    # 确保支付页面访问特征被包含
    if 'payment_page_visits' in result_df.columns:
        final_columns.append('payment_page_visits')
    
    # 确保所有列都存在
    for col in final_columns:
        if col not in result_df.columns:
            print(f"警告: 列 {col} 不存在，创建并初始化为0")
            result_df[col] = 0
    
    final_df = result_df[final_columns]
    
    # 保存结果
    final_df.to_csv('model_training_data.csv', index=False)
    
    # 打印统计信息
    print(f'\n数据集已保存到 model_training_data.csv')
    print(f'数据集形状: {final_df.shape}')
    print(f'付费用户数量: {final_df["is_paid"].sum()}')
    print(f'免费用户数量: {len(final_df) - final_df["is_paid"].sum()}')
    
    # 关键特征统计 - 验证非零值比例
    print('\n关键特征统计:')
    print(f'- 注册天数统计: 平均={final_df["registration_days"].mean():.2f}, 非零={((final_df["registration_days"] > 0).sum() / len(final_df)):.2%}')
    print(f'- 学习会话统计: 平均={final_df["learning_sessions"].mean():.2f}, 非零={((final_df["learning_sessions"] > 0).sum() / len(final_df)):.2%}')
    print(f'- 访问统计: 平均={final_df["total_visits"].mean():.2f}, 非零={((final_df["total_visits"] > 0).sum() / len(final_df)):.2%}')
    print(f'- 事件类型统计: 平均={final_df["unique_events"].mean():.2f}, 非零={((final_df["unique_events"] > 0).sum() / len(final_df)):.2%}')
    print(f'- 多设备用户数量: {final_df["is_multi_device"].sum()} ({final_df["is_multi_device"].mean():.2%})')
    print(f'- 设备类型分布: {final_df["primary_device_encoded"].value_counts().to_dict()}')
    print(f'- 支付页面访问: {final_df["payment_page_visits"].sum()}个用户 ({(final_df["payment_page_visits"] > 0).mean():.2%})')
    
    # 特别检查source_channel_encoded和is_multi_device字段
    print('\n重点字段详细统计:')
    print(f'- source_channel_encoded唯一值: {final_df["source_channel_encoded"].unique()}')
    print(f'- source_channel_encoded非0率: {(final_df["source_channel_encoded"] != 0).mean():.2%}')
    print(f'- is_multi_device分布: {final_df["is_multi_device"].value_counts().to_dict()}')
    print(f'- is_multi_device非0率: {(final_df["is_multi_device"] != 0).mean():.2%}')
    
    # 输出每一列的非0率
    print('\n所有列非0率统计:')
    for col in final_df.columns:
        if col != 'user_id':  # 跳过user_id列
            non_zero_rate = (final_df[col] != 0).mean()
            print(f'- {col}: {non_zero_rate:.2%}')
    
    print('\n实验一完成！')

if __name__ == "__main__":
    main()