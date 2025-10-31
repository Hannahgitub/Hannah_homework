# -任务2.5：分析统计主站最受欢迎的视频TOP 20
# 使用Spark SQL和原生SQL对比分析性能差异

import re
import time
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, count, col, desc, when

def main():
    # 初始化SparkSession
    spark = SparkSession.builder \
        .appName("VideoPopularityAnalysis") \
        .master("local[*]") \
        .getOrCreate()
    
    print("=== 任务2.5：分析统计主站最受欢迎的视频TOP 20 ===")
    print("数据来源: access.20161111.log")
    
    # 方法1：使用Spark SQL分析
    print("\n1. 使用Spark SQL进行分析...")
    spark_start_time = time.time()
    
    # 读取日志文件
    log_df = spark.read.text("access.20161111.log")
    
    # 解析日志文件内容，提取请求URL
    log_df = log_df.withColumn("request_url", regexp_extract(col("value"), '\"(GET|POST) (.+) HTTP/1\\.1\"', 2))
    
    # 提取视频ID - 匹配 /video/ 或 mid= 格式的视频请求
    log_df = log_df.withColumn("video_id", regexp_extract(col("request_url"), "/video/(\\d+)", 1))
    
    # 另外检查POST请求中的mid参数
    log_df = log_df.withColumn("video_id", \
        when(col("video_id") == "", regexp_extract(col("request_url"), "mid=(\\d+)\\&?", 1)).otherwise(col("video_id")))
    
    # 筛选出有效的视频访问记录
    video_df = log_df.filter(col("video_id") != "")
    
    # 创建临时视图
    video_df.createOrReplaceTempView("video_views")
    
    # 使用Spark SQL查询最受欢迎的TOP 20视频
    top_videos_spark = spark.sql("""
        SELECT video_id, COUNT(*) as view_count
        FROM video_views
        GROUP BY video_id
        ORDER BY view_count DESC
        LIMIT 20
    """)
    
    spark_end_time = time.time()
    spark_duration = spark_end_time - spark_start_time
    
    print(f"Spark SQL 分析耗时: {spark_duration:.2f} 秒")
    print("\nSpark SQL - 最受欢迎的TOP 20视频:")
    top_videos_spark.show()
    
    # 确保output目录存在
    import os
    os.makedirs("output", exist_ok=True)
    
    # 保存Spark SQL结果
    top_videos_spark.write.csv("output/top20_videos_spark", header=True, mode="overwrite")
    print("\nSpark SQL结果已保存到 output/top20_videos_spark 目录")
    
    # 方法2：使用原生SQL（SQLite）分析 - 由于数据量大，采样一部分数据进行演示
    print("\n2. 使用原生SQL（SQLite）进行分析...")
    
    # 为避免内存问题，采样处理
    sample_size = 1000000  # 采样100万行进行演示
    print(f"从日志文件中采样 {sample_size} 行数据进行原生SQL分析...")
    
    sqlite_start_time = time.time()
    
    # 创建SQLite数据库
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    
    # 创建表
    cursor.execute('''
    CREATE TABLE log_records (
        request_url TEXT,
        video_id TEXT
    )
    ''')
    
    # 读取并解析日志文件
    video_pattern1 = re.compile(r'/video/(\d+)')
    video_pattern2 = re.compile(r'mid=(\d+)\&?')
    
    count = 0
    batch_size = 10000
    batch_data = []
    
    try:
        with open("access.20161111.log", 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if count >= sample_size:
                    break
                    
                # 提取请求URL
                match = re.search(r'"(GET|POST) (.+) HTTP/1\.1"', line)
                if match:
                    request_url = match.group(2)
                    video_id = ""
                    
                    # 尝试匹配/video/格式
                    video_match = video_pattern1.search(request_url)
                    if video_match:
                        video_id = video_match.group(1)
                    else:
                        # 尝试匹配mid=格式
                        video_match = video_pattern2.search(request_url)
                        if video_match:
                            video_id = video_match.group(1)
                    
                    if video_id:
                        batch_data.append((request_url, video_id))
                        
                        if len(batch_data) >= batch_size:
                            cursor.executemany('INSERT INTO log_records VALUES (?, ?)', batch_data)
                            batch_data = []
                            count += batch_size
                            if count % 100000 == 0:
                                print(f"已处理 {count} 行数据...")
        
        # 插入剩余数据
        if batch_data:
            cursor.executemany('INSERT INTO log_records VALUES (?, ?)', batch_data)
        
        conn.commit()
        
        # 执行原生SQL查询
        cursor.execute('''
        SELECT video_id, COUNT(*) as view_count
        FROM log_records
        GROUP BY video_id
        ORDER BY view_count DESC
        LIMIT 20
        ''')
        
        top_videos_sqlite = cursor.fetchall()
        sqlite_end_time = time.time()
        sqlite_duration = sqlite_end_time - sqlite_start_time
        
        print(f"原生SQLite分析耗时 (基于采样数据): {sqlite_duration:.2f} 秒")
        print("\nSQLite - 最受欢迎的TOP 20视频:")
        for video_id, view_count in top_videos_sqlite:
            print(f"视频ID: {video_id}, 访问次数: {view_count}")
            
    except Exception as e:
        print(f"原生SQL分析过程中出错: {e}")
    finally:
        # 关闭连接
        conn.close()
    
    # 性能对比分析
    print("\n3. 性能对比分析:")
    print(f"Spark SQL分析耗时: {spark_duration:.2f} 秒")
    if 'sqlite_duration' in locals():
        print(f"SQLite分析耗时 (基于{sample_size}行采样数据): {sqlite_duration:.2f} 秒")
    
    print("\n结论:")
    print("- Spark SQL适合处理大规模数据集，具有分布式处理能力")
    print("- 原生SQL在小规模数据上性能可能更好")
    print("- 对于本案例中的超大规模日志文件(1800万行)，Spark SQL的优势会更加明显")
    print("- Spark SQL支持更大规模数据的处理，而不受单机内存限制")
    
    # 停止SparkSession
    spark.stop()
    
    print("\n=== 任务2.5完成 ===")

if __name__ == "__main__":
    main()