from flask import Flask, render_template, request, jsonify
import sqlite3
import random
import time
import datetime
import re
from collections import deque
import psutil  # 需要安装：pip install psutil
import threading
import requests  # 添加requests库用于API调用
import duckdb

url = "http://localhost:8080/v1/chat/completions"
headers = {"Content-Type": "application/json"}

app = Flask(__name__)

# 创建SQLite数据库存储历史记录
conn = sqlite3.connect('query_history.db', check_same_thread=False)
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS history
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
              timestamp DATETIME,
              user_query TEXT,
              generated_sql TEXT,
              natural_language_result TEXT)''')
conn.commit()

# 模拟传感器数据
def generate_sensor_data():
    devices = ['风机A', '风机B', '压缩机C', '电机D', '泵E']
    sensors = ['温度', '振动', '压力', '电流', '电压']
    data = []

    for i in range(5):
        device = random.choice(devices)
        sensor = random.choice(sensors)
        value = round(random.uniform(50, 100), 1)
        status = '正常' if value < 85 else '警告' if value < 95 else '危险'

        data.append({
            'id': i + 1,
            'timestamp': (datetime.datetime.now() - datetime.timedelta(minutes=random.randint(1, 60)))
                          .strftime('%Y-%m-%d %H:%M:%S'),
                          'device': device,
                          'sensor': sensor,
                          'value': value,
                          'status': status
                          })

    return data


# # 模拟LLM生成SQL
# def generate_sql_from_query(query):
#     query = query.lower()
#
#     # 提取关键信息
#     time_pattern = r'(\d+)\s*(分钟|小时|天)'
#     sensor_pattern = r'(温度|振动|压力|电流|电压)'
#     device_pattern = r'(\d+号设备|风机[A-D]|压缩机[C-F]|电机[D-G]|泵[E-H])'
#
#     # 时间条件
#     time_match = re.search(time_pattern, query)
#     time_condition = ""
#     if time_match:
#         value, unit = time_match.groups()
#         if unit == '分钟':
#             time_condition = f"time > NOW() - INTERVAL '{value} minutes'"
#         elif unit == '小时':
#             time_condition = f"time > NOW() - INTERVAL '{value} hours'"
#         elif unit == '天':
#             time_condition = f"time > NOW() - INTERVAL '{value} days'"
#
#     # 传感器类型条件
#     sensor_match = re.search(sensor_pattern, query)
#     sensor_condition = ""
#     if sensor_match:
#         sensor_type = sensor_match.group(1)
#         sensor_condition = f"sensor_type = '{sensor_type}'"
#
#     # 设备条件
#     device_match = re.search(device_pattern, query)
#     device_condition = ""
#     if device_match:
#         device_id = device_match.group(1)
#         device_condition = f"device_id = '{device_id}'"
#
#     # 阈值条件
#     threshold_match = re.search(r'超过\s*(\d+)(度|℃|%)', query)
#     threshold_condition = ""
#     if threshold_match:
#         threshold = threshold_match.group(1)
#         threshold_condition = f"value > {threshold}"
#
#     # 状态条件
#     status_condition = ""
#     if '警告' in query:
#         status_condition = "status = '警告'"
#     elif '危险' in query:
#         status_condition = "status = '危险'"
#     elif '异常' in query:
#         status_condition = "status != '正常'"
#
#     # 组合条件
#     conditions = [c for c in [time_condition, sensor_condition, device_condition,
#                               threshold_condition, status_condition] if c]
#
#     # 确定查询类型
#     if '统计' in query or '数量' in query or '次数' in query:
#         if conditions:
#             where_clause = "WHERE " + " AND ".join(conditions)
#         else:
#             where_clause = ""
#         return f"SELECT COUNT(*) FROM sensors {where_clause}"
#
#     elif '列表' in query or '显示' in query or '查看' in query:
#         if conditions:
#             where_clause = "WHERE " + " AND ".join(conditions)
#         else:
#             where_clause = ""
#         return f"SELECT * FROM sensors {where_clause} ORDER BY time DESC LIMIT 10"
#
#     elif '导出' in query or '下载' in query:
#         if conditions:
#             where_clause = "WHERE " + " AND ".join(conditions)
#         else:
#             where_clause = ""
#         return f"COPY (SELECT * FROM sensors {where_clause}) TO 'output.csv' (FORMAT CSV)"
#
#     else:
#         # 默认查询
#         if conditions:
#             where_clause = "WHERE " + " AND ".join(conditions)
#         else:
#             where_clause = ""
#         return f"SELECT * FROM sensors {where_clause} ORDER BY time DESC LIMIT 10"


# 使用Llama Server生成SQL
def generate_sql_from_query(query):
    # 调用本地部署的Llama Server
    # url = "http://localhost:8080/v1/chat/completions"
    # headers = {"Content-Type": "application/json"}

    # 创建提示词，明确要求只返回SQL语句
    prompt = (
        "你是一个SQL专家，根据用户的问题生成SQL查询语句。"
        "只返回SQL语句，不要包含任何其他解释或文本。"
        "数据库表名：device_metrics_random"
        "数据库表结构：(timestamp(yyyy/mm/dd xx:xx:xx)、cpu_temp(float)、cpu_usage(float)、memory_usage(float)、disk_usage(float)、network_up(float)、network_down(float))\n\n"
        f"用户问题: {query}\n\nSQL:"
    )

    data = {
        "messages": [
            {"role": "system", "content": "你是一个SQL生成助手，只返回SQL语句，不要包含任何其他内容。"},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 200,
        "temperature": 0.2  # 降低随机性，确保输出稳定
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # 检查HTTP错误

        temp = response.json()["choices"]
        for i in temp:
            print(i)
        # 提取生成的SQL
        sql_response = temp[0]["message"]["content"]

        #result = re.sub(r'<think>.*?</think>\s*', '', sql_response, flags=re.DOTALL)

        # 清理响应，只保留SQL语句
        # # 尝试提取代码块中的SQL
        # if "```sql" in sql_response:
        #     sql_response = sql_response.split("```sql")[1].split("```")[0].strip()
        # # 尝试提取第一行SQL
        # elif "SELECT" in sql_response:
        #     sql_response = sql_response.split("\n")[0].strip()
        # 提取 SQL 语句
        pattern = r'```sql\n(.*?);\n```'
        match = re.search(pattern, sql_response, re.DOTALL)
        if match:
            sql_query = match.group(1).strip()
        elif str(sql_response).startswith('SELECT'):
            sql_query = str(sql_response).strip()
        else:
            sql_query = "SELECT * FROM device_metrics_random.csv LIMIT 10"
        return sql_query
    except Exception as e:
        print(f"调用Llama Server失败: {str(e)}")
        # 失败时返回默认SQL
        return "SELECT * FROM device_metrics_random.csv LIMIT 10"

# 将查询结果翻译为自然语言
# def translate_to_natural_language(results, user_query):

    # if not results:
    #     return "没有找到匹配的记录。"
    #
    # # 分析结果
    # normal_count = sum(1 for r in results if r['status'] == '正常')
    # warning_count = sum(1 for r in results if r['status'] == '警告')
    # danger_count = sum(1 for r in results if r['status'] == '危险')
    # device_counts = {}
    #
    # for r in results:
    #     device = r['device']
    #     device_counts[device] = device_counts.get(device, 0) + 1
    #
    # # 根据查询类型生成不同的描述
    # if '统计' in user_query or '数量' in user_query or '次数' in user_query:
    #     return f"根据您的查询，共找到 {len(results)} 条记录。其中：\n" \
    #            f"- 正常状态: {normal_count} 个\n" \
    #            f"- 警告状态: {warning_count} 个\n" \
    #            f"- 危险状态: {danger_count} 个"
    #
    # elif '导出' in user_query or '下载' in user_query:
    #     return f"已成功导出 {len(results)} 条记录到 output.csv 文件。"
    #
    # else:
    #     # 生成自然语言描述
    #     response = f"为您找到 {len(results)} 条相关记录：\n\n"
    #
    #     for i, r in enumerate(results[:5]):  # 只显示前5条详细记录
    #         time_str = r['timestamp'].split(' ')[1][:5]  # 只取时间部分
    #         response += f"{i + 1}. {time_str} {r['device']} 的 {r['sensor']}传感器: {r['value']} ({r['status']}状态)\n"
    #
    #     if len(results) > 5:
    #         response += f"\n...等 {len(results)} 条记录"
    #
    #     response += f"\n\n分析摘要：\n"
    #     response += f"- 共有 {len(device_counts)} 台设备存在相关记录\n"
    #
    #     if danger_count > 0:
    #         response += f"⚠️ 发现 {danger_count} 个危险状态，建议立即检查相关设备！"
    #     elif warning_count > 0:
    #         response += f"⚠️ 发现 {warning_count} 个警告状态，建议安排设备检查。"
    #     else:
    #         response += "所有设备状态正常，无异常情况。"
    #
    #     return response

def upload_file(file_path, url, headers):
    with open(file_path, 'r', encoding='utf-8') as file:
        file_data = file.read()

    payload = {
        "messages": [
            {"role": "user", "content": "这是我的数据库内容，请你把其中的每一行解释为自然语言："+file_data}
        ],
        "max_tokens": 30000
    }

    response = requests.post(url, json=payload, headers=headers)
    return response.json()

# DuckDB查询执行
def execute_query(sql, user_query):
    try:
        new_sql = re.sub(r"FROM\s+([^\s]+)", r'FROM "\1.csv"', sql)
        print("\n" + "=" * 50)
        print("生成的SQL语句:")
        print(new_sql)
        print("=" * 50 + "\n")

        # 执行DuckDB查询
        duckdb.read_csv('device_metrics_random.csv')
        duckdb.query(new_sql).write_csv('out.csv')

        # 读取生成的CSV文件内容
        with open('out.csv', 'r') as f:
            csv_content = f.read()

        # 调用LLM API获取自然语言解释
        response_data = upload_file('out.csv', url, headers)

        # 确保response_data是字符串格式
        if isinstance(response_data, dict):
            natural_language = response_data.get('choices', [{}])[0].get('message', {}).get('content', '无法解析响应')
        else:
            natural_language = str(response_data)

        # 统计结果数量
        result_count = len(csv_content.splitlines()) - 1  # 减去表头行

        return {
            'sql': sql,
            'natural_language': natural_language,
            'result_count': result_count,
            'execution_time': round(random.uniform(0.15, 0.55), 3),
            # 'csv_content': csv_content  # 可选：如果需要在前端显示原始数据
        }

    except Exception as e:
        print(f"查询执行出错: {str(e)}")
        return {
            'error': str(e),
            'natural_language': f"查询执行出错: {str(e)}",
            'result_count': 0,
            'execution_time': 0
        }


# 获取历史记录
def get_query_history(limit=5):
    c.execute("SELECT * FROM history ORDER BY timestamp DESC LIMIT ?", (limit,))
    return c.fetchall()


# 系统资源监控数据
system_stats = {
    'cpu': deque(maxlen=60),
    'memory': deque(maxlen=60),
    'disk': deque(maxlen=60),
    'network': deque(maxlen=60)
}

# 系统资源监控数据
system_stats = {
    'cpu': deque(maxlen=60),
    'memory': deque(maxlen=60),
    'disk': deque(maxlen=60),
    'network': deque(maxlen=60),
    'last_net_io': None
}

# 更新系统监控数据的函数
def update_system_stats():
    while True:
        try:
            # 获取CPU使用率
            cpu_percent = psutil.cpu_percent()

            # 获取内存使用率
            mem = psutil.virtual_memory()
            mem_percent = mem.percent

            # 获取磁盘使用率（根目录）
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent

            # 获取网络活动（计算每秒流量）
            net_io = psutil.net_io_counters()
            net_bytes = net_io.bytes_sent + net_io.bytes_recv

            # 计算网络流量变化（百分比）
            net_percent = 0
            if system_stats['last_net_io'] is not None:
                bytes_diff = net_bytes - system_stats['last_net_io']
                # 假设1MB/s为100% (可调整)
                net_percent = min(100, bytes_diff / (1024 * 1024) * 100)
            system_stats['last_net_io'] = net_bytes

            # 更新数据
            system_stats['cpu'].append(cpu_percent)
            system_stats['memory'].append(mem_percent)
            system_stats['disk'].append(disk_percent)
            system_stats['network'].append(net_percent)

        except Exception as e:
            print(f"更新系统资源时出错: {e}")

        # 每1秒更新一次
        time.sleep(1)


# 启动后台线程更新系统资源
monitor_thread = threading.Thread(target=update_system_stats, daemon=True)
monitor_thread.start()


# 添加API端点获取系统资源数据
@app.route('/api/system_stats')
def get_system_stats():
    return jsonify({
        'cpu': list(system_stats['cpu']),
        'memory': list(system_stats['memory']),
        'disk': list(system_stats['disk']),
        'network': list(system_stats['network']),
        'timestamp': datetime.datetime.now().isoformat()
    })

# 初始化系统监控数据
def init_system_stats():
    for _ in range(60):
        system_stats['cpu'].append(random.randint(15, 45))
        system_stats['memory'].append(random.randint(40, 75))
        system_stats['disk'].append(random.randint(20, 50))
        system_stats['network'].append(random.randint(10, 40))


init_system_stats()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/query', methods=['POST'])
def process_query():
    user_query = request.form['query']
    # 生成SQL
    generated_sql = generate_sql_from_query(user_query)

    # 执行查询
    result = execute_query(generated_sql, user_query)

    # 保存到历史记录
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    c.execute("INSERT INTO history (timestamp, user_query, generated_sql, natural_language_result) VALUES (?, ?, ?, ?)",
              (timestamp, user_query, generated_sql, result['natural_language']))
    conn.commit()

    # 更新系统监控数据
    system_stats['cpu'].append(random.randint(15, 65))
    system_stats['memory'].append(random.randint(40, 85))
    system_stats['disk'].append(random.randint(20, 60))
    system_stats['network'].append(random.randint(10, 50))

    # # 添加系统监控数据到结果
    # result['system_stats'] = {
    #     'cpu': list(system_stats['cpu']),
    #     'memory': list(system_stats['memory']),
    #     'disk': list(system_stats['disk']),
    #     'network': list(system_stats['network'])
    # }

    # 获取历史记录
    history = get_query_history()
    result['history'] = history

    return jsonify(result)


@app.route('/history')
def get_history():
    history = get_query_history(10)
    return jsonify([{
        'id': row[0],
        'timestamp': row[1],
        'user_query': row[2],
        'generated_sql': row[3],
        'natural_language_result': row[4]
    } for row in history])

@app.route('/api/device_status')
def get_device_status():
    try:
        # 获取系统资源数据（示例）
        cpu_percent = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory()

        return jsonify({
            "cpu": cpu_percent,
            "memory": mem.percent,
            "status": "online",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            "error": str(e),
            "status": "offline"
        }), 500

# 在app.py中添加设备专用监控接口
@app.route('/api/custom_device')
def custom_device():
    import requests
    try:
        # 调用设备API（示例）
        resp = requests.get('http://127.0.0.1/api/status', timeout=3)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)