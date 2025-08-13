from flask import Flask, render_template, request, jsonify
import sqlite3
import random
import time
import datetime
import re
from collections import deque
import psutil
import threading
import requests  # 添加requests库用于API调用
import duckdb

# 调用本地部署的Llama Server
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

# 使用Llama Server生成SQL
def generate_sql_from_query(query):
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