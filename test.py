import requests

url = "http://localhost:8080/v1/chat/completions"
headers = {"Content-Type": "application/json"}
data = {
    "messages": [
        {"role": "user", "content": "解释一下量子计算"}
    ],
    "max_tokens": 300
}

response = requests.post(url, headers=headers, json=data)
print(response.json()["choices"][0]["message"]["content"])