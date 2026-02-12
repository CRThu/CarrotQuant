import requests
import json

url = "http://localhost:8000/api/v1/market/tasks/download"
payload = {"table_name": "cn_stock_em"}

try:
    response = requests.post(url, json=payload)
    print(f"Status: {response.status_code}")
    print(response.json())
except Exception as e:
    print(f"Error: {e}")
