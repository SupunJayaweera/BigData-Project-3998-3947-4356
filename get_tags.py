import urllib.request
import json
try:
    url = "https://registry.hub.docker.com/v2/repositories/bitnami/spark/tags?page_size=20"
    response = urllib.request.urlopen(url)
    data = json.loads(response.read().decode('utf-8'))
    for item in data.get('results', []):
        print(item.get('name'))
except Exception as e:
    print(f"Error: {e}")
