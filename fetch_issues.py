import requests
import json

token = "COLOQUE_SEU_GITHUB_PAT_AQUI"
headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
url = "https://api.github.com/repos/Estatistica-Praticagem/prev_ml_correnteza/issues?state=all"
response = requests.get(url, headers=headers)
if response.status_code == 200:
    for issue in response.json():
        print(f"#{issue['number']} - {issue['title']}")
        print(issue['body'])
        print("-" * 50)
else:
    print(f"Error {response.status_code}: {response.text}")

