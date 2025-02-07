import requests
import os

API_KEY_NEWS = os.getenv("API_KEY_NEWS")

response = requests.get(
    f"https://newsapi.org/v2/top-headlines?q=Nvidia&country=us&category=technology&apiKey={API_KEY_NEWS}"
)

print(response.json())
