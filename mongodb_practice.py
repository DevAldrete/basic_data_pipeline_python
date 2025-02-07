from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://aldretelearns:learn15escrub@cluster0.6vd90.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client: MongoClient = MongoClient(uri, server_api=ServerApi("1"))

list_databases = client.list_databases()

for db in list_databases:
    print(db)

