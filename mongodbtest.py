import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = client['testdb']
mytable = mydb['book']
mydict = {
    "name": "RUNOOB", "alexa": "10000", "url": "https://www.runoob.com"
}
query = {"name": "RUNOOB"}
mycol.delete_one(query)
# x = mycol.insert_one(mydict)
x = mycol.find_one()
print(x)
