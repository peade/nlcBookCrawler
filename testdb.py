from mongoOperate import dataToMongo

op = dataToMongo()
alldata = op.getAll()
for i in alldata:
    print(i)
