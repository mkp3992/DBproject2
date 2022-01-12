import pandas as pd
import pymongo
from pymongo import MongoClient


class MongoDB(object):

    def __init__(self, dBName=None, collectionName=None):

        self.dBName = dBName
        self.collectionName = collectionName
        self.client = MongoClient("localhost", 27017, maxPoolSize=50)
        self.DB = self.client[self.dBName]
        self.collection = self.DB[self.collectionName]

    def InsertData(self, path=None):

        df = pd.read_csv(path)
        data = df.to_dict('records')

        self.collection.insert_many(data, ordered=False)
        print("All the Data has been Exported to Mongo DB Server .... ")


def department():
    client = pymongo.MongoClient('mongodb://localhost:27017')
    mydb = client['company']
    mycol = mydb['department']
    mycol2 = mydb['resdep']
    result = mycol.aggregate([
        {
            '$lookup': {
                'from': 'employee',
                'localField': 'Mgr_ssn',
                'foreignField': 'Ssn',
                'as': 'EMP_DEP1'
            }
        },
        {
            '$lookup': {
                'from': 'employee',
                'localField': 'Dno',
                'foreignField': 'Dno',
                'as': 'EMP_DEP2'
            }
        },
        {
            '$project': {
                'Dname': 1,
                'EMP_DEP1.Lname': 1,
                'Date': 1,
                'EMP_DEP2.Lname': 1,
                'EMP_DEP2.Fname': 1,
                'EMP_DEP2.Salary': 1
            }
        }
    ])
    resultList = []
    for item in result:
        resultList.append(item)
        print(item)

    print("Result list length  :", len(resultList))
    x = mycol2.insert_many(resultList)


def employee():
    client = pymongo.MongoClient('mongodb://localhost:27017')
    mydb = client['company']
    mycol = mydb['employee']
    mycol2 = mydb['resemp']
    result = mycol.aggregate([
        {
            '$lookup': {
                'from': 'department',
                'localField': 'Dno',
                'foreignField': 'Dno',
                'as': 'EMP_DEP'
            }
        },
        {
            '$lookup': {
                'from': 'project',
                'localField': 'Dno',
                'foreignField': 'Dno',
                'as': 'EMP_PROJ'
            }
        },
        {
            '$lookup': {
                'from': 'works_on',
                'localField': 'Ssn',
                'foreignField': 'Essn',
                'as': 'PROJ_HOURS'
            }
        },
        {
            '$project': {
                'Fname': 1,
                'Lname': 1,
                'EMP_DEP.Dname': 1,
                'EMP_PROJ.Pname': 1,
                'EMP_PROJ.Pno': 1,
                'PROJ_HOURS.Total_hours': 1
            }
        }
    ])
    resultList = []
    for item in result:
        resultList.append(item)
        print(item)

    print("Result list length  :", len(resultList))
    x = mycol2.insert_many(resultList)

def project():
    client = pymongo.MongoClient('mongodb://localhost:27017')
    mydb = client['company']
    mycol = mydb['project']
    mycol2 = mydb['resproj']
    result = mycol.aggregate([
        {
            '$lookup': {
                'from': 'department',
                'localField': 'Dno',
                'foreignField': 'Dno',
                'as': 'PROJ_DEP'
            }
        },
        {
            '$lookup': {
                'from': 'employee',
                'localField': 'Dno',
                'foreignField': 'Dno',
                'as': 'PROJ_EMP'
            }
        },
        {
            '$lookup': {
                'from': 'works_on',
                'localField': 'Pno',
                'foreignField': 'Pno',
                'as': 'PROJ_HOURS'
            }
        },
        {
            '$project': {
                'Pname': 1,
                'Pno': 1,
                'PROJ_DEP.Dname': 1,
                'PROJ_EMP.Fname': 1,
                'PROJ_EMP.Lname': 1,
                'PROJ_HOURS.Total_hours': 1
            }
        }
    ])
    resultList = []
    for item in result:
        resultList.append(item)
        print(item)

    print("Result list length  :", len(resultList))
    x = mycol2.insert_many(resultList)

def samplequeries():
    client = pymongo.MongoClient('mongodb://localhost:27017')
    mydb = client['company']
    mycoll1 = mydb['resdep']
    mycoll2 = mydb['resemp']
    mycoll3 = mydb['resproj']
    f = open("sample.txt", "w")
    f.write("Executing query for finding columns corresponding to department research:-")
    f.write('\n')
    for i in mycoll1.find({'Dname': 'Software'}):
        print(i)
        f.write(str(i))
    f.write('\n')
    f.write('\n')

    f.write("Executing query for finding columns corresponding to project name reorganization:-")
    f.write('\n')
    for i in mycoll2.find({'Fname': "'James'"}, {'Lname': "'Borg'"}):
        print(i)
        f.write(str(i))
    f.write('\n')
    f.write('\n')

    f.write("Executing query for finding columns corresponding to first name and last name:-")
    f.write('\n')
    for i in mycoll3.find({'Pname': "'ProductX'"}):
        print(i)
        f.write(str(i))
    f.close()


if __name__ == "__main__":
    mongodb1 = MongoDB(dBName='company', collectionName='department')
    mongodb1.InsertData(path="/Users/mahipanchal/Downloads/DEPARTMENT.csv")

    mongodb2 = MongoDB(dBName='company', collectionName='employee')
    mongodb2.InsertData(path="/Users/mahipanchal/Downloads/EMPLOYEE.csv")

    mongodb3 = MongoDB(dBName='company', collectionName='project')
    mongodb3.InsertData(path="/Users/mahipanchal/Downloads/PROJECT.csv")

    mongodb4 = MongoDB(dBName='company', collectionName='works_on')
    mongodb4.InsertData(path="/Users/mahipanchal/Downloads/WORKS_ON.csv")

    project()
    department()
    employee()
    samplequeries()