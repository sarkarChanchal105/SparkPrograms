# Import the necessary libraries
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

## Create the Spark Context with the config to run the program in standalone more
conf= SparkConf().setAppName("SimpleJoin").setMaster("local")
sc=SparkContext(conf=conf)

## Create the RDD from the file
fileA= sc.textFile('../input/join1_FileA.txt')

print("Printind FileA.........")
print(fileA.collect())
print("....End .............\n")

## Create the RDD from the file
fileB = sc.textFile('../input/join1_FileB.txt')

print("Printind FileB.........")
print (fileB.collect())

print("....End .............\n")

## The function returns the key and value of file A
def split_fileA(line):
    word=line.split(',')[0]
    count = int(line.split(',')[1])

    return (word,count)

## Create the RDD using the map function. Split the key and value
fileA_data = fileA.map(split_fileA)
print("After applying the  function....")
print(fileA_data.collect())

## The function returns the key and value of file B
def split_fileB(line):
    date=line.split(',')[0].split(' ')[0]
    word=line.split(',')[0].split(' ')[1]
    count_string=(line.split(',')[1])
    #count_string=int(line.split(',')[2])
    return (word, date + " " + count_string)

## Create the RDD using the map function. Split the key and value
fileB_data = fileB.map(split_fileB)

print("After applying the  function....")
print(fileB_data.collect())

print ("Running a Join.........")

## Join the RDD based on the joining keys.
fileB_joined_fileA = fileB_data.join(fileA_data)

print("Printing the join result.....")

print(fileB_joined_fileA.collect())

for k,v in fileB_joined_fileA.collect():
    print(k,v)
