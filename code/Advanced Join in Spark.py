
# Import the necessary libraries
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

## Create the Spark Context with the config to run the program in standalone more
conf= SparkConf().setAppName("AdvancedJoin").setMaster("local")
sc=SparkContext(conf=conf)

## Create the RDD from the file
show_views_file= sc.textFile('../input/join2_gennum?.txt')
print("First few records of show views file")
print(show_views_file.take(2))

## Returns the key and value in a tuple
def split_show_views(line):
    show=line.split(',')[0]
    views=int(line.split(',')[1])
    return (show, views)

## Returns the key and value in a tuple
def split_show_channel(line):
    show = line.split(',')[0]
    channel = (line.split(',')[1])
    return (show, channel)

## Create a new RDD using the map function.
show_views=show_views_file.map(split_show_views)

print("Printing RDD of show viwws after prcessing")
print(show_views.take(5))


## Create the RDD from the file
show_channel_file = sc.textFile('../input/join2_genchan?.txt')
print("First few records of show channel file")
print(show_channel_file.take(2))

## Create a new RDD using the map function.
show_channel = show_channel_file.map(split_show_channel)

print("Printing RDD of show channel file after processing")
print(show_channel.take(5))

## join the two datasets
joined_dataset = show_channel.join(show_views)

print("Printing the joined data set..")
print(joined_dataset.take(5))

## get the
def extract_channel_views(show_views_channel):
    channel,views=show_views_channel[1]
    return (channel, views)

## Join the dataset
channel_views = joined_dataset.map(extract_channel_views)


print("Printing after extracting channel and viewers...")
print(channel_views.take(5))

def some_function(a, b):
    some_result=int(a)+int(b)
    return some_result


print("Print total number of viwerws by channel...")

print(channel_views.reduceByKey(some_function).collect())

