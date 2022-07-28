import pd as pd
import tweepy
from pyspark.shell import spark
import csv
from csv import writer
import seaborn as sns
import matplotlib.pyplot as plt
import config
import re
import pandas as pd

client = tweepy.Client(bearer_token=config.BEARER_TOKEN)
print('Welcome to Twitter Analytics application!')
query = input('Enter the keyword to get the tweets:')
hashtag_list = []
url_list = []
data1 = []
textfile1 = open("Url_file.txt", "w")
filename = open("tweetdata.csv", "w")
csvwriter = csv.writer(filename)


def word_count(str):
    counts = dict()
    words = str.split()
    for word in words:
        if word in counts:
            counts[word] += 1
        else:
            counts[word] = 1
    return counts


def extract_hashtags(text):
    # initializing hashtag_list variable

    # splitting the text into words
    for word in text.split():

        # checking the first character of every word
        if word[0] == '#':
            # adding the word to the hashtag_list
            hashtag_list.append(word[1:])


def extract_urls(texts):
    p2 = re.compile(r'(?:http|ftp|https)://(?:[\w_-]+(?:(?:\.[\w_-]+)+))(?:[\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?')
    for word in texts.split():
        # urls = re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', word)
        data = p2.findall(word)
        if len(data) > 0:
            url_list.append(data)
            for elements in data:
                textfile1.write(elements + "\n")


def addtweetstocsv():
    # Open file in append mode
    with open('a.csv', 'w', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        for item in data1:
            csv_writer.writerow([item])


def addtweetstosql():
    df = (spark.read.format("csv")
          .option("inferSchema", "true")
          .load('a.csv'))
    df.createOrReplaceTempView("us_delay_flights_tbl")
    df2 = spark.sql('select * from us_delay_flights_tbl')
    df2.show()


def addhashtagstocsv(hashtags):
    df = pd.DataFrame([(k, v) for k, v in hashtags.items()], columns=['HashTag', 'Count'])
    df.to_csv('hash.csv', sep=',')


def addhashtagstosql():
    df = (spark.read.format("csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load('hash.csv'))
    df.createOrReplaceTempView("Hashtagscount")
    df2 = spark.sql('select * from Hashtagscount')
    df2.show()


def plotdiagrams():
    df = (spark.read.format("csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load('hash.csv'))
    df.createOrReplaceTempView("Hashtagscount")
    df2 = spark.sql('select * from Hashtagscount')
    df2.show()
    df2 = df2.toPandas()
    df2.set_index('Count').plot()
    plt.rcParams["figure.figsize"] = [7.50, 3.50]
    plt.rcParams["figure.autolayout"] = True
    plt.show()
    plt.title("Tweet Count with Bar plot")
    plt.bar(df2['Count'], height=df2['_c0'])
    plt.show()

def plotheatmap():
    df = (spark.read.format("csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load('hash.csv'))
    df.createOrReplaceTempView("Hashtagscount")
    df2 = spark.sql('select Count from Hashtagscount')
    df2.show()
    df2 = df2.toPandas()
    sns.heatmap(df2)
    plt.title("Tweet Count with heat map")
    plt.show()

def sqlqueries():
    df = (spark.read.format("csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load('hash.csv'))
    df.createOrReplaceTempView("Hashtagscount")
    res =spark.sql('select * from Hashtagscount where Count>5')
    print('Results for values greater than 5')
    res.show()
    res1 = spark.sql('select * from Hashtagscount where Count>10')
    print('Results for values greater than 10')
    res1.show()
    res2 = spark.sql('select * from Hashtagscount where Count<5')
    print('Results for values less than 5')
    res2.show()
    res3 = spark.sql('select COUNT(HashTag) from Hashtagscount')
    print('Results for  Count of totoal results')
    res3.show()
    res4 = spark.sql('select * from Hashtagscount')
    print('Print all the results from the table')
    res4.show()
    res5 = spark.sql('select * from Hashtagscount where HashTag=="USA"')
    print('Tweets with Usa')
    res5.show()
    res6 =spark.sql('select * from Hashtagscount where Count==10')
    print('Results for values equal to 10')
    res6.show()
    res7 =spark.sql('select * from Hashtagscount where Hashtag LIKE "a%"')
    print('Tweets starts with "A:"')
    res7.show()
    res8 = spark.sql('select * from Hashtagscount where Count BETWEEN 10 AND 20')
    print('Results for tags between 10 and 20')
    res8.show()

    res9 = spark.sql('select * from Hashtagscount LIMIT 3')
    print('Selecting first three rows')
    res9.show()




for tweet in tweepy.Paginator(client.search_recent_tweets, query=query, max_results=100).flatten(limit=100):
    extract_hashtags(tweet.text)
    extract_urls(tweet.text)
    data1.append(tweet.text)

df1 = pd.DataFrame(hashtag_list, columns=['hashtags'])
df2 = pd.DataFrame(url_list, columns=['urls'])

textfile = open("Hash_file.txt", "w")
for element in hashtag_list:
    textfile.write(element + "\n")
textfile.close()

print('Hashtag Count:')
print(df1.count())
print('Url count:')
print(df2.count())

file = open("Hash_file.txt", "r")
data = file.read()

wordcount = word_count(data)
print(wordcount)
addtweetstocsv()
addtweetstosql()
addhashtagstocsv(wordcount)
addhashtagstosql()
plotdiagrams()
plotheatmap()
sqlqueries()
