from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

sc = SparkContext.getOrCreate(conf)
lines = sc.textFile("file:///G:/SCALA-SPARK/InputFiles/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():

 print "%s %i" % (key,value)

!pip install pydotplus
Requirement already satisfied: pydotplus in c:\users\saurabh\appdata\local\enthought\canopy\edm\envs\user\lib\site-packages
Requirement already satisfied: pyparsing>=2.0.1 in c:\users\saurabh\appdata\local\enthought\canopy\edm\envs\user\lib\site-packages (from pydotplus)

!pip install pyspark
Requirement already satisfied: pyspark in c:\users\saurabh\appdata\local\enthought\canopy\edm\envs\user\lib\site-packages
Requirement already satisfied: py4j==0.10.4 in c:\users\saurabh\appdata\local\enthought\canopy\edm\envs\user\lib\site-packages (from pyspark)