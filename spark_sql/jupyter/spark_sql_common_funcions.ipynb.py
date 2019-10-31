from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import asc, desc

sqlContext = SQLContext(sc)

movieText= sc.textFile('/user/cloudera/movies.dat')
ratingText= sc.textFile('/user/cloudera/ratings.dat')

ratingsData = ratingText.map(lambda line: line.split(':'))
movieData = movieText.map(lambda line: line.split(':'))


