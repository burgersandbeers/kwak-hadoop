from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import asc, desc

sqlContext = SQLContext(sc)

movieText= sc.textFile('/user/cloudera/movies.dat')
ratingText= sc.textFile('/user/cloudera/ratings.dat')

ratingsData = ratingText.map(lambda line: line.split(':'))
movieData = movieText.map(lambda line: line.split(':'))

movies = movieData.map(lambda r: Row(user_id=int(r[0]), movie_id=int(r[1]) ,rating=int(r[2]) ,timestamp=r[3]))


