#!/usr/bin/env python
# coding: utf-8

# <h1><center>SparkFlix Analytics: Exploring Movie Reviews with PySpark and SQL </center></h1>
# 
# # Sai Sanwariya Narayan
# 
# ## The goals of this project is:
# ### - Use Data Frames in Spark for Processing Structured Data
# ### - Perform Basic DataFrame Transformations such as filtering rows and selecting columns of DataFrame
# ### - Create New Columns of DataFrame using `withColumn`
# ### - Use DF SQL Functions to transform a string into an Array
# ### - Filter on a DF Column that is an Array using `array_contains`
# ### - Perform Join operations on DataFrames 
# ### - Use groupBy, followed by count and sum DF transformation to calculate the count and the sum of a DF column (e.g., reviews) for each group (e.g., movie).
# ### - Perform sorting on a DataFrame column
# ### - Apply the obove to find Movies in a Genre of your choice that has good reviews with a significant number of ratings (use 10 as the threshold for local mode, 100 as the threshold for cluster mode).
# ### - After completing all exercises in the Notebook, convert the code for processing large reviews dataset and large movies dataset to find movies with top average ranking with at least 100 reviews for a genre of comedy.

# ## Importing pyspark first.

# In[1]:


import pyspark


# ### Once we import pyspark, we need to import "SparkContext".  Every spark program needs a SparkContext object
# ### In order to use Spark SQL on DataFrames, we also need to import SparkSession from PySpark.SQL

# In[2]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, FloatType
from pyspark.sql.functions import col, column
from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql import Row


# ## We then create a Spark Session variable (rather than Spark Context) in order to use DataFrame. 

# In[3]:


ss=SparkSession.builder.appName("Lab 5 Top Reviews").getOrCreate()


# In[4]:


ss.sparkContext.setCheckpointDir("~/scratch")


# In[5]:


rating_schema = StructType([ StructField("UserID", IntegerType(), False ),                             StructField("MovieID", IntegerType(), True),                             StructField("Rating", FloatType(), True ),                             StructField("RatingID", IntegerType(), True ),                            ])


# In[6]:


ratings_DF = ss.read.csv("/storage/home/ratings_2.csv", schema= rating_schema, header=True, inferSchema=False)
# In the cluster mode, we need to change to  `header=False` because it does not have header.


# In[7]:


movie_schema = StructType([ StructField("MovieID", IntegerType(), False),                             StructField("MovieTitle", StringType(), True ),                             StructField("Genres", StringType(), True ),                            ])


# In[8]:


movies_DF = ss.read.csv("/storage/home/movies_large.csv", schema=movie_schema, header=False, inferSchema=False)
# In the cluster mode, we need to change to `header=False` because it does not have header.


# In[9]:


#movies_DF.printSchema()


# In[10]:


#movies_DF.show(10)


# In[11]:


movies_genres_DF = movies_DF.select("MovieID","Genres")


# In[12]:


movies_genres_rdd = movies_genres_DF.rdd


# In[13]:


movies_genres_rdd.take(3)


# In[14]:


movies_genres2_rdd = movies_genres_rdd.flatMap(lambda x: x['Genres'].split('|'))


# In[15]:


movies_genres2_rdd.take(3)


# In[16]:


movies_genres3_rdd = movies_genres2_rdd.map(lambda x: (x, 1))


# In[17]:


movies_genres_count_rdd = movies_genres3_rdd.reduceByKey(lambda x, y: x+y)


# In[18]:


#movies_genres_count_rdd.take(10)


# In[19]:


movies_genres_count_rdd.saveAsTextFile("/storage/home/MovieGenres_count_Cluster.txt")


# In[20]:


#ratings_DF.printSchema()


# In[21]:


#ratings_DF.show(5)


# # 2. DataFrames Transformations
# DataFrame in Spark provides higher-level transformations that are convenient for selecting rows, columns, and for creating new columns.  These transformations are part of Spark SQL.
# 
# ## 2.1 `where` DF Transformation for Filtering/Selecting Rows
# Select rows from a DataFrame (DF) that satisfy a condition.  This is similar to "WHERE" clause in SQL query language.
# - One important difference (compared to SQL) is we need to add `col( ...)` when referring to a column name. 
# - The condition inside `where` transformation can be an equality test, `>` test, or '<' test, as illustrated below.

# # `show` DF action
# The `show` DF action is similar to `take` RDD action. It takes a number as a parameter, which is the number of elements to be randomly selected from the DF to be displayed.

# In[22]:


#movies_DF.where(movies_DF["MovieTitle"] == "Toy Story (1995)").show()


# In[23]:


#ratings_DF.where(ratings_DF["Rating"] > 3).show(5)


# # `count` DF action
# The `count` action returns the total number of elements in the input DataFrame.

# In[24]:


ratings_DF.filter(4 < ratings_DF["Rating"]).count()


# # Filtering DF Rows

# In[25]:


review_5_count = ratings_DF.where(5 == col("Rating")).count()
print(review_5_count)


# # DataFrame Transformation for Selecting Columns
# 
# DataFrame transformation `select` is similar to the projection operation in SQL: it returns a DataFrame that contains all of the columns selected.

# In[26]:


#movies_DF.select("MovieTitle").show(5)


# In[27]:


#movies_DF.select(col("MovieTitle")).show(5)


# # Selecting DF Columns

# In[28]:


movie_rating_DF = ratings_DF.select(col("MovieID"),col("Rating"))


# In[29]:


#movie_rating_DF.show(5)


# # Statistical Summary of Numerical Columns
# DataFrame provides a `describe` method that provides a summary of basic statistical information (e.g., count, mean, standard deviation, min, max) for numerical columns.

# In[30]:


#ratings_DF.describe().show()


# ## RDD has a histogram method to compute the total number of rows in each "bucket".
# The code below selects the Rating column from `ratings_DF`, converts it to an RDD, which maps to extract the rating value for each row, which is used to compute the total number of reviews in 5 buckets.

# In[31]:


ratings_DF.select(col("Rating")).rdd.map(lambda row: row[0]).histogram([0,1,2,3,4,5,6])


# # Transforming the Generes Column into Array of Generes 
# ## We want transform a column Generes, which represent all Generes of a movie using a string that uses "|" to connect the Generes so that we can later filter for movies of a Genere more efficiently.
# ## This transformation can be done using `split` Spark SQL function (which is different from python `split` function)

# In[32]:


Splitted_Generes_DF= movies_DF.select(split(col("Genres"), '\|'))
#Splitted_Generes_DF.show(5)


# ## Adding a Column to a DataFrame using withColumn
# 
# # `withColumn` DF Transformation
# 
# We often need to transform content of a column into another column. For example, it is desirable to transform the column Genres in the movies DataFrame into an `Array` of genres that each movie belongs, we can do this using the DataFrame method `withColumn`.

# ### Creates a new column called "Genres_Array", whose values are arrays of genres for each movie, obtained by splitting the column value of "Genres" for each row (movie).

# In[33]:


moviesG2_DF= movies_DF.withColumn("Genres_Array",split("Genres", '\|') )


# In[34]:


#moviesG2_DF.printSchema()


# In[35]:


#moviesG2_DF.show(5)


# # Choosing Comedy as Genre

# In[36]:


from pyspark.sql.functions import array_contains
movies_your_genre_DF = moviesG2_DF.filter(array_contains(moviesG2_DF["Genres_Array"], "Comedy" ))


# In[37]:


#movies_your_genre_DF.show(5)


# # An DF-based approach to compute Average Movie Ratings and Total Count of Reviews for each movie.

# # `groupBy` DF transformation
# Takes a column name (string) as the parameter, the transformation groups rows of the DF based on the column.  All rows with the same value for the column is grouped together.  The result of groupBy transformation is often folled by an aggregation across all rows in the same group.  
# 
# # `sum` DF transformation
# Takes a column name (string) as the parameter. This is typically used after `groupBy` DF transformation, `sum` adds the content of the input column of all rows in the same group.
# 
# # `count` DF transformation
# Returns the number of rows in the DataFrame.  When `count` is used after `groupBy`, it returns a DataFrame with a column called "count" that contains the total number of rows for each group generated by the `groupBy`.

# In[38]:


Movie_RatingSum_DF = ratings_DF.groupBy("MovieID").sum("Rating")


# In[39]:


#Movie_RatingSum_DF.show(4)


# # Calculating the total number of reviews for each movies.

# In[40]:


Movie_RatingCount_DF = ratings_DF.groupBy(col("MovieID")).count()


# In[41]:


#Movie_RatingCount_DF.show(4)


# # Join Transformation on Two DataFrames

# In[42]:


Movie_Rating_Sum_Count_DF = Movie_RatingSum_DF.join(Movie_RatingCount_DF, "MovieID", 'inner')


# In[43]:


#Movie_Rating_Sum_Count_DF.show(4)


# In[44]:


Movie_Rating_Count_Avg_DF = Movie_Rating_Sum_Count_DF.withColumn("AvgRating", (col("sum(Rating)") / col("count")) )


# In[45]:


#Movie_Rating_Count_Avg_DF.show(4)


# ##  Next, we want to join the avg_rating_total_review_DF with moviesG2_DF

# In[46]:


joined_DF = Movie_Rating_Count_Avg_DF.join(moviesG2_DF,'MovieID', 'inner')


# In[47]:


#moviesG2_DF.printSchema()


# In[48]:


#joined_DF.printSchema()


# In[49]:


#joined_DF.show(4)


# # Filter DataFrame on an Array Column of DataFrame Using `array_contains`

# In[50]:


from pyspark.sql.functions import array_contains
SelectGenreAvgRating_DF = joined_DF.filter(array_contains('Genres_Array',                                                "Comedy")).select("MovieID","AvgRating","count","MovieTitle")


# In[51]:


#SelectGenreAvgRating_DF.show(5)


# In[52]:


SelectGenreAvgRating_DF.count()


# In[53]:


#SelectGenreAvgRating_DF.describe().show()


# In[54]:


SortedSelectGenreAvgRating_DF = SelectGenreAvgRating_DF.orderBy('AvgRating', ascending=False)


# In[55]:


#SortedSelectGenreAvgRating_DF.show(10)


# ### Use DataFrame method `where` or `filter` to find all movies (in your choice of genre) that have more than 10 reviews (change this to 100 for the cluster mode).

# In[56]:


SortedFilteredSelectGenreAvgRating_DF = SortedSelectGenreAvgRating_DF.where(col("count") > 10)


# In[57]:


#SortedFilteredSelectGenreAvgRating_DF.show(5)


# ## Cluster mode tests with Comedy genre.

# In[58]:


output_path = "/storage/home/SortedFilteredComedyMovieAvgRating_Cluster"
SortedFilteredSelectGenreAvgRating_DF.write.csv(output_path)


# In[59]:


ss.stop()


# - Choice of the genre for analysis 
# 
#      - Comedy
#      
#     _____
# 
# - Top five movies in the genre?
#     - 1. Life Is Beautiful (La Vita è bella) (1997) 
#     - 2. Pulp Fiction (1994)
#     - 3. Monty Python and the Holy Grail (1975)
#     - 4. Thin Man, The (1934)
#     - 5. Sting, The (1973)
#     
#     _____
# 
# - The computation time in Cluster mode took: 
# 
#     - real 	 1m 10.355s
# 
# 
# 

# In[ ]:





# In[ ]:




