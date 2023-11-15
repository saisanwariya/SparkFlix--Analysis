# SparkFlix Analytics: Exploring Movie Reviews with PySpark and SQL

## Overview

This project, created by Sai Sanwariya Narayan, utilizes PySpark and SQL for in-depth analysis of movie reviews. It is aimed at leveraging the power of DataFrames in Spark to process structured data and perform a variety of transformations and actions to gain insights into movie ratings and genres.

## Program Functionality

1. **DataFrame Operations**: Implementing basic DataFrame transformations like filtering rows, selecting columns, and creating new columns using `withColumn`.
2. **String to Array Transformation**: Utilizing DataFrame SQL functions to convert strings into arrays for efficient processing.
3. **Array Filtering**: Employing `array_contains` for filtering DataFrame columns that are arrays.
4. **DataFrame Join Operations**: Executing join operations on DataFrames to merge data from different sources.
5. **Aggregation and Grouping**: Using `groupBy`, `count`, and `sum` DataFrame transformations for aggregating data.
6. **Sorting and Analysis**: Performing sorting on DataFrame columns to rank movies based on criteria like average rating and review count.
7. **Application in Different Modes**: Adapting the code to process large datasets in both local and cluster modes. Specifically, finding top-rated movies in the comedy genre with at least 100 reviews in cluster mode.
8. **Tools and Technologies**: The project involves the use of PySpark, Spark SQL, and RDD (Resilient Distributed Dataset) transformations.

____

# Academic Integrity Statement

Please note that all work included in this project is the original work of the author, and any external sources or references have been properly cited and credited. It is strictly prohibited to copy, reproduce, or use any part of this work without permission from the author.

If you choose to use any part of this work as a reference or resource, you are responsible for ensuring that you do not plagiarize or violate any academic integrity policies or guidelines. The author of this work cannot be held liable for any legal or academic consequences resulting from the misuse or misappropriation of this work.

Any unauthorized copying or use of this work may result in serious consequences, including but not limited to academic penalties, legal action, and damage to personal and professional reputation. Therefore, please use this work only as a reference and always ensure that you properly cite and attribute any sources or references used.

____

## Notes

- The project offers insights into the practical application of PySpark and SQL in data processing and analysis.
- It is essential to have a basic understanding of Python, PySpark, and SQL to fully grasp the project's implementation.
- The project is flexible and can be adapted for different genres or criteria based on the user's interest.
- The choice of using DataFrame over RDD is driven by the need for more sophisticated operations and optimizations available in Spark SQL.
- The project showcases the strength of Spark in handling large-scale data efficiently, particularly in a distributed computing environment like a cluster.