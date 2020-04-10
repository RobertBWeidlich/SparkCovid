#
# todo: run this as a Python script
#
in_data_file = \
  '/data1/covid/humdata/20200328/time_series_covid19_deaths_global.csv'

# Convert CSV file to DataFrame
cv_df = spark.read.option('inferSchema', 'true') \
                  .option('header', 'true')      \
                  .csv(in_data_file)

# explain DF - use this to make sure that inferSchema works correctly
cv_df.explain()

# data dump of DF
cv_df.take(3)

# sort DF
cv_df_sorted = cv_df.sort('Country/Region')

# DataFrame queries
#dfQueryResults_01 = cv_df.groupBy('Country/Region').count()

# Group By
dfQueryResults_01 = cv_df.groupBy('Country/Region').count()

# Group By, sort by count descending
dfQueryResults_01.sort('count').show(20)

# Group By, sort by count descending, show count > 1

# pretty print the table
dfQueryResults_01.show()
dfQueryResults_01.show(200)

# todo
#   1. sort in descending order
#   2. show only those where count > 1
dfQueryResults_01.sort('count', ascending=False).show(20)

# SQL queries

# create a view so we can perform SQL queries on the DataFrame
cv_df.createOrReplaceTempView('cv_country')










