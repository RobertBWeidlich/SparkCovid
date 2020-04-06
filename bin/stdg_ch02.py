#
# from Spark The Definitive Guide, Chapter 2
#

flightData2015 = spark.read.option('inferSchem', 'true').option('header', 'true').csv('/home/weidlich/Desktop/Projects/Projects2020/Spark-The-Definitive-Guide/data/flight-data/csv/2010-summary.csv')

print(flightData2015)

flightData2015.take(20)

type(flightData2015)
type(flightData2015).take(7)
flightData2015.take(7)
flightData2015.sort('ORIGIN_COUNTRY_NAME')

flightData2015.sort('ORIGIN_COUNTRY_NAME').take(3)
flightData2015.sort('ORIGIN_COUNTRY_NAME').explain()

sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DATE_COUNTRY_NAME).count()
)

sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

flightData2015.createOrReplaceTempView('flight_data_2015')

sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, COUNT(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()
dataFrameWay

sqlWay.explain()
print(sqlWay)


maxSql = spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count) as dest_total
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY sum(count) DESC
    LIMIT 20
""")

maxSql.show()

