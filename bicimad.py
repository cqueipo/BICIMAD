from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
import json
from pprint import pprint

junio2019 = spark.read.json('201906.json')
print ('- JUNIO 2019')
junio2019.show()
junio2019.count()

junio2020 = spark.read.json('202006.json')
print ('- JUNIO 2020')
junio2020.show()
junio2020.count()

print ('Número de viajes en 2019: ' + str(junio2019.count()))
print ('Número de viajes en 2020: ' + str(junio2020.count()))

print ('- JUNIO 2019:')
junio2019.groupBy('user_type').count().show()
print ('- JUNIO 2020:')
junio2020.groupBy('user_type').count().show()

print ('- JUNIO 2019:')
junio2019.groupBy('ageRange').count().show()
print ('- JUNIO 2020:')
junio2020.groupBy('ageRange').count().show()

idplug2019 = junio2019.groupBy('idplug_station').count()
idplug2020 = junio2020.groupBy('idplug_station').count()

print ('- JUNIO 2019:')
idplug2019.filter(idplug2019['count']>5000).show()
print ('- JUNIO 2021:')
idplug2020.filter(idplug2020['count']>2500).show()

idunplug2019 = junio2019.groupBy('idunplug_station').count()
idunplug2020 = junio2020.groupBy('idunplug_station').count()

print ('- JUNIO 2019:')
idunplug2019.filter(idunplug2019['count']>5500).show()
print ('- JUNIO 2021:')
idunplug2020.filter(idunplug2020['count']>2500).show()

print ('- JUNIO 2019:')
idplug2019.filter(idplug2019['count']<500).show()
print ('- JUNIO 2021:')
idplug2020.filter(idplug2020['count']<330).show()

print ('- JUNIO 2019:')
idunplug2019.filter(idunplug2019['count']<500).show()
print ('- JUNIO 2020:')
idunplug2020.filter(idunplug2020['count']<330).show()

print ('- JUNIO 2019:')
junio2019.groupBy('user_type').sum('travel_time').show()
print ('- JUNIO 2O20:')
junio2020.groupBy('user_type').sum('travel_time').show()

junio2021 = spark.read.json('202106.json')
print ('- JUNIO 2021')
junio2021.show()
junio2021.count()

print ('Número de viajes en 2021: ' + str(junio2021.count()))

print ('- JUNIO 2021:')
junio2021.groupBy('user_type').count().show()

print ('- JUNIO 2021:')
junio2021.groupBy('ageRange').count().show()

idplug2021 = junio2021.groupBy('idplug_station').count()

print ('- JUNIO 2021:')
idplug2021.filter(idplug2021['count']>3700).show()

idunplug2021 = junio2021.groupBy('idunplug_station').count()

print ('- JUNIO 2021:')
idunplug2021.filter(idunplug2021['count']>3700).show()
idunplug2021 = junio2021.groupBy('idunplug_station').count()

print ('- JUNIO 2021:')
idplug2021.filter(idplug2021['count']<400).show()

print ('- JUNIO 2021:')
junio2021.groupBy('user_type').sum('travel_time').show()

dic2018 = spark.read.json('201812.json')
print ('- DICIEMBRE 2018')
dic2018.show()
dic2018.count()

dic2020 = spark.read.json('202012.json')
print ('- DICIEMBRE 2020')
dic2020.show()
dic2020.count()

print ('Número de viajes en 2018: ' + str(dic2018.count()))
print ('Número de viajes en 2020: ' + str(dic2020.count()))

print ('- DICIEMBRE 2018:')
dic2018.groupBy('user_type').count().show()
print ('- DICIEMBRE 2020:')
dic2020.groupBy('user_type').count().show()

print ('- DICIEMBRE 2018:')
dic2018.groupBy('ageRange').count().show()
print ('- DICIEMBRE 2020:')
dic2020.groupBy('ageRange').count().show()

idplug2018 = dic2018.groupBy('idplug_station').count()
idplug_2020 = dic2020.groupBy('idplug_station').count()

print ('- DICIEMBRE 2018:')
idplug2018.filter(idplug2018['count']>880).show()
print ('- DICIEMBRE 2020:')
idplug_2020.filter(idplug_2020['count']>2800).show()

idunplug2018 = dic2018.groupBy('idplug_station').count()
idunplug_2020 = dic2020.groupBy('idplug_station').count()

print ('- DICIEMBRE 2018:')
idunplug2018.filter(idunplug2018['count']>3300).show()
print ('- DICIEMBRE 2020:')
idunplug_2020.filter(idunplug_2020['count']>2800).show()

print ('- DICIEMBRE 2018:')
idplug2018.filter(idplug2018['count']<100).show()
print ('- DICIEMBRE 2020:')
idplug_2020.filter(idplug_2020['count']<12).show()

print ('- DICIEMBRE 2018:')
idunplug2018.filter(idunplug2018['count']<110).show()
print ('- DICIEMBRE 2020:')
idunplug_2020.filter(idunplug_2020['count']<15).show()

print ('- DICIEMBRE 2018:')
dic2018.groupBy('user_type').sum('travel_time').show()
print ('- DICIEMBRE 2020:')
dic2020.groupBy('user_type').sum('travel_time').show()


