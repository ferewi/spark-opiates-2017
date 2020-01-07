from pyspark import SparkContext, SparkConf # evtl obsolete
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('opioids2017').getOrCreate()

# TODO: configure cluster if needed
#confCluster = SparkConf()
#confCluster.set("spark.executor.memory", "8g")
#confCluster.set("spark.executor.cores", "4")

# base directory on hdfs
basepath = "opioids2017/in/"

presc = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(basepath + "PartD_Prescriber_PUF_NPI_Drug_17.txt")
opioids = spark.read.format("csv").option("header", "true").load(basepath + "opioids2017.csv")
npidata = spark.read.format("csv").option("header", "true").load(basepath + "npidata_20050523-20171210.csv")
taxdata = spark.read.format("csv").option("header", "true").load(basepath + "17zpallagi.csv")
presc.registerTempTable("presc")
opioids.registerTempTable("opioids")
npidata.registerTempTable("npidata")
taxdata.registerTempTable("taxdata")

# Reduziere prescribtion data auf Zeilen mit Opiaten
genericNames = spark.sql("SELECT DISTINCT `Generic Name` FROM opioids")
genericNames.registerTempTable("genericNames")
presc = spark.sql("SELECT p.npi, p.nppes_provider_state as state, p.nppes_provider_city as city, p.drug_name, p.generic_name, p.total_claim_count FROM presc AS p INNER JOIN genericNames AS g ON p.generic_name = trim(g.`Generic Name`)")
presc.registerTempTable("presc")

# ergänze ZIP Codes aus npidata
presc = spark.sql("SELECT p.*, n.`Provider Business Mailing Address Postal Code` as mzip, n.`Provider Business Practice Location Address Postal Code` as pzip FROM presc as p INNER JOIN npidata as n ON p.npi = n.NPI")
presc.registerTempTable("presc")

# Beispielabfrage: Absolute Opiatverschreibungen nach Bundesstaat (inkl. export)
spark.sql("SELECT state, SUM(total_claim_count) as total_claims FROM presc GROUP BY state ORDER BY total_claims DESC").toPandas().to_csv("claims_by_state_abs.csv", encoding="UTF-8")
# Tax data mit einbeziehen
spark.sql("SELECT p.state, SUM(p.total_claim_count)/SUM(t.N2) as claims_relative FROM presc as p INNER JOIN taxdata as t ON p.state = t.STATE AND t.zipcode = '00000' GROUP BY p.state ORDER BY claims_relative DESC").toPandas().to_csv("claims_by_state_rel.csv", encoding="UTF-8")

# ----------------------------------
