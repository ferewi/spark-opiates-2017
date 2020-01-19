from pyspark import SparkContext, SparkConf # evtl obsolete
from pyspark.sql import SQLContext,SparkSession
spark = SparkSession.builder.appName('opioids2017').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(spark.sparkContext)

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
popdata = spark.read.format("csv").option("header", "true").load(basepath + "population_by_zip_2010.csv")
deaths = spark.read.format("csv").option("header", "true").load(basepath + "deaths_by_county_2018.csv")
statefips = spark.read.format("csv").option("header", "true").load(basepath + "state_fips.csv")
stateLongLat = spark.read.format("csv").option("header", "true").load(basepath + "states_long_lat.csv")
fipsLongLat = spark.read.format("csv").option("header", "true").load(basepath + "fips_long_lat.csv")
fipsdataRDD = sc.textFile(basepath + "zipfips/*")

presc.registerTempTable("presc")
opioids.registerTempTable("opioids")
npidata.registerTempTable("npidata")
taxdata.registerTempTable("taxdata")
popdata.registerTempTable("popdata")
statefips.registerTempTable("statefips")
deaths.registerTempTable("deaths")
stateLongLat.registerTempTable("stateLongLat")
fipsLongLat.registerTempTable("fipsLongLat")

# Vorverarbeitung fuer Zip to FIPS mapping
fipsdataRDD = fipsdataRDD.map(lambda str: [str[:5], str[23:25], str[25:28], str[28:]])
fipsdata = sqlContext.createDataFrame(fipsdataRDD, ["zipcode", "state_abbr", "county_fips", "county_name"])
fipsdata.registerTempTable("fipsdata")
fipsdata = spark.sql("""
    SELECT
        f.zipcode AS zipcode,
        FIRST(f.state_abbr) AS state,
        FIRST(sf.state_name) AS state_name,
        FIRST(sf.fips) AS state_fips,
        FIRST(f.county_fips) AS county_fips,
        FIRST(CONCAT(sf.fips, f.county_fips)) as fips,
        FIRST(f.county_name) AS county
    FROM fipsdata AS f
    INNER JOIN statefips AS sf ON f.state_abbr = sf.state_abbr
    GROUP BY f.zipcode
    ORDER BY f.zipcode
""")
fipsdata.registerTempTable("fipsdata")

# Reduziere prescribtion data auf Zeilen mit Opiaten
genericNames = spark.sql("SELECT DISTINCT `Generic Name` FROM opioids")
genericNames.registerTempTable("genericNames")
presc = spark.sql("""
    SELECT
        p.npi,
        p.nppes_provider_state as state,
        p.nppes_provider_city as city,
        p.drug_name,
        p.generic_name,
        p.total_claim_count
    FROM presc AS p
    INNER JOIN genericNames AS g ON p.generic_name = trim(g.`Generic Name`)
""")
presc.registerTempTable("presc")

# ergaenze ZIP Codes aus npidata
presc = spark.sql("""
    SELECT
        p.*,
        n.`Provider Business Practice Location Address Postal Code` as zip,
        f.fips
    FROM presc as p
    INNER JOIN npidata as n ON p.npi = n.NPI
    INNER JOIN fipsdata as f ON n.`Provider Business Practice Location Address Postal Code` = f.zipcode
""")
presc.registerTempTable("presc")

popdata = spark.sql("""
    SELECT
        f.fips AS fips,
        FIRST(f.state) AS state,
        FIRST(f.state_name) AS state_name,
        FIRST(f.county) AS county,
        SUM(p.population) as population
    FROM popdata AS p
    INNER JOIN fipsdata f ON p.zipcode = f.zipcode
    GROUP BY fips
""")
popdata.registerTempTable("popdata")

# aggrgate Prescriptions by fipscode
presc = spark.sql("""
    SELECT
        f.fips AS fips,
        SUM(p.total_claim_count) as claim_count
    FROM presc AS p
    INNER JOIN fipsdata f ON SUBSTR(p.zip, 0, 5) = f.zipcode
    GROUP BY f.fips
""")
presc.registerTempTable("presc")

taxdata = spark.sql("""
    SELECT
        f.fips AS fips,
        t.agi_stub AS agi_stub,
        SUM(t.N1) as N1,
        SUM(t.A00100) as A00100
    FROM taxdata AS t
    INNER JOIN fipsdata f ON t.zipcode = f.zipcode
    GROUP BY f.fips, t.agi_stub
""")
taxdata.registerTempTable("taxdata")

# assemble dataset
byFips = spark.sql("""
    SELECT
        pp.fips,
        FIRST(pp.state) as state,
        FIRST(pp.state_name) as state_name,
        TRIM(LOWER(FIRST(pp.county))) AS county,
        FIRST(fll.latitude) as latitude,
        FIRST(fll.longitude) as longitude,
        SUM(pp.population)/IF(COUNT(t.agi_stub) > 0, COUNT(t.agi_stub), 1) as population,
        SUM(t.N1) as num_tax_returns,
        SUM(t.A00100) as total_agi,
        SUM(t.A00100)/SUM(t.N1) as mean_agi,
        CASE
            WHEN SUM(t.A00100)/SUM(t.N1) < 25 THEN 1
            WHEN SUM(t.A00100)/SUM(t.N1) < 50 THEN 2
            WHEN SUM(t.A00100)/SUM(t.N1) < 75 THEN 3
            WHEN SUM(t.A00100)/SUM(t.N1) < 100 THEN 4
            WHEN SUM(t.A00100)/SUM(t.N1) < 200 THEN 5
            WHEN SUM(t.A00100)/SUM(t.N1) >= 200 THEN 6
        ELSE 0
        END as agi_group,
        COALESCE(SUM(p.claim_count)/IF(COUNT(t.agi_stub) > 0, COUNT(t.agi_stub), 1), 0) as claim_count,
        COALESCE(SUM(p.claim_count)/SUM(pp.population), 0) as claim_rate,
        COALESCE(SUM(IF(d.deaths_by_residence = '^', 0, d.deaths_by_residence))/IF(COUNT(t.agi_stub) > 0, COUNT(t.agi_stub), 1), 0) as deaths_by_residence,
        COALESCE(SUM(IF(d.deaths_by_residence = '^', 0, d.deaths_by_residence))/SUM(pp.population), 0) as death_rate
    FROM popdata as pp
    LEFT JOIN deaths as d ON TRIM(LOWER(pp.county)) = TRIM(LOWER(d.county)) AND TRIM(LOWER(pp.state_name)) = TRIM(LOWER(d.state))
    LEFT JOIN taxdata as t ON pp.fips = t.fips
    LEFT JOIN presc as p ON pp.fips = p.fips
    LEFT JOIN fipsLongLat fll ON pp.fips = fll.fips
    GROUP BY pp.fips
    ORDER BY deaths_by_residence DESC
""")
byFips.registerTempTable("byFips")
byFips.toPandas().to_csv("results_by_fips.csv", encoding="UTF-8")

byState = spark.sql("""
    SELECT
        bf.state as state,
        FIRST(bf.state_name) as state_name,
        FIRST(sll.latitude) as latitude,
        FIRST(sll.longitude) as longitude,
        SUM(bf.population) as population,
        SUM(bf.num_tax_returns) as num_tax_returns,
        SUM(bf.total_agi) as total_agi,
        SUM(bf.total_agi)/SUM(bf.num_tax_returns) as mean_agi,
        CASE
            WHEN SUM(bf.total_agi)/SUM(bf.num_tax_returns) < 25 THEN 1
            WHEN SUM(bf.total_agi)/SUM(bf.num_tax_returns) < 50 THEN 2
            WHEN SUM(bf.total_agi)/SUM(bf.num_tax_returns) < 75 THEN 3
            WHEN SUM(bf.total_agi)/SUM(bf.num_tax_returns) < 100 THEN 4
            WHEN SUM(bf.total_agi)/SUM(bf.num_tax_returns) < 200 THEN 5
            WHEN SUM(bf.total_agi)/SUM(bf.num_tax_returns) >= 200 THEN 6
        ELSE 0
        END as agi_group,
        SUM(bf.claim_count) as claim_count,
        SUM(bf.claim_count)/SUM(bf.population) as claim_rate,
        SUM(bf.deaths_by_residence) as deaths_by_residence,
        SUM(bf.deaths_by_residence)/SUM(bf.population) as death_rate
    FROM byFips AS bf
    LEFT JOIN stateLongLat AS sll ON bf.state = sll.state
    GROUP BY bf.state
    ORDER BY deaths_by_residence DESC
""")
byState.toPandas().to_csv("results_by_state.csv", encoding="UTF-8")
# zip level
#spark.sql("SELECT STATE as state, SUBSTR(zipcode, 0, 3) as zip, SUM(N1) as tax_returns FROM taxdata GROUP BY state, SUBSTR(zipcode, 0, 3) ")

# Dataframe fuer Regressionsmodell
#spark.sql("SELECT state, SUBSTR(pzip, 0, 3) as zip FROM presc INNER JOIN")
# ----------------------------------


# Beispielabfrage: Absolute Opiatverschreibungen nach Bundesstaat (inkl. export)
#spark.sql("SELECT state, SUM(total_claim_count) as total_claims FROM presc GROUP BY state ORDER BY total_claims DESC").toPandas().to_csv("claims_by_state_abs.csv", encoding="UTF-8")

# Tax data mit einbeziehen
#spark.sql("SELECT p.state, SUM(p.total_claim_count)/SUM(t.N2) as claims_relative FROM presc as p INNER JOIN taxdata as t ON p.state = t.STATE AND t.zipcode = '00000' GROUP BY p.state ORDER BY claims_relative DESC").toPandas().to_csv("claims_by_state_rel.csv", encoding="UTF-8")
