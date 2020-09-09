#Importing reuired libraries
import datetime as dt
import json
import pyspark
from pyspark.sql import functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import HiveContext,SparkSession

#### Function to get max_createdmsec from Audit table ######


def get_last_loadValue(audit_value, audit_schema, source_table, target_table):
    last_load_value = spark.sql("select coalesce(max({0}),'662691661000') from {1} \
        where status= 'success' AND source_table ='{2}' AND target_table='{3}'" \
                                .format(audit_value, audit_schema, source_table, target_table)).collect()[0][0]
    return last_load_value


#### Function to fetch data from source table with last_loaded_date > max_createdmsec  ######
def get_router_data(source_schema, last_loaded_date, max_createdmsec):
    df = spark.sql("SELECT distinct eventid, datum, createdmsec, crm.assetid as assetid, explode(hosts) as host FROM {0} WHERE datum >= '{1}' and createdmsec > {2} and eventid is not null and crm.assetid is not null"\
                   .format(source_schema, last_loaded_date, max_createdmsec))
    return df

def get_wifi_data(WifiParams_source_schema, Wifi_params_last_loaded_date, Wifi_params_max_createdmsec):
    df = spark.sql("select distinct eventid,datum,wifiparams24.wifiBssid as wifiBssid24 ,wifiparams5.wifiBssid as wifiBssid5 FROM {0} WHERE datum >= '{1}' and createdmsec > {2} and eventid is not null and crm.assetid is not null"\
                   .format(WifiParams_source_schema, Wifi_params_last_loaded_date, Wifi_params_max_createdmsec))
    return df

if __name__ == "__main__":
### Setting up environment and variable parameters in JSON file #####
    env = 'dev'

    tdm_table = 'wificlientrole'


    parameterFile = 'variable.json'
    with open(parameterFile) as jsonFile:
        jobProperties = json.load(jsonFile)
    jobParameters = jobProperties.get(env)
    
    MetastoreUri = jobParameters.get("MetastoreURI")
    WareheHouseDir = jobParameters.get("sparkWarehouseDir")
    db_tdm = jobParameters.get("DB_TDM")
    db_stage = jobParameters.get("DB_CDL_STAGE")
    source_table = jobParameters.get("STG_HOSTS_TABLE")
    WifiParams_source_table = jobParameters.get("STG_ROUTER_TABLE")
    audit_column = jobParameters.get("AUDIT_COLUMN")
    db_audit = jobParameters.get("DB_AUDIT")
    audit_table = jobParameters.get("ROUTER_AUDIT_TABLE")

# Defining source,audit and tdm schema format
    source_schema = db_stage + "." + source_table
    WifiParams_source_schema = db_stage + "." + WifiParams_source_table
    audit_schema = db_audit + "." + audit_table
    target_schema = db_tdm + "." + tdm_table

#Creating Spark Session 
    spark = SparkSession.builder\
        .master("yarn")\
        .enableHiveSupport()\
        .appName("CustomerFacingService_RouterSourceData")\
        .config("hive.exec.dynamic.partition", "true")\
        .config("hive.exec.dynamic.partition.mode", "nonstrict")\
        .config("spark.sql.warehouse.dir", "warehehousedir")\
        .getOrCreate()

    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")

    exec_start_time = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%m:%S")

    print("ETL Job execution started at " + str(exec_start_time))

    max_createdmsec = get_last_loadValue("audit_value", audit_schema, source_table, tdm_table)

    print("Last loaded data has max_createdmsec = " + str(max_createdmsec))

    last_loaded_date = dt.datetime.utcfromtimestamp(int(max_createdmsec) / 1000).strftime("%Y-%m-%d")

    print("Last loaded data Date = " + str(last_loaded_date))

    df = get_router_data(source_schema,last_loaded_date,max_createdmsec)
    
    cdlhosts_data_cnt = df.count()

    print("Incremental Data to be loaded to TDM Table =" + str(cdlhosts_data_cnt) + " records")
if (cdlhosts_data_cnt > 0):
    df1 = df.withColumn('eventdate',regexp_replace(col("datum"), "-", ""))
    hst = df1.filter("host.interfacetype = 'WIFI'")\
                                    .select(col("eventid").alias("id"),\
                                    col("assetid").alias("networkdeviceid"),\
                                    col("host.hostmac").alias("MacAddress"),\
                                    col("host.hostname").alias("HostName"),\
                                    "eventtime",\
                                    "eventdate",\
                                    f.get_json_object("host.params", "$.opaquedata.band").alias("band")\
                                    )
    dup = hst.dropDuplicates()
    hst_cnt = dup.count()
    print("Total source count is : " + str(hst_cnt))

#######cdl_acscoll_wifiparam_prq table data ########

    Wifi_params_max_createdmsec = get_last_loadValue("audit_value", audit_schema, WifiParams_source_table, tdm_table)

    print("Last loaded data has Wifi_params_max_createdmsec = " + str(Wifi_params_max_createdmsec))

    Wifi_params_last_loaded_date = dt.datetime.utcfromtimestamp(int(Wifi_params_max_createdmsec) / 1000).strftime("%Y-%m-%d")

    print("Wifi_params Last loaded data Date = " + str(Wifi_params_last_loaded_date))

#Fetching data from source using function created above
    Wifi_params_data = get_wifi_data(WifiParams_source_schema,Wifi_params_last_loaded_date,Wifi_params_max_createdmsec)

    Wifi_params_data_df = Wifi_params_data.withColumn("eventdate",regexp_replace(col("datum"), "-", ""))

    wfp5 = Wifi_params_data_df.filter("eventid is not null").select(col("eventid").alias("id"),\
                                                                           col("wifiBssid5").alias("bssid"),\
                                                                           "eventdate")
    wfp_24 = Wifi_params_data_df.filter("eventid is not null").select(col("eventid").alias("id"),\
                                                                            col("wifiBssid24").alias("bssid"),\
                                                                            "eventdate")



    src_rec_2_4 = hst.filter("band = '2.4GHz'")\
                    .join(wfp_24, on=['id', 'eventdate'], how='left')

    src_rec_5 = hst.filter("band = '5GHz'")\
                    .join(wfp5, on=['id', 'eventdate'], how='left')

    src_rec_null = hst.filter("band is null").withColumn("Bssid", f.lit(None))

    rec_2_4 = src_rec_2_4.select("id","networkdeviceid","Bssid","MacAddress","HostName","eventtime","band","eventdate")
    rec_5 = src_rec_5.select("id","networkdeviceid","Bssid","MacAddress","HostName","eventtime","band","eventdate")
 
    src_rec = src_rec_null.select("id","networkdeviceid","Bssid","MacAddress","HostName","eventtime","band","eventdate")
    source_record = rec_2_4.union(rec_5).union(src_rec)

    source_record.createOrReplaceGlobalTempView("src_records")

    wcr = spark.sql("""SELECT distinct
                         ID,
                         NetworkDeviceID,
                         Bssid,
                         MacAddress,
                         HostName,
                         EventTime,
                         EventDate
                         FROM global_temp.src_records""")

    wcr.createOrReplaceGlobalTempView("wcr")

    src_rec_count_df = spark.sql("select distinct id from global_temp.src_records")
    src_rec_count = src_rec_count_df.count()
        
    print("Total source count is : " + str(src_rec_count))

    tgt_rec_count = wcr.select('ID').distinct().count()

    print("Total target count will be : " + str(tgt_rec_count))

    insertdf = spark.sql("""Insert into cdl_tdata_model.wificlientrole\
                     PARTITION (eventdate) \
                     select ID,\
                            NetworkDeviceID,\
                            Bssid,\
                            MacAddress,\
                            HostName,\
                            EventTime,\
                            eventdate \
                            from global_temp.wcr\
                               """)

    load_date = last_loaded_date.replace('-', '')


    tdm_backup = spark.sql("SELECT * FROM {0} WHERE {1} >= '{2}'".format(target_schema, "eventdate", last_loaded_date))

    exec_end_time = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%m:%S")

    print("ETL Job execution completed successfully at " + str(exec_end_time))

    if (src_rec_count == tgt_rec_count):
            source_stats = "Count of distinct event id =" + str(src_rec_count)
            target_stats = "Count of distinct event id =" + str(tgt_rec_count)
            audit_column = 'createdmsec'
            audit_value = df.select([max("createdmsec")]).collect()[0][0]
            audit_rec = (exec_start_time, exec_end_time, source_table, tdm_table, source_stats, target_stats, audit_column,           audit_value, 'success')
            audit_rec_df = spark.createDataFrame([audit_rec],
                                                 ["execution_start", "execution_end", "source_table", "target_table",         "source_stats", "target_stats", "audit_column", "audit_value", "status"])
            audit_rec_df.write.insertInto(audit_schema, overwrite=False)
            print("ETL Job completed, Audit table updated successfully at " + str(exec_end_time))
            spark.stop()

    else:
            source_stats = "Count of distinct event id =" + str(src_cnt)
            target_stats = "Count of distinct event id =" + str(target_cnt)
            audit_column = 'createdmsec'
            audit_value = host_data.select([max("createdmsec")]).collect()[0][0]
            print("Record Count mistmatch between Source table and Target Table")
            print("Running Clean Up to clear partial load if any")
            spark.sql("ALTER TABLE {0} DROP PARTITION {1} > '{2}'".format(target_schema, 'eventdate', load_date))
            tdm_backup.write.insertInto(target_schema, overwrite=False)
            print("Clean Up Process Complete")
            audit_rec = (exec_start_time, exec_end_time, source_table, tdm_table, source_stats, target_stats, audit_column, audit_value, 'fail')
            audit_rec_df = spark.createDataFrame([audit_rec], ["execution_start", "execution_end", "source_table", "target_table", "source_stats"])
            audit_rec_df.write.insertInto(audit_schema, overwrite=False)
            spark.stop()
            
else:
        exec_end_time = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%m:%S")
        src_cnt = 0
        target_cnt = 0
        source_stats = "Count of distinct event id =" + str(src_cnt)
        target_stats = "Count of distinct event id =" + str(target_cnt)
        audit_column = 'createdmsec'
        audit_value = max_createdmsec
        audit_rec = (exec_start_time, exec_end_time, source_table, tdm_table, source_stats, target_stats, audit_column, audit_value, 'success')
        audit_rec_df = spark.createDataFrame([audit_rec], ["execution_start", "execution_end", "source_table", "target_table", "source_stats"])
        audit_rec_df.write.insertInto(audit_schema, overwrite=False)
        print("No Data Avaialable Hence ETL Completed with 0 Records loaded to Target Table")
        spark.stop()

