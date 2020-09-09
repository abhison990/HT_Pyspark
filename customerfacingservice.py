#Importing reuired libraries
import datetime as dt
import json
import pyspark
import sys
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
    df = spark.sql("SELECT * FROM {0} WHERE datum >= '{1}' and createdmsec > {2}" \
                   .format(source_schema, last_loaded_date, max_createdmsec))
    return df

if __name__ == "__main__":
### Setting up environment and variable parameters in JSON file #####
    env = 'dev'

    tdm_table = 'customerfacingservice'


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

#Using function created above for calculating max date in host source table
    max_createdmsec = get_last_loadValue("audit_value", audit_schema, source_table, tdm_table)


    print("Last loaded data has max_createdmsec = " + str(max_createdmsec))

    last_loaded_date = dt.datetime.utcfromtimestamp(int(max_createdmsec) / 1000).strftime("%Y-%m-%d")

    print("Last loaded data Date = " + str(last_loaded_date))

    load_date = last_loaded_date.replace('-', '')

#Fetching data from source using function created above
    host_data = get_router_data(source_schema, last_loaded_date, max_createdmsec)

    host_data_cnt = host_data.count()

    print("Incremental Data to be loaded to TDM Table =" + str(host_data_cnt) + " records")
#print(host_data)
if (host_data_cnt > 0):
        df1 = host_data.withColumn("ValidForBegin",from_unixtime((host_data.createdmsec.cast('bigint')/1000)).cast('timestamp'))\
        .withColumn("type",f.lit("Router"))\
        .withColumn("validforend",f.lit("N/A"))\
        .withColumn('eventdate',regexp_replace(col("datum"), "-", ""))

        hosts_null = df1.filter("hosts is NULL")

        df_hosts_null = hosts_null.select(col("eventid").alias("id"),\
                                  col("crm.assetid").alias("networkdeviceid"),\
                                  "ValidForBegin",\
                                  "validforend",\
                                  col("crm.servicename").alias("servicename"),\
                                  col("crm.assetid").alias("customerproductid"),\
                                 lit(None).alias("interfacetype"),
                                  "eventdate",\
                                  "type")
                          
        hosts_not_null = df1.filter("hosts is NOT NULL")

        df_host_empty = hosts_not_null.filter("size(hosts) <= 0").select(col("eventid").alias("id"),\
                                                        col("crm.assetid").alias("networkdeviceid"),\
                                                        "ValidForBegin",\
                                                        "validforend",\
                                                        col("crm.servicename").alias("servicename"),\
                                                        col("crm.assetid").alias("customerproductid"),\
                                                        lit(None).alias("interfacetype"),\
                                                        "eventdate",\
                                                        "type")
        df_host_not_empty = hosts_not_null.filter("size(hosts) > 0").withColumn("host", explode("hosts")).\
                                    select(col("eventid").alias("id"),\
                                                        col("crm.assetid").alias("networkdeviceid"),\
                                                        "ValidForBegin",\
                                                        "validforend",\
                                                        col("crm.servicename").alias("servicename"),\
                                                        col("crm.assetid").alias("customerproductid"),\
                                                        lit(None).alias("interfacetype"),\
                                                        "eventdate",\
                                                        "type")\
                                                    .drop("hosts").drop("host")

        hostdata = df_host_not_empty.unionAll(df_host_empty).unionAll(df_hosts_null)

        hostdata_cnt = hostdata.count()

        print("Host data after explode : ") + str(hostdata_cnt)

#######cdl_acscoll_wifiparam_prq table data ########

#Using function created above for calculating max date in Wifi_params source table
        Wifi_params_max_createdmsec = get_last_loadValue("audit_value", audit_schema, WifiParams_source_table, tdm_table)

        print("Last loaded data has Wifi_params_max_createdmsec = " + str(Wifi_params_max_createdmsec))

        Wifi_params_last_loaded_date = dt.datetime.utcfromtimestamp(int(Wifi_params_max_createdmsec) / 1000).strftime("%Y-%m-%d")

        print("Wifi_params Last loaded data Date = " + str(Wifi_params_last_loaded_date))

#Fetching data from source using function created above
        Wifi_params_data = get_router_data(WifiParams_source_schema, Wifi_params_last_loaded_date, Wifi_params_max_createdmsec)

#print(Wifi_params_data)

#Wifi_params_cnt = Wifi_params_data.count()

#print("Incremental Data to be loaded to TDM Table =" + str(Wifi_params_cnt) + " records")

#if (Wifi_params_cnt > 0):
        Wifi_params_data_df = Wifi_params_data.withColumn("eventdate",regexp_replace(col("datum"), "-", ""))
                  
        cdl_acscoll_wifiparams_prq = Wifi_params_data_df.select("eventid",\
                                         col("crm.assetid").alias("networkdeviceid"),\
                                         "eventdate")                                                      
                                                                 
#joining host and wifi_params table on eventid to get final data to load in tdm


        cond = [hostdata.id == cdl_acscoll_wifiparams_prq.eventid, hostdata.eventdate == cdl_acscoll_wifiparams_prq.eventdate]
        cfs = hostdata.alias("h").join(cdl_acscoll_wifiparams_prq.alias("p"),cond ,"left")
                            
#WifiClientRole.createOrReplaceGlobalTempView("WifiClientRoleView")
        customerFacingservice = cfs.select(col("h.id").alias("id"),\
                                    "h.NetworkDeviceID",\
                                   "h.ValidForBegin",\
                                   "h.ValidForEnd",\
                                   "h.servicename",\
                                  "h.customerproductid",\
                                  "h.eventdate",\
                                   "h.type")

        drop = customerFacingservice.distinct()

        customerFacingservice_cnt = drop.count()

        print("Records are : ") + str(customerFacingservice_cnt)

        drop.createOrReplaceGlobalTempView("customerFacingserviceView")

        insertdf = spark.sql("""Insert into cdl_tdata_model.customerfacingservice \
                        PARTITION (eventdate,Type) \
                        select id,\
                               networkdeviceid,\
                               ValidForBegin,\
                               ValidForEnd,\
                               servicename,\
                               customerproductid,\
						       eventdate,\
                               type \
						       from global_temp.customerFacingserviceView \
                     """)
        load_date = last_loaded_date.replace('-', '')

        tdm_backup = spark.sql("SELECT * FROM {0} WHERE {1} >= '{2}'".format(target_schema, "eventdate", last_loaded_date))


#tdm_backup.show()

#customerFacingservice.write.insertInto(target_schema, overwrite=True).partitionBy("eventdate","Type")

        exec_end_time = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%m:%S")

        print("ETL Job execution completed successfully at " + str(exec_end_time))

####### Now we need to insert audit table with above fetched data from source #######

#Fetching count of source table

        src_cnt = spark.sql("SELECT COUNT(DISTINCT eventid) FROM {0} WHERE {1} >= '{2}'".format(source_schema, 'datum', last_loaded_date)).collect()[0][0]

#Fetching count of target/tdm table
        target_cnt = spark.sql("SELECT COUNT(DISTINCT id) FROM {0} WHERE {1} >= '{2}'".format(target_schema, 'eventdate',load_date)).collect()[0][0]  
    
        print("source count = " + str(src_cnt))
        print("target count = " + str(target_cnt))

        if (src_cnt == target_cnt):
            source_stats = "Count of distinct event id =" + str(src_cnt)
            target_stats = "Count of distinct event id =" + str(target_cnt)
            audit_column = 'createdmsec'
            audit_value = host_data.select([max("createdmsec")]).collect()[0][0]
            audit_rec = (exec_start_time,exec_end_time,source_table,tdm_table,source_stats,target_stats,audit_column,audit_value, 'success')
            audit_rec_df = spark.createDataFrame([audit_rec],["execution_start", "execution_end", "source_table", "target_table","source_stats", "target_stats", "audit_column", "audit_value", "status"])
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
