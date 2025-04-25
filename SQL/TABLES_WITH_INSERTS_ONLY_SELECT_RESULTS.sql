WITH dml_stats 
AS ( 
    SELECT dbo.OWNER AS table_owner 
        ,dbo.object_name AS table_name 
        ,SUM(stat.inserts) Total_Inserts 
        ,SUM(stat.updates) Total_Updates 
        ,SUM(stat.deletes) Total_Deletes 
        ,MIN(stat.TIMESTAMP) min_timestamp 
        ,MAX(stat.TIMESTAMP) max_timestamp 
    FROM sys.optstat_snapshot$ stat 
        ,dba_objects dbo 
    WHERE dbo.object_id = stat.obj# 
        AND dbo.OWNER <> 'SYS' 
        AND stat.updates = 0 
        AND stat.deletes = 0 
        AND stat.inserts > 1000 
    GROUP BY dbo.OWNER 
        ,dbo.object_name 
    ) 
    ,aggr_data 
AS ( 
    SELECT dml.* 
        ,dbt.partitioned table_partitioned 
        ,dbt.compression table_compress 
        ,(SELECT SUM(dtm.updates) + SUM(dtm.deletes) 
            FROM dba_tab_modifications dtm 
            WHERE dbt.OWNER = dtm.TABLE_OWNER 
                AND dbt.table_NAME = dtm.table_name 
            ) dbtm 
        ,( 
            SELECT LISTAGG(DISTINCT compression, ', ' ON OVERFLOW TRUNCATE WITH COUNT) 
            FROM dba_tab_partitions dbp 
            WHERE dbp.table_owner = dbt.OWNER 
                AND dbp.table_name = dbt.table_name 
            ) part_compress 
        ,( 
            SELECT ROUND(SUM(bytes / 1024 / 1024)) 
            FROM dba_segments dbs 
            WHERE dbs.OWNER = dbt.OWNER 
                AND dbs.segment_name = dbt.table_name 
            ) size_Mb 
    FROM dml_stats dml 
        ,dba_tables dbt 
    WHERE dml.table_owner = dbt.OWNER 
        AND dml.table_name = dbt.table_name 
    ) 
SELECT table_owner 
    ,table_name 
    ,total_inserts 
    ,total_updates 
    ,total_deletes 
    ,size_Mb 
    ,table_partitioned 
    ,table_compress 
    ,part_compress 
FROM aggr_data 
WHERE 1 = 1 
    AND size_Mb > 1000 --check objects with size>1Gb 
    AND NVL(part_compress, 'DISABLED') <> 'ENABLED' 
    AND NVL(table_compress, 'DISABLED') <> 'ENABLED' 
    AND NVL(dbtm, 0) = 0 -- nothing in dba_tab_modifications 
ORDER BY size_Mb DESC 