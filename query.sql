SELECT
    DISTINCT process_owner, process_name, parent_process_name, computername, parent_executablepath, executable_path, hour,
        COUNT(*) as count, array_agg(elastic_id) as elastic_id
    from data_lake.upe_datalake
    where tenant_id = 'tenant_c'
    and CAST(CONCAT(year,'-',month,'-',day) as DATE) >= CAST('2021-10-14' as DATE)
    and CAST(CONCAT(year,'-',month,'-',day) as DATE) < CAST('2021-10-15' as DATE)
    GROUP BY process_owner, process_name, parent_process_name, computername, parent_executablepath, executable_path, hour
