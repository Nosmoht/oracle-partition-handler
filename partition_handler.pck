CREATE OR REPLACE PACKAGE partition_handler AUTHID DEFINER IS
  /*****************************************************************************
  * Partition Handler
  *
  * Requires:  log_admin >= 1.2.3
  *            module_admin >= 0.7.1
  *            partition_types >= 0.9.25
  *            partition_tools >= 0.9.9
  *
  *****************************************************************************/
  gc_module_version CONSTANT module_admin.t_module_version := '0.9.45';
  gc_module_label   CONSTANT module_admin.t_module_label := $$PLSQL_UNIT || ' v' || gc_module_version;

  -- Process actions
  c_pa_not_found CONSTANT PLS_INTEGER := 0; -- Partition was not found
  c_pa_found     CONSTANT PLS_INTEGER := 1; -- Partition was found
  --  c_pa_must_rename CONSTANT PLS_INTEGER := 2; -- Partition must be renamed
  c_pa_renamed    CONSTANT PLS_INTEGER := 3; -- Partition was renamed
  c_pa_must_add   CONSTANT PLS_INTEGER := 4; -- Partition must be added
  c_pa_added      CONSTANT PLS_INTEGER := 5; -- Partition was added
  c_pa_must_split CONSTANT PLS_INTEGER := 6; -- Partition must be splitted
  c_pa_splitted   CONSTANT PLS_INTEGER := 7; -- Partition was splitted
  c_pa_must_merge CONSTANT PLS_INTEGER := 8; -- Partition must be merged
  c_pa_merged     CONSTANT PLS_INTEGER := 9; -- Partition was merged
  --  c_pa_must_move   CONSTANT PLS_INTEGER := 10; -- Partition must be moved
  c_pa_moved     CONSTANT PLS_INTEGER := 11; -- Partition was moved
  c_pa_must_drop CONSTANT PLS_INTEGER := 12; -- Partition must be dropped
  c_pa_dropped   CONSTANT PLS_INTEGER := 13; -- Partition was dropped

  /******************************************************************************
  * Cursor
  ******************************************************************************/
  -- Select object definitions: Tables before indexes, then order by order# to handle objects in the right order
  CURSOR cur_obj_defs IS
    SELECT o.object_id,
           o.enabled,
           o.order#,
           o.object_type,
           o.object_owner,
           o.object_name,
           o.label_name,
           o.partition_key,
           o.partition_key_data_type,
           o.partition_technique,
           o.subpartition_key,
           o.subpartition_key_data_type,
           o.subpartition_technique
      FROM partition_object o
     WHERE o.enabled = partition_constants.c_enabled
     ORDER BY o.object_type DESC, o.order# ASC NULLS LAST;

  -- Cursor to select partition definitions
  CURSOR cur_part_defs(p_obj_id_in   IN partition_types.t_object_id,
                       p_ptype_id_in IN partition_types.t_partition_type_id,
                       partition#_in IN partition_types.t_partition_number DEFAULT NULL) IS
    SELECT k.p_ptype_id,
           k.p_tech_id,
           (SELECT t.name FROM p_tech t WHERE t.id = k.p_tech_id) AS partition_technique,
           d.p_obj_id,
           d.partition#,
           d.subpartition#,
           decode(o.p_otype_id, partition_constants.c_obj_type_table_id, dtc.data_type, partition_constants.c_obj_type_index_id, dicc.data_type) AS data_type,
           --
           t.name AS partition_type,
           -- Partition name
           CAST(NULL AS VARCHAR2(30)) AS parent_partition_name,
           nvl2(d.p_naming_function, NULL, d.p_name) AS partition_name, -- Set name to blank if naming function is set
           nvl2(d.p_naming_function, d.p_name, NULL) AS partition_name_format, -- Set partition name format if naming funcztion is set
           d.p_name_prefix AS partition_name_prefix,
           d.p_name_suffix AS partition_name_suffix,
           d.p_naming_function AS partition_naming_function,
           -- Tablespace name
           nvl2(d.tbs_naming_function, NULL, d.tbs_name) AS tablespace_name, -- Set name to blank if naming function is set
           nvl2(d.tbs_naming_function, d.tbs_name, NULL) AS tablespace_name_format, -- Set tablespace name format if naming function is set
           d.tbs_name_prefix AS tablespace_name_prefix,
           d.tbs_name_suffix AS tablespace_name_suffix,
           d.tbs_naming_function AS tablespace_naming_function,
           -- Replace all occurencies of ' in the high value to avoid errors
           decode(REPLACE(d.high_value, partition_constants.c_apostroph),
                  -- If the high_value is not DEFAULT or MAXVALUE and the data type of the partition key column is VARCHAR2 or DATE/TIMESTAMP
                  -- the high value will be enclosed by '' to make the handling easier
                  partition_constants.c_maxvalue,
                  partition_constants.c_maxvalue,
                  partition_constants.c_default,
                  partition_constants.c_default,
                  decode(dtc.data_type,
                         partition_constants.c_dt_varchar,
                         partition_constants.c_apostroph || d.high_value || partition_constants.c_apostroph,
                         partition_constants.c_dt_date,
                         partition_constants.c_apostroph || d.high_value || partition_constants.c_apostroph,
                         partition_constants.c_dt_timestamp_0,
                         partition_constants.c_apostroph || d.high_value || partition_constants.c_apostroph,
                         partition_constants.c_dt_timestamp_0_wltz,
                         partition_constants.c_apostroph || d.high_value || partition_constants.c_apostroph,
                         partition_constants.c_dt_timestamp_0_wtz,
                         partition_constants.c_apostroph || d.high_value || partition_constants.c_apostroph,
                         partition_constants.c_dt_timestamp_6,
                         partition_constants.c_apostroph || d.high_value || partition_constants.c_apostroph,
                         partition_constants.c_dt_timestamp_6_wltz,
                         partition_constants.c_apostroph || d.high_value || partition_constants.c_apostroph,
                         partition_constants.c_dt_timestamp_6_wtz,
                         partition_constants.c_apostroph || d.high_value || partition_constants.c_apostroph,
                         partition_constants.c_dt_timestamp_9,
                         partition_constants.c_apostroph || d.high_value || partition_constants.c_apostroph,
                         partition_constants.c_dt_timestamp_9_wltz,
                         partition_constants.c_apostroph || d.high_value || partition_constants.c_apostroph,
                         partition_constants.c_dt_timestamp_9_wtz,
                         partition_constants.c_apostroph || d.high_value || partition_constants.c_apostroph,
                         d.high_value)) AS high_value,
           -- Set the convert function related to the data type. DATE will be converted with TO_DATE, TIMESTAMP with TO_TIMESTAMP
           -- and TIMESTAMP WITH TIME ZONE with TO_TIMESTAMP_TZ
           decode(dtc.data_type,
                  partition_constants.c_dt_date,
                  partition_constants.c_to_date,
                  partition_constants.c_dt_timestamp_0,
                  partition_constants.c_to_timestamp,
                  partition_constants.c_dt_timestamp_0_wltz,
                  partition_constants.c_to_timestamp_tz,
                  partition_constants.c_dt_timestamp_0_wtz,
                  partition_constants.c_to_timestamp_tz,
                  partition_constants.c_dt_timestamp_6,
                  partition_constants.c_to_timestamp,
                  partition_constants.c_dt_timestamp_6_wltz,
                  partition_constants.c_to_timestamp_tz,
                  partition_constants.c_dt_timestamp_6_wtz,
                  partition_constants.c_to_timestamp_tz,
                  partition_constants.c_dt_timestamp_9,
                  partition_constants.c_to_timestamp,
                  partition_constants.c_dt_timestamp_9_wltz,
                  partition_constants.c_to_timestamp_tz,
                  partition_constants.c_dt_timestamp_9_wtz,
                  partition_constants.c_to_timestamp_tz,
                  NULL) AS high_value_convert_function,
           -- high value format will be used with convert_function so it must be enclosed in '' if set
           nvl2(d.high_value_format, partition_constants.c_apostroph || d.high_value_format || partition_constants.c_apostroph, NULL) AS high_value_format, -- Set the is_timestamp flag to simplify handling inside the code if data type is DATE or TIMESTAMP
           -- Improve performance by storing converted results
           CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS high_value_ts_tz,
           regexp_instr(dtc.data_type, 'DATE|TIMESTAMP') AS is_timestamp,
           d.auto_adjust_enabled,
           d.auto_adjust_function,
           d.auto_adjust_value,
           d.auto_adjust_end,
           d.auto_adjust_start,
           d.moving_window,
           d.moving_window_function,
           -- NUMBER attribute to store the process action for this partition definition. Default is 'Partition not found'
           to_number(c_pa_not_found) AS process_action,
           -- NUMBER attribute to store the index of the existing partition in table_parts() array for the process action
           to_number(-1) AS process_index,
           -- Scale of data type (Only used for NUMBER)
           to_number(nvl(dtc.data_scale, 0)) AS data_scale
      FROM p_def d
      JOIN p_key k
        ON (k.p_obj_id = d.p_obj_id)
      JOIN p_obj o
        ON (o.id = k.p_obj_id)
      JOIN p_ptype t
        ON (t.id = k.p_ptype_id)
    -- Join DBA_TAB_COLUMNS to get the data type of the partition key for tables.
    -- LEFT JOIN will make sure that the field is empty if the current partition key is different
    -- or the table is currently not partitioned
      LEFT JOIN dba_tab_columns dtc
        ON (dtc.owner = o.owner AND dtc.table_name = o.name AND dtc.column_name = k.column_name)
    -- Join DBA_IND_COLUMNS to get the data type of the partition key for indexes.
      LEFT JOIN dba_ind_columns dic
        ON (dic.index_owner = o.owner AND dic.index_name = o.name)
      LEFT JOIN dba_tab_columns dicc
        ON (dicc.owner = dic.table_owner AND dicc.table_name = dic.table_name AND dicc.column_name = dic.column_name)
     WHERE k.p_obj_id = p_obj_id_in
       AND k.p_ptype_id = p_ptype_id_in
       AND d.enabled = partition_constants.c_enabled
          -- If partition definition is loaded (partition#_in is NULL), then subpartition# must be 0       
       AND d.subpartition# = nvl2(partition#_in, d.subpartition#, 0)
          -- If subpartition definition is loaded (partition#_in is NOT NULL), then the partition# must be partition#_in
       AND nvl2(partition#_in, d.partition#, 0) = nvl2(partition#_in, partition#_in, 0)
          -- If subpartition definition is loaded (partition#_in is NOT NULL), then the subpartition# must not be 0
       AND nvl2(partition#_in, d.subpartition#, -1) <> nvl2(partition#_in, 0, -2)
     ORDER BY k.p_tech_id, d.partition#, d.subpartition#;

  /*****************************************************************************
  * Load object definitions
  *****************************************************************************/
  PROCEDURE load_object_defs(obj_defs_out OUT NOCOPY partition_types.t_object_table);

  /*****************************************************************************
  * Handle object
  *****************************************************************************/
  PROCEDURE handle_object(obj_def_in IN partition_object%ROWTYPE);

  /*****************************************************************************
  * Main procedure to handle all partitions
  *****************************************************************************/
  PROCEDURE handle_partitions;
END partition_handler;
/
CREATE OR REPLACE PACKAGE BODY partition_handler IS
  /*****************************************************************************
  * Constants
  *****************************************************************************/
  -- Default parallel degree
  c_default_parallel_degree  CONSTANT BINARY_INTEGER := 4;
  c_default_ddl_lock_timeout CONSTANT BINARY_INTEGER := 300;
  -- Messages
  c_config_check_required   CONSTANT VARCHAR2(30 CHAR) := 'Configuration check required! ';
  c_feature_not_implemented CONSTANT VARCHAR2(30 CHAR) := 'Feature not yet implemented';
  c_start_partition_handler CONSTANT VARCHAR2(30 CHAR) := 'Start Partition Handler';
  c_ended_partition_handler CONSTANT VARCHAR2(30 CHAR) := 'Ended Partition Handler';
  c_program_error           CONSTANT VARCHAR2(50 CHAR) := 'Programming Error. Contact the Support!';
  -- Statement extensions
  c_alter                 CONSTANT VARCHAR2(30 CHAR) := 'ALTER ';
  c_tablespace            CONSTANT VARCHAR2(30 CHAR) := ' TABLESPACE ';
  c_update_indexes        CONSTANT VARCHAR2(30 CHAR) := ' UPDATE INDEXES ';
  c_update_global_indexes CONSTANT VARCHAR2(30 CHAR) := ' UPDATE GLOBAL INDEXES ';
  c_parallel              CONSTANT VARCHAR2(10 CHAR) := ' PARALLEL ';
  c_move                  CONSTANT VARCHAR2(10 CHAR) := ' MOVE ';
  c_drop                  CONSTANT VARCHAR2(10 CHAR) := ' DROP ';
  c_split                 CONSTANT VARCHAR2(10 CHAR) := ' SPLIT ';
  c_merge                 CONSTANT VARCHAR2(10 CHAR) := ' MERGE ';
  c_rename                CONSTANT VARCHAR2(10 CHAR) := ' RENAME ';

  /*****************************************************************************
  * Global variables
  *****************************************************************************/
  g_nls_lanugage     nls_database_parameters.value%TYPE;
  g_nls_territory    nls_database_parameters.value%TYPE;
  g_parallel_degree  BINARY_INTEGER := c_default_parallel_degree;
  g_ddl_lock_timeout BINARY_INTEGER := c_default_ddl_lock_timeout;

  /*****************************************************************************
  * Exceptions
  *****************************************************************************/
  -- Oracle Exceptions
  e_invalid_identifier EXCEPTION; -- ORA-00904 Invalid identifier
  PRAGMA EXCEPTION_INIT(e_invalid_identifier, -00904);
  e_right_bracket_missing EXCEPTION; -- ORA-00907 Right bracket missing
  PRAGMA EXCEPTION_INIT(e_right_bracket_missing, -00907);
  e_invalid_sign EXCEPTION; -- ORA-00911 Invalid sign
  PRAGMA EXCEPTION_INIT(e_invalid_sign, -00911);
  e_tablespace_does_not_exist EXCEPTION; -- ORA-00959 Tablespace does not exist
  PRAGMA EXCEPTION_INIT(e_tablespace_does_not_exist, -00959);
  e_insufficient_privileges EXCEPTION; -- ORA-01031 insufficient privileges
  PRAGMA EXCEPTION_INIT(e_insufficient_privileges, -01031);
  e_unable_to_create_init_extent EXCEPTION; -- ORA-01658
  PRAGMA EXCEPTION_INIT(e_unable_to_create_init_extent, -01658);
  e_invalid_alter_table_option EXCEPTION; -- Error in generated ALTER TABLE statement
  PRAGMA EXCEPTION_INIT(e_invalid_alter_table_option, -01735);
  e_date_format_not_recognized EXCEPTION; -- ORA-01821 Date format not recognized
  PRAGMA EXCEPTION_INIT(e_date_format_not_recognized, -01821);
  e_date_format_picture_wrong EXCEPTION; -- ORA-01830 date format picture ends before converting entire input string
  PRAGMA EXCEPTION_INIT(e_date_format_picture_wrong, -01830);
  e_interval_invalid EXCEPTION; -- ORA-01867 Interval is invalid
  PRAGMA EXCEPTION_INIT(e_interval_invalid, -01867);
  e_no_privilege_on_tablespace EXCEPTION; -- ORA-01950 No Privilege on tablespace
  PRAGMA EXCEPTION_INIT(e_no_privilege_on_tablespace, -01950);
  e_partition_does_not_exist EXCEPTION; -- ORA-02149 Partition does not exist
  PRAGMA EXCEPTION_INIT(e_partition_does_not_exist, -02149);
  e_tablespace_name_expected EXCEPTION; -- ORA-02216 Tablespace name expected
  PRAGMA EXCEPTION_INIT(e_tablespace_name_expected, -02216);
  e_referenced_by_foreign_key EXCEPTION; -- ORA-02266: unique/primary keys in table referenced by enabled foreign keys
  PRAGMA EXCEPTION_INIT(e_referenced_by_foreign_key, -02266);
  e_numeric_or_value_error EXCEPTION; -- ORA-06502 Numeric or value error: character buffer too small
  PRAGMA EXCEPTION_INIT(e_numeric_or_value_error, -06502);
  e_invalid_partition_name EXCEPTION; -- ORA-14006 Invalid partition name
  PRAGMA EXCEPTION_INIT(e_invalid_partition_name, -14006);
  e_assigned_name_not_distinct EXCEPTION; -- ORA-14011 names assigned to resulting partitions must be distinct
  PRAGMA EXCEPTION_INIT(e_assigned_name_not_distinct, -14011);
  e_resulting_partition_name_col EXCEPTION; -- ORA-14012 resulting partition name conflicts with that of an existing partition
  PRAGMA EXCEPTION_INIT(e_resulting_partition_name_col, -14012);
  e_duplicate_partition_name EXCEPTION; -- ORA-14013 duplicate partition name
  PRAGMA EXCEPTION_INIT(e_duplicate_partition_name, -14013);
  e_partition_bound_element_max EXCEPTION; -- ORA-14019 partition bound element must be one of: string, datetime or interval literal, number, or MAXVALUE
  PRAGMA EXCEPTION_INIT(e_partition_bound_element_max, -14019);
  e_drop_high_part_of_glolal_ix EXCEPTION; -- ORA-14078 you may not drop the highest partition of a GLOBAL index
  PRAGMA EXCEPTION_INIT(e_drop_high_part_of_glolal_ix, -14078);
  e_partition_name_must_differ EXCEPTION; -- ORA-14082 new partition name must differ from that of any other partition of the object
  PRAGMA EXCEPTION_INIT(e_partition_name_must_differ, -14082);
  e_incomplete_part_bound_date EXCEPTION; -- ORA-14120  incompletely specified partition bound for a DATE column
  PRAGMA EXCEPTION_INIT(e_incomplete_part_bound_date, -14120);
  e_dublicate_subpartition_name EXCEPTION; -- ORA-14159 duplicate subpartition name
  PRAGMA EXCEPTION_INIT(e_dublicate_subpartition_name, -14159);
  e_subpartition_bound_too_high EXCEPTION; -- ORA-14202 subpartition bound of subpartition \"%s\" is too high
  PRAGMA EXCEPTION_INIT(e_subpartition_bound_too_high, -14202);
  e_cannot_move_partition EXCEPTION; -- ORA-14257 Cannot move partition
  PRAGMA EXCEPTION_INIT(e_cannot_move_partition, -14257);
  e_new_subpartname_must_differ EXCEPTION; -- ORA-14263 new subpartition name must differ from that of any other subpartition of the object
  PRAGMA EXCEPTION_INIT(e_new_subpartname_must_differ, -14263);
  e_partition_bound_element_null EXCEPTION; -- ORA-14308 Partition bound element must be one of: string, datetime or interval literal, number, or NULL
  PRAGMA EXCEPTION_INIT(e_partition_bound_element_null, -14308);
  e_unable_to_extend_segment EXCEPTION; -- ORA-30036
  PRAGMA EXCEPTION_INIT(e_unable_to_extend_segment, -30036);
  e_with_time_zone_missing EXCEPTION; -- ORA-30078
  PRAGMA EXCEPTION_INIT(e_with_time_zone_missing, -30078);
  -- Partition Handler exceptions
  e_object_does_not_exist EXCEPTION; -- The object does not exist
  PRAGMA EXCEPTION_INIT(e_object_does_not_exist, -20000);
  e_invalid_column_name EXCEPTION; -- Column does not exist
  PRAGMA EXCEPTION_INIT(e_invalid_column_name, -20001);
  e_first_type_is_subpartition EXCEPTION; -- Configuration error in P_DEF
  PRAGMA EXCEPTION_INIT(e_first_type_is_subpartition, -20002);
  e_wrong_partition_technique EXCEPTION; -- Configuration error in P_LIST, P_HASH or P_RANGE
  PRAGMA EXCEPTION_INIT(e_wrong_partition_technique, -20003);
  e_no_part_def_found EXCEPTION; -- No partition definition found in P_LIST, P_HASH or P_RANGE
  PRAGMA EXCEPTION_INIT(e_no_part_def_found, -20004);
  e_feature_not_implemented EXCEPTION; -- Feature is currently not implemented
  PRAGMA EXCEPTION_INIT(e_feature_not_implemented, -20005);
  e_data_type_not_supported EXCEPTION; -- Data type is not supported
  PRAGMA EXCEPTION_INIT(e_data_type_not_supported, -20006);
  --  e_no_p_naming_function EXCEPTION; -- No value assigned to P_NAMING_FUNCTION
  --  PRAGMA EXCEPTION_INIT(e_no_p_naming_function, -20007);
  e_auto_adjust_dt_not_supported EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_auto_adjust_dt_not_supported, -20008); -- Auto adjust not supported for this data type
  e_auto_adjust_function_missing EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_auto_adjust_function_missing, -20009); -- Auto adjust function missing
  e_unknown_process_action EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_unknown_process_action, -20010);
  e_idx_redefinition_impossible EXCEPTION; -- Indexes can't be redefined online
  PRAGMA EXCEPTION_INIT(e_idx_redefinition_impossible, -20011);
  e_no_moving_window_function EXCEPTION; -- No moving window function provided
  PRAGMA EXCEPTION_INIT(e_no_moving_window_function, -20012);
  e_invalid_configuration EXCEPTION; -- Configuration error
  PRAGMA EXCEPTION_INIT(e_invalid_configuration, -20100);
  -- Critical Exception on redefinition
  e_critical_on_redefinition EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_critical_on_redefinition, -20999);

  -- Cursor to select existing table partitions.
  -- This is used to fill t_partition_info.
  CURSOR cur_tab_partitions(owner_in IN dba_tab_partitions.table_owner%TYPE, table_name_in IN dba_tab_partitions.table_name%TYPE) IS
    SELECT 'PARTITION' AS partition_type,
           dtp.partition_name,
           NULL AS subpartition_name,
           dtp.tablespace_name,
           dtp.high_value,
           CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS high_value_ts_tz,
           dtp.subpartition_count,
           'PARTITION ' || dtp.partition_name AS label_name
      FROM dba_tab_partitions dtp
     WHERE dtp.table_owner = owner_in
       AND dtp.table_name = table_name_in
     ORDER BY dtp.partition_position;

  -- Cursor to select existing table subpartitions
  -- Result will also be used to fill t_partition_info
  CURSOR cur_tab_subpartitions(owner_in          IN dba_tab_subpartitions.table_owner%TYPE,
                               table_name_in     IN dba_tab_subpartitions.table_name%TYPE,
                               partition_name_in IN dba_tab_subpartitions.partition_name%TYPE DEFAULT NULL) IS
    SELECT 'SUBPARTITION' AS partition_type,
           dts.partition_name,
           dts.subpartition_name,
           dts.tablespace_name,
           dts.high_value,
           CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS high_value_ts_tz,
           0 AS subpartition_count,
           'SUBPARTITION ' || dts.subpartition_name AS label_name
      FROM dba_tab_subpartitions dts
     WHERE dts.table_owner = owner_in
       AND dts.table_name = table_name_in
       AND nvl2(partition_name_in, dts.partition_name, -1) = nvl2(partition_name_in, partition_name_in, -1)
     ORDER BY dts.subpartition_position;

  -- Cursor to select existing index partitions.
  -- This is used to fill t_partition_info.
  CURSOR cur_ind_partitions(owner_in IN dba_tab_partitions.table_owner%TYPE, index_name_in IN dba_tab_partitions.table_name%TYPE) IS
    SELECT 'PARTITION',
           dip.partition_name,
           NULL AS subpartition_name,
           dip.tablespace_name,
           dip.high_value,
           CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS high_value_ts_tz,
           dip.subpartition_count,
           'PARTITION ' || dip.partition_name AS label_name
      FROM dba_ind_partitions dip
     WHERE dip.index_owner = owner_in
       AND dip.index_name = index_name_in
     ORDER BY dip.partition_position;

  -- Cursor to select existing table subpartitions
  -- Result will also be used to fill t_partition_info
  CURSOR cur_ind_subpartitions(owner_in          IN dba_tab_subpartitions.table_owner%TYPE,
                               index_name_in     IN dba_tab_subpartitions.table_name%TYPE,
                               partition_name_in IN dba_tab_subpartitions.partition_name%TYPE DEFAULT NULL) IS
    SELECT 'SUBPARTITION',
           dis.partition_name,
           dis.subpartition_name,
           dis.tablespace_name,
           dis.high_value,
           CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS high_value_ts_tz,
           0 AS subpartition_count,
           'SUBPARTITION ' || dis.subpartition_name AS label_name
      FROM dba_ind_subpartitions dis
     WHERE dis.index_owner = owner_in
       AND dis.index_name = index_name_in
       AND nvl2(partition_name_in, dis.partition_name, -1) = nvl2(partition_name_in, partition_name_in, -1)
     ORDER BY dis.subpartition_position;

  /******************************************************************************
  * Create the SQL to add a partition and execute it
  ******************************************************************************/
  PROCEDURE add_partition(obj_def_in IN partition_object%ROWTYPE, part_def_io IN OUT NOCOPY partition_types.t_partition_def) IS
    v_module_action log_admin.t_module_action := 'add_partition';
    v_sql           VARCHAR2(4000 CHAR);
  BEGIN
    log_admin.debug(obj_def_in.label_name || ': Try to add ' || part_def_io.partition_type || ' ' || part_def_io.partition_name,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    -- Create SQL to execute
    v_sql := c_alter || obj_def_in.label_name;
  
    -- If a subpartition will be added the partition must be modified
    IF part_def_io.partition_type_id = partition_constants.c_par_type_subpartition_id THEN
      v_sql := v_sql || ' MODIFY PARTITION ' || part_def_io.parent_partition_name;
    END IF;
  
    -- Add the ADD clause
    v_sql := v_sql || ' ADD ' || part_def_io.partition_type || ' ' || part_def_io.partition_name;
  
    -- Differentiate by partition technique
    IF -- RANGE
     part_def_io.partition_tech_id = partition_constants.c_par_tech_range_id THEN
      v_sql := v_sql || ' VALUES LESS THAN (';
    
      -- if a high value format is defined, use it together with the convert function
      IF part_def_io.high_value_format IS NOT NULL THEN
        v_sql := v_sql || part_def_io.high_value_convert_function || '(' || part_def_io.high_value || ', ' || part_def_io.high_value_format || ')';
      ELSE
        v_sql := v_sql || part_def_io.high_value;
      END IF; -- part_def_io.convert_function IS NOT NULL
    ELSE
      v_sql := v_sql || ' VALUES (' || part_def_io.high_value;
    END IF; -- part_def_io.p_tech_id
    -- Close brackets and add tablespace clause
    v_sql := v_sql || ')' || c_tablespace || part_def_io.tablespace_name;
    -- The clause INVALIDATE or UPDATE GLOBAL INDEXES is allowed only for ADD 
    -- partition to a HASH partitioned table or ADD subpartition to a composite partitioned table.
    -- ORA-14633: Klausel zur Verwaltung von Index bei ADD von List-Unterpartition zu einer zusammengesetzten partitionierten Tabelle nicht zulässig
    IF (part_def_io.partition_type_id = partition_constants.c_par_type_partition_id AND part_def_io.partition_tech_id = partition_constants.c_par_tech_hash_id)
    -- OR (part_def_io.partition_type_id = partition_constants.c_par_type_subpartition_id AND part_def_io.partition_tech_id != partition_constants.c_par_tech_list_id) 
     THEN
      v_sql := v_sql || c_update_indexes;
    END IF;
    sql_admin.execute_sql(sql_in => v_sql);
    -- Set process action to added
    part_def_io.process_action := c_pa_added;
    -- Log info messages
    log_admin.info(obj_def_in.label_name || ' ' || part_def_io.partition_type || ' ' || part_def_io.partition_name || ' added',
                   module_name_in => $$PLSQL_UNIT,
                   module_action_in => v_module_action);
  EXCEPTION
    WHEN e_incomplete_part_bound_date THEN
      RAISE e_invalid_configuration;
    WHEN e_invalid_sign THEN
      RAISE e_invalid_configuration;
    WHEN e_date_format_not_recognized THEN
      RAISE e_invalid_configuration;
    WHEN e_tablespace_does_not_exist THEN
      RAISE e_invalid_configuration;
    WHEN e_duplicate_partition_name THEN
      log_admin.error('Can not add partition: partition name does already exist',
                      module_name_in                                            => $$PLSQL_UNIT,
                      module_action_in                                          => v_module_action,
                      sql_code_in                                               => SQLCODE,
                      sql_errm_in                                               => SQLERRM);
      RAISE e_invalid_configuration;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END add_partition;

  /*****************************************************************************
  * Drop partition
  *****************************************************************************/
  PROCEDURE drop_partition(obj_def_in IN partition_object%ROWTYPE, part_in IN partition_types.t_partition_info) IS
    v_module_action log_admin.t_module_action := 'drop_partition';
    v_sql           VARCHAR2(4000 CHAR);
  BEGIN
    log_admin.debug(obj_def_in.label_name || ': Try to drop ' || part_in.partition_type || ' ' || CASE WHEN part_in.subpartition_name IS NULL THEN
                    part_in.partition_name ELSE part_in.subpartition_name END,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    -- Create SQL to execute
    v_sql := c_alter || obj_def_in.label_name || c_drop || part_in.partition_type || ' ' || CASE
               WHEN part_in.subpartition_name IS NULL THEN
                part_in.partition_name
               ELSE
                part_in.subpartition_name
             END || c_update_indexes;
    -- Execute SQL
    sql_admin.execute_sql(sql_in => v_sql);
    -- Log information
    log_admin.info(obj_def_in.label_name || ' ' || part_in.partition_type || ' ' || CASE WHEN part_in.subpartition_name IS NULL THEN part_in.partition_name ELSE
                   part_in.subpartition_name END || ' dropped',
                   module_name_in => $$PLSQL_UNIT,
                   module_action_in => v_module_action);
  EXCEPTION
    WHEN e_drop_high_part_of_glolal_ix THEN
      log_admin.error('Can not drop partition: you may not drop the highest partition of a GLOBAL index',
                      module_name_in                                                                    => $$PLSQL_UNIT,
                      module_action_in                                                                  => v_module_action,
                      sql_code_in                                                                       => SQLCODE,
                      sql_errm_in                                                                       => SQLERRM);
      --RAISE e_invalid_configuration;
    WHEN e_referenced_by_foreign_key THEN
      log_admin.error('Can not drop partition: unique/primary keys in table referenced by enabled foreign keys',
                      module_name_in                                                                           => $$PLSQL_UNIT,
                      module_action_in                                                                         => v_module_action,
                      sql_code_in                                                                              => SQLCODE,
                      sql_errm_in                                                                              => SQLERRM);
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END drop_partition;

  /******************************************************************************/
  /* Create the SQL to split a partition/subpartition and execute it
  /******************************************************************************/
  PROCEDURE split_partition(obj_def_in    IN partition_object%ROWTYPE,
                            split_part_in IN partition_types.t_partition_info,
                            part_def_io   IN OUT NOCOPY partition_types.t_partition_def) IS
    v_module_action log_admin.t_module_action := 'split_partition';
    v_sql           VARCHAR2(4000 CHAR);
  BEGIN
    log_admin.debug(obj_def_in.label_name || ': Try to split ' || part_def_io.partition_type || ' ' || CASE WHEN split_part_in.subpartition_name IS NULL THEN
                    split_part_in.partition_name ELSE split_part_in.subpartition_name END,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  
    -- Create SQL to execute
    v_sql := c_alter || obj_def_in.label_name || c_split || part_def_io.partition_type || ' ' || CASE
             -- Differentiate by partition type. PARTITION or SUBPARTITION
               WHEN part_def_io.partition_type_id = partition_constants.c_par_type_partition_id THEN
                split_part_in.partition_name
               ELSE
                split_part_in.subpartition_name
             END || -- Differentiate by partition technique. Splitting requires different statements by different techniques.
             CASE part_def_io.partition_tech_id -- Partition by List
               WHEN partition_constants.c_par_tech_list_id THEN
                ' VALUES ' -- Partition by Range
               WHEN partition_constants.c_par_tech_range_id THEN
                ' AT '
             END || '(' || -- Differentiate by data type.
             CASE
               WHEN part_def_io.is_timestamp = 1 THEN
                part_def_io.high_value_convert_function || '(' || part_def_io.high_value || ', ' || part_def_io.high_value_format || ')'
               ELSE
                part_def_io.high_value
             END || ') INTO (' || part_def_io.partition_type || ' ' || part_def_io.partition_name || -- Add TABLESPACE clause
             c_tablespace || part_def_io.tablespace_name || ', ' || -- Differentiate by partition type. PARTITION or SUBPARTITION
             part_def_io.partition_type || ' ' || CASE
               WHEN part_def_io.partition_type_id = partition_constants.c_par_type_partition_id THEN
                split_part_in.partition_name
               ELSE
                split_part_in.subpartition_name
             END || ')' || -- If object type is Table add UPDATE GLOBAL INDEXES
             CASE
               WHEN obj_def_in.object_type = partition_constants.c_obj_type_table THEN
                c_update_global_indexes
             END || c_parallel || g_parallel_degree;
    sql_admin.execute_sql(sql_in => v_sql);
    -- Set the process action to splitted
    part_def_io.process_action := c_pa_splitted;
  
    -- Log debug message
    log_admin.info(obj_def_in.label_name || ' ' || part_def_io.partition_type || ' ' || CASE WHEN split_part_in.subpartition_name IS NULL THEN
                   split_part_in.partition_name ELSE split_part_in.subpartition_name END || ' splitted into ' || part_def_io.partition_name || ' and ' || CASE WHEN
                   split_part_in.subpartition_name IS NULL THEN split_part_in.partition_name ELSE split_part_in.subpartition_name END,
                   module_name_in => $$PLSQL_UNIT,
                   module_action_in => v_module_action);
  EXCEPTION
    WHEN e_incomplete_part_bound_date THEN
      RAISE e_invalid_configuration;
    WHEN e_invalid_sign THEN
      RAISE e_invalid_configuration;
    WHEN e_date_format_not_recognized THEN
      RAISE e_invalid_configuration;
    WHEN e_tablespace_does_not_exist THEN
      RAISE e_invalid_configuration;
    WHEN e_resulting_partition_name_col THEN
      log_admin.error(SQLERRM(SQLCODE), module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action, sql_code_in => SQLCODE, sql_errm_in => SQLERRM);
      RAISE e_invalid_configuration;
    WHEN e_assigned_name_not_distinct THEN
      log_admin.error(SQLERRM(SQLCODE), module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action, sql_code_in => SQLCODE, sql_errm_in => SQLERRM);
      RAISE e_invalid_configuration;
    WHEN e_duplicate_partition_name THEN
      log_admin.error(SQLERRM(SQLCODE), module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action, sql_code_in => SQLCODE, sql_errm_in => SQLERRM);
      RAISE e_invalid_configuration;
    WHEN e_partition_bound_element_max THEN
      log_admin.error(SQLERRM(SQLCODE), module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action, sql_code_in => SQLCODE, sql_errm_in => SQLERRM);
      RAISE e_invalid_configuration;
    WHEN e_partition_bound_element_null THEN
      log_admin.error(SQLERRM(SQLCODE), module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action, sql_code_in => SQLCODE, sql_errm_in => SQLERRM);
      RAISE e_invalid_configuration;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END split_partition;

  /*****************************************************************************
  * - Implement part_def_io
  * - Implement subpartitions
  *****************************************************************************/
  PROCEDURE merge_partitions(obj_def_in               IN partition_object%ROWTYPE,
                             partition_type_in        IN partition_types.t_partition_type_name,
                             first_partition_name_in  IN dba_tab_partitions.partition_name%TYPE,
                             second_partition_name_in IN dba_tab_partitions.partition_name%TYPE,
                             merge_partition_name_in  IN dba_tab_partitions.partition_name%TYPE,
                             tablespace_name_in       IN dba_tablespaces.tablespace_name%TYPE) IS
    v_module_action log_admin.t_module_action := 'merge_partitions';
    v_sql           VARCHAR2(4000 CHAR);
  BEGIN
    log_admin.debug(obj_def_in.label_name || ': Trying to merge partition ' || first_partition_name_in || ' and ' || second_partition_name_in,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    -- Create SQL to execute
    v_sql := c_alter || obj_def_in.label_name || c_merge || partition_type_in || 'S ' || first_partition_name_in || ', ' || second_partition_name_in ||
             ' INTO ' || partition_type_in || ' ' || merge_partition_name_in || c_tablespace || tablespace_name_in || c_parallel || g_parallel_degree ||
             c_update_global_indexes;
    sql_admin.execute_sql(sql_in => v_sql);
    -- Log message
    log_admin.info(obj_def_in.label_name || ' ' || partition_type_in || ' ' || first_partition_name_in || ' and  ' || second_partition_name_in ||
                   ' merged into ' || merge_partition_name_in,
                   module_name_in => $$PLSQL_UNIT,
                   module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
  END merge_partitions;

  /******************************************************************************
  * Rename a partition or subpartition
  ******************************************************************************/
  PROCEDURE rename_partition(obj_def_in            IN partition_object%ROWTYPE,
                             part_def_io           IN OUT NOCOPY partition_types.t_partition_def,
                             old_partition_name_in IN dba_tab_partitions.partition_name%TYPE) IS
    v_module_action log_admin.t_module_action := 'rename_partition';
    v_sql           VARCHAR2(4000 CHAR);
  BEGIN
    log_admin.debug(obj_def_in.label_name || ': Try to rename ' || part_def_io.partition_type || ' ' || old_partition_name_in || ' to ' ||
                    part_def_io.partition_name,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    -- Create SQL to execute
    v_sql := c_alter || obj_def_in.label_name || c_rename || part_def_io.partition_type || ' ' || old_partition_name_in || ' TO ' || part_def_io.partition_name;
    sql_admin.execute_sql(sql_in => v_sql);
    -- Set the process action to renamed
    part_def_io.process_action := c_pa_renamed;
    -- Log debug message
    log_admin.info(obj_def_in.label_name || ' ' || part_def_io.partition_type || ' ' || old_partition_name_in || ' renamed to ' || part_def_io.partition_name,
                   module_name_in => $$PLSQL_UNIT,
                   module_action_in => v_module_action);
  EXCEPTION
    WHEN e_duplicate_partition_name THEN
      log_admin.error(SQLERRM(SQLCODE), module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action, sql_code_in => SQLCODE, sql_errm_in => SQLERRM);
      RAISE e_invalid_configuration;
    WHEN e_partition_name_must_differ THEN
      log_admin.error(SQLERRM(SQLCODE), module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action, sql_code_in => SQLCODE, sql_errm_in => SQLERRM);
      RAISE e_invalid_configuration;
    WHEN e_new_subpartname_must_differ THEN
      log_admin.error(SQLERRM(SQLCODE), module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action, sql_code_in => SQLCODE, sql_errm_in => SQLERRM);
      RAISE e_invalid_configuration;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END rename_partition;

  /******************************************************************************
  * Move a partition
  ******************************************************************************/
  PROCEDURE move_partition(obj_def_in IN partition_object%ROWTYPE, part_def_io IN OUT NOCOPY partition_types.t_partition_def) IS
    v_module_action log_admin.t_module_action := 'move_partition';
    v_sql           VARCHAR2(4000 CHAR);
  BEGIN
    log_admin.debug(obj_def_in.label_name || ': Try to move ' || part_def_io.partition_type || ' ' || part_def_io.partition_name || ' to tablespace ' ||
                    part_def_io.tablespace_name,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    -- Create SQL to execute
    v_sql := c_alter || obj_def_in.label_name || CASE
               WHEN obj_def_in.object_type = partition_constants.c_obj_type_table THEN
                c_move
               ELSE
                ' REBUILD '
             END || part_def_io.partition_type || ' ' || part_def_io.partition_name || c_tablespace || part_def_io.tablespace_name || CASE
               WHEN obj_def_in.object_type = partition_constants.c_obj_type_index THEN
                ' ONLINE '
             END || c_parallel || g_parallel_degree;
    -- Execute SQL
    sql_admin.execute_sql(sql_in => v_sql);
    -- Set process action
    part_def_io.process_action := c_pa_moved;
    --
    log_admin.info(obj_def_in.label_name || ' ' || part_def_io.partition_type || ' ' || part_def_io.partition_name || ' move to tablespace ' ||
                   part_def_io.tablespace_name,
                   module_name_in => $$PLSQL_UNIT,
                   module_action_in => v_module_action);
  EXCEPTION
    WHEN e_tablespace_does_not_exist THEN
      log_admin.error(SQLERRM(SQLCODE), module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action, sql_code_in => SQLCODE, sql_errm_in => SQLERRM);
      RAISE e_invalid_configuration;
      -- ORA-14257 can happen when a partition should be moved which has subpartitions
    WHEN e_cannot_move_partition THEN
      NULL;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END move_partition;

  /*****************************************************************************
  * Load object definitions
  *****************************************************************************/
  PROCEDURE load_object_defs(obj_defs_out OUT NOCOPY partition_types.t_object_table) IS
    v_module_action log_admin.t_module_action := 'load_object_defs';
  BEGIN
    log_admin.debug('Load object definitions', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  
    OPEN cur_obj_defs;
  
    FETCH cur_obj_defs BULK COLLECT
      INTO obj_defs_out;
  
    CLOSE cur_obj_defs;
  
    log_admin.debug(obj_defs_out.count || ' object definition(s) loaded', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      IF cur_obj_defs%ISOPEN THEN
        CLOSE cur_obj_defs;
      END IF;
    
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END load_object_defs;

  /******************************************************************************/
  /* Load definitions
  /******************************************************************************/
  PROCEDURE load_partition_defs(obj_def_in    IN partition_object%ROWTYPE,
                                partition#_in IN partition_types.t_partition_number DEFAULT NULL,
                                defs_out      OUT NOCOPY partition_types.t_partition_def_table) IS
    v_module_action     log_admin.t_module_action := 'load_partition_defs';
    v_def_type          VARCHAR2(12 CHAR);
    n_partition_type_id PLS_INTEGER;
  BEGIN
    IF partition#_in IS NULL THEN
      v_def_type          := 'partition';
      n_partition_type_id := partition_constants.c_par_type_partition_id;
    ELSE
      v_def_type          := 'subpartition';
      n_partition_type_id := partition_constants.c_par_type_subpartition_id;
    END IF;
  
    log_admin.debug('Load ' || v_def_type || ' definition(s) of ' || obj_def_in.label_name,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  
    -- Open cursor and fetch result
    OPEN cur_part_defs(p_obj_id_in => obj_def_in.object_id, p_ptype_id_in => n_partition_type_id, partition#_in => partition#_in);
  
    FETCH cur_part_defs BULK COLLECT
      INTO defs_out;
  
    CLOSE cur_part_defs;
  
    -- Log debug information
    log_admin.debug(defs_out.count || ' ' || v_def_type || ' definition(s) loaded.', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      IF cur_part_defs%ISOPEN THEN
        CLOSE cur_part_defs;
      END IF;
    
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END load_partition_defs;

  /******************************************************************************/
  /* Load the current existing partition information from data dictionary
  /******************************************************************************/
  PROCEDURE load_dba_tab_partitions(obj_def_in IN partition_object%ROWTYPE, part_info_out OUT NOCOPY partition_types.t_partition_info_table) IS
    v_module_action log_admin.t_module_action := 'load_dba_tab_partitions';
  BEGIN
    log_admin.debug(obj_def_in.label_name || ': Loading partitions', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  
    OPEN cur_tab_partitions(owner_in => obj_def_in.object_owner, table_name_in => obj_def_in.object_name);
  
    FETCH cur_tab_partitions BULK COLLECT
      INTO part_info_out;
  
    CLOSE cur_tab_partitions;
  
    log_admin.debug(obj_def_in.label_name || ':' || part_info_out.count, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      IF cur_tab_partitions%ISOPEN THEN
        CLOSE cur_tab_partitions;
      END IF;
    
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END load_dba_tab_partitions;

  /*****************************************************************************
  * Load the current existing subpartition information from data dictionary
  *****************************************************************************/
  PROCEDURE load_dba_tab_subpartitions(obj_def_in    IN partition_object%ROWTYPE,
                                       part_name_in  IN dba_tab_subpartitions.partition_name%TYPE,
                                       part_info_out OUT NOCOPY partition_types.t_partition_info_table) IS
    v_module_action log_admin.t_module_action := 'load_dba_tab_subpartitions';
  BEGIN
    log_admin.debug(obj_def_in.label_name || ' ' || part_name_in || ': Loading subpartitions',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  
    OPEN cur_tab_subpartitions(owner_in => obj_def_in.object_owner, table_name_in => obj_def_in.object_name, partition_name_in => part_name_in);
  
    FETCH cur_tab_subpartitions BULK COLLECT
      INTO part_info_out;
  
    CLOSE cur_tab_subpartitions;
  
    log_admin.debug(obj_def_in.object_owner || ' ' || obj_def_in.label_name || ' ' || part_name_in || ':' || part_info_out.count,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      IF cur_tab_subpartitions%ISOPEN THEN
        CLOSE cur_tab_subpartitions;
      END IF;
    
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END load_dba_tab_subpartitions;

  /*****************************************************************************
  * Load the current existing partition information from data dictionary
  *****************************************************************************/
  PROCEDURE load_dba_ind_partitions(obj_def_in IN partition_object%ROWTYPE, part_info_out OUT NOCOPY partition_types.t_partition_info_table) IS
    v_module_action log_admin.t_module_action := 'load_dba_ind_partitions';
  BEGIN
    log_admin.debug(obj_def_in.label_name || ': Loading partitions', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  
    OPEN cur_ind_partitions(owner_in => obj_def_in.object_owner, index_name_in => obj_def_in.object_name);
  
    FETCH cur_ind_partitions BULK COLLECT
      INTO part_info_out;
  
    CLOSE cur_ind_partitions;
  
    log_admin.debug(obj_def_in.label_name || ':' || part_info_out.count, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      IF cur_ind_partitions%ISOPEN THEN
        CLOSE cur_ind_partitions;
      END IF;
    
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END load_dba_ind_partitions;

  /*****************************************************************************
  * Load the current existing subpartition information from data dictionary
  *****************************************************************************/
  PROCEDURE load_dba_ind_subpartitions(obj_def_in    IN partition_object%ROWTYPE,
                                       part_name_in  IN dba_ind_subpartitions.partition_name%TYPE,
                                       part_info_out OUT NOCOPY partition_types.t_partition_info_table) IS
    v_module_action log_admin.t_module_action := 'load_dba_ind_subpartitions';
  BEGIN
    log_admin.debug(obj_def_in.label_name || ' ' || part_name_in || ': Loading subpartitions',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  
    OPEN cur_ind_subpartitions(owner_in => obj_def_in.object_owner, index_name_in => obj_def_in.object_name, partition_name_in => part_name_in);
  
    FETCH cur_ind_subpartitions BULK COLLECT
      INTO part_info_out;
  
    CLOSE cur_ind_subpartitions;
  
    log_admin.debug(obj_def_in.label_name || ':' || part_info_out.count, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  
  EXCEPTION
    WHEN OTHERS THEN
      IF cur_ind_subpartitions%ISOPEN THEN
        CLOSE cur_ind_subpartitions;
      END IF;
    
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END load_dba_ind_subpartitions;

  /*****************************************************************************
  * Convert the high values from existing partitions to real timestamps.
  *****************************************************************************/
  PROCEDURE convert_obj_parts_high_value(obj_parts_io IN OUT NOCOPY partition_types.t_partition_info_table) IS
    v_module_action log_admin.t_module_action := 'convert_obj_parts_high_value';
    l_v2_tab        partition_types.t_varchar2_tab;
    l_ts_tab        partition_types.t_timestamp_tab;
  BEGIN
    log_admin.debug('Convert ' || obj_parts_io.count || ' high values', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  
    IF obj_parts_io.count = 0 THEN
      RETURN;
    END IF;
  
    -- Create table of varchar2 with all high values
    FOR i IN obj_parts_io.first .. obj_parts_io.last LOOP
      EXIT WHEN obj_parts_io(i).high_value IN(partition_constants.c_maxvalue, partition_constants.c_default);
      l_v2_tab(i) := obj_parts_io(i).high_value;
    END LOOP;
  
    -- If there is nothing to convert return.
    IF l_v2_tab.count = 0 THEN
      RETURN;
    END IF;
  
    -- Convert the high values to real timestamps
    partition_tools.convert_v2_tab_to_ts_tab(v2_tab_in => l_v2_tab, ts_tab_out => l_ts_tab);
  
    -- Assign the real timestamps to the table partition information
    FOR i IN l_ts_tab.first .. l_ts_tab.last LOOP
      obj_parts_io(i).high_value_ts_tz := l_ts_tab(i);
    END LOOP;
  
    log_admin.debug(obj_parts_io.count || ' high value(s) converted', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.error('Error while converting table partitions high value to timestamp with time zone.' || dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
  END convert_obj_parts_high_value;

  /*******************************************************************************
  * Load information about existing partitions
  *******************************************************************************/
  PROCEDURE load_existing_partitions(obj_def_in    IN partition_object%ROWTYPE,
                                     convert_ts_in IN BOOLEAN,
                                     part_info_out OUT partition_types.t_partition_info_table) IS
    v_module_action log_admin.t_module_action := 'load_existing_partitions';
  BEGIN
    IF obj_def_in.object_type = partition_constants.c_obj_type_table THEN
      load_dba_tab_partitions(obj_def_in => obj_def_in, part_info_out => part_info_out);
    ELSE
      load_dba_ind_partitions(obj_def_in => obj_def_in, part_info_out => part_info_out);
    END IF;
    IF convert_ts_in THEN
      convert_obj_parts_high_value(obj_parts_io => part_info_out);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END load_existing_partitions;

  /*******************************************************************************
  * Load information about existing subpartitions
  *******************************************************************************/
  PROCEDURE load_existing_subpartitions(obj_def_in        IN partition_object%ROWTYPE,
                                        partition_name_in IN VARCHAR2,
                                        convert_ts_in     IN BOOLEAN,
                                        part_info_out     OUT partition_types.t_partition_info_table) IS
    v_module_action log_admin.t_module_action := 'load_existing_subpartitions';
  BEGIN
    IF obj_def_in.object_type = partition_constants.c_obj_type_table THEN
      load_dba_tab_subpartitions(obj_def_in => obj_def_in, part_name_in => partition_name_in, part_info_out => part_info_out);
    ELSE
      load_dba_ind_subpartitions(obj_def_in => obj_def_in, part_name_in => partition_name_in, part_info_out => part_info_out);
    END IF;
    IF convert_ts_in THEN
      convert_obj_parts_high_value(obj_parts_io => part_info_out);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END load_existing_subpartitions;

  /*****************************************************************************
  * Set start time of run for an object
  ****************************************************************************/
  PROCEDURE set_last_run_start(object_id_in IN partition_types.t_object_id, last_run_start_ts_in IN p_obj.last_run_start_ts%TYPE DEFAULT systimestamp) IS
  BEGIN
    UPDATE p_obj o SET o.last_run_start_ts = last_run_start_ts_in WHERE o.id = object_id_in;
    COMMIT;
  END set_last_run_start;

  /*****************************************************************************
  * Set end time of run for an object
  *****************************************************************************/
  PROCEDURE set_last_run_end(object_id_in IN partition_types.t_object_id, last_run_end_ts_in IN p_obj.last_run_end_ts%TYPE DEFAULT systimestamp) IS
  BEGIN
    UPDATE p_obj o SET o.last_run_end_ts = last_run_end_ts_in WHERE o.id = object_id_in;
    COMMIT;
  END set_last_run_end;

  /*****************************************************************************
  * Helper function to create the partition definition convert literal
  *****************************************************************************/
  FUNCTION get_part_def_convert_literal(part_def_in IN partition_types.t_partition_def, idx_in IN NUMBER) RETURN VARCHAR2 IS
  BEGIN
    IF part_def_in.auto_adjust_function IS NULL THEN
      IF part_def_in.auto_adjust_value IS NULL THEN
        RETURN part_def_in.high_value_convert_function || '(' || part_def_in.high_value || ', ' || part_def_in.high_value_format || ')';
      ELSE
        RETURN part_def_in.high_value_convert_function || '(' || part_def_in.high_value || ', ' || part_def_in.high_value_format || ') + (' || idx_in || ' * ' || part_def_in.auto_adjust_value || ')';
      END IF;
    ELSE
      IF part_def_in.high_value_convert_function IS NULL THEN
        RETURN part_def_in.auto_adjust_function || '(' || part_def_in.high_value || ', (' || idx_in || ' * ' || part_def_in.auto_adjust_value || '))';
      ELSE
        RETURN part_def_in.auto_adjust_function || '(' || part_def_in.high_value_convert_function || '(' || part_def_in.high_value || ', ' || part_def_in.high_value_format || '), (' || idx_in || ' * ' || part_def_in.auto_adjust_value || '))';
      END IF;
    END IF;
  END get_part_def_convert_literal;

  /*****************************************************************************
  * Calculate the partition definitions for an auto adjust partition definition
  * for data type NUMBER
  *****************************************************************************/
  PROCEDURE calculate_part_defs_number(part_def_in IN partition_types.t_partition_def, part_defs_io IN OUT NOCOPY partition_types.t_partition_def_table) IS
    v_module_action log_admin.t_module_action := 'calculate_part_defs_number';
    n               PLS_INTEGER := nvl(part_defs_io.last, 0);
  BEGIN
    -- Loop from minus old partions until new partitions 
    FOR i IN -part_def_in.auto_adjust_start .. part_def_in.auto_adjust_end LOOP
      n := n + 1;
      -- Take over all values from the partition definition
      part_defs_io(n) := part_def_in;
      -- Calculate high value
      IF part_defs_io(n).auto_adjust_function IS NULL THEN
        part_defs_io(n).high_value := part_def_in.high_value + (i * part_def_in.auto_adjust_value);
      ELSE
        -- Create literal and convert it
        partition_tools.convert_literal_to_varchar2(literal_in => get_part_def_convert_literal(part_def_in => part_def_in, idx_in => i),
                                                    v2_out     => part_defs_io(n).high_value);
      END IF;
    END LOOP;
  EXCEPTION
    WHEN e_invalid_identifier THEN
      log_admin.error(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_right_bracket_missing THEN
      log_admin.error(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END calculate_part_defs_number;

  /*****************************************************************************
  * Calculate the high value of the partition definitions for an auto adjust 
  * partition definition for data type NUMBER with a moving window
  *****************************************************************************/
  PROCEDURE calculate_part_defs_number_mw(part_def_io IN OUT NOCOPY partition_types.t_partition_def) IS
    v_module_action log_admin.t_module_action := 'calculate_part_defs_number_mw';
  BEGIN
    -- Get the current value
    partition_tools.convert_literal_to_number(literal_in => part_def_io.moving_window_function, n_out => part_def_io.high_value);
    -- Save the current high value
    partition_config.update_high_value(obj_id_in        => part_def_io.p_obj_id,
                                       partition#_in    => part_def_io.partition#,
                                       subpartition#_in => part_def_io.subpartition#,
                                       high_value_in    => part_def_io.high_value);
    COMMIT;
  EXCEPTION
    WHEN e_invalid_identifier THEN
      log_admin.error('Function ' || part_def_io.moving_window_function || ' seems not to exist or is not accessible: ' ||
                      dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_right_bracket_missing THEN
      log_admin.error(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END calculate_part_defs_number_mw;

  /*****************************************************************************
  * Calculate the partition definitions for an auto adjust partition definition
  * for data type VARCHAR
  *****************************************************************************/
  PROCEDURE calculate_part_defs_varchar(part_def_in IN partition_types.t_partition_def, part_defs_io IN OUT NOCOPY partition_types.t_partition_def_table) IS
    v_module_action log_admin.t_module_action := 'calculate_part_defs_varchar';
    n               PLS_INTEGER := nvl(part_defs_io.last, 0);
  BEGIN
    -- Loop from minus old partitions till new partitions
    FOR i IN -part_def_in.auto_adjust_start .. part_def_in.auto_adjust_end LOOP
      n := n + 1;
      -- Take over all values from the partition definition
      part_defs_io(n) := part_def_in;
      -- Check if there is an auto_adjust_function which is currently required for VARCHAR2
      IF part_defs_io(n).auto_adjust_function IS NULL THEN
        RAISE e_auto_adjust_function_missing;
      END IF;
      -- Calculate high value
      partition_tools.convert_literal_to_varchar2(literal_in => get_part_def_convert_literal(part_def_in => part_def_in, idx_in => i),
                                                  v2_out     => part_defs_io(n).high_value);
      -- Enclosue high value with '
      part_defs_io(n).high_value := partition_constants.c_apostroph || part_defs_io(n).high_value || partition_constants.c_apostroph;
    END LOOP;
  EXCEPTION
    WHEN e_invalid_identifier THEN
      log_admin.error(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_right_bracket_missing THEN
      log_admin.error(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_auto_adjust_function_missing THEN
      log_admin.error('AUTO_ADJUST_FUNCTION must be set for data type VARCHAR2', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
      RAISE e_invalid_configuration;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END calculate_part_defs_varchar;

  /*****************************************************************************
  * Calculate the partition definitions for an auto adjust partition definition
  * for data type DATE/TIMESTAMP
  * Date's and Timestamp's will be calculated with only one SQL
  * to avoid context switches and speed up the process
  *****************************************************************************/
  PROCEDURE calculate_part_defs_ts(part_def_in IN partition_types.t_partition_def, part_defs_io IN OUT NOCOPY partition_types.t_partition_def_table) IS
    v_module_action log_admin.t_module_action := 'calculate_part_defs_ts';
    n_last_idx      PLS_INTEGER := nvl(part_defs_io.last, 0);
    n               PLS_INTEGER := 0;
    --l_v2_tab partition_tools.t_varchar2_tab;
    l_in_tab  partition_types.t_literal_format_tab;
    l_out_tab partition_types.t_ts_wltz_high_value_tab;
  BEGIN
    -- Add partition definitions
    FOR i IN -part_def_in.auto_adjust_start .. part_def_in.auto_adjust_end LOOP
      n := n + 1;
      -- Take over all values from the partition definition
      part_defs_io(n_last_idx + n) := part_def_in;
      -- For date's and timestamp's literals will be generated with the help of convert function
      -- and adjust function which will later be used to generate the real timestamp.
      l_in_tab(l_in_tab.count + 1).literal := get_part_def_convert_literal(part_def_in => part_def_in, idx_in => i);
      -- Add the high value format
      l_in_tab(l_in_tab.last).format := part_def_in.high_value_format;
    END LOOP;
  
    -- Convert
    partition_tools.convert_lf_tab_to_ts_hv_tab(lf_tab_in => l_in_tab, ts_hv_tab_out => l_out_tab);
  
    -- Loop trough all converted records and assign the converted values to the partition definition
    FOR i IN l_out_tab.first .. l_out_tab.last LOOP
      part_defs_io(n_last_idx + i).high_value_ts_tz := l_out_tab(i).ts_wltz;
      part_defs_io(n_last_idx + i).high_value := partition_constants.c_apostroph || l_out_tab(i).high_value || partition_constants.c_apostroph;
    END LOOP;
  EXCEPTION
    WHEN e_invalid_identifier THEN
      log_admin.error(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_date_format_not_recognized THEN
      log_admin.error(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_right_bracket_missing THEN
      log_admin.error(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END calculate_part_defs_ts;

  /*****************************************************************************
  * Calculate the partition definitions for an auto adjust partition definition
  * for data type DATE/TIMESTAMP with moving window enabled
  *
  * part_def_in   Moving Window Definition
  * part_defs_io  Table of partition definitions
  * current_idx_out   Index in part_defs_io of the definition which will store current data
  *****************************************************************************/
  PROCEDURE calculate_part_defs_ts_mw(part_def_in IN partition_types.t_partition_def, part_defs_io IN OUT NOCOPY partition_types.t_partition_def_table) IS
    v_module_action log_admin.t_module_action := 'calculate_part_defs_ts_mw';
    ts_current      TIMESTAMP WITH TIME ZONE := systimestamp; -- current timestamp
    n_current_idx   PLS_INTEGER := NULL;
    n_def_idx       PLS_INTEGER := NULL; -- Index of the definition which is currently prepared
    b_done          BOOLEAN := FALSE;
    i               PLS_INTEGER := 0;
  BEGIN
    -- Loop through all definitions to find the definition which stores current data
    FOR i IN part_defs_io.first .. part_defs_io.last LOOP
      -- If the current timestamp will be stored in this definition, the current definition is found
      IF (ts_current < part_defs_io(i).high_value_ts_tz) THEN
        -- Save the index of the partition which stores current data
        n_current_idx := i;
        EXIT;
      END IF;
    END LOOP;
  
    -- If the index of the definition which stores current data is the first definition
    -- old definitions must be added before the first definition
    IF n_current_idx = part_defs_io.first THEN
      -- Set the calculation factor to one more than the oldest partition. All younger partitions are already calculated
      i := part_def_in.auto_adjust_start + 1;
      -- Set the index to one lower then the first definition.
      n_def_idx := part_defs_io.first - 1;
      -- Loop while the definition which stores current data is not found or not all new partitions are added
      WHILE (NOT b_done) OR (n_def_idx >= n_current_idx - part_def_in.auto_adjust_start) LOOP
        -- Assign all value of the auto adjust definition to the current partition
        part_defs_io(n_def_idx) := part_def_in;
        -- Create literal to calculate the value of the current definition and calculate timestamp high value
        partition_tools.convert_literal_to_ts_tz(literal_in => get_part_def_convert_literal(part_def_in => part_def_in, idx_in => -i),
                                                 ts_tz_out  => part_defs_io(n_def_idx).high_value_ts_tz);
        -- Calculate literal of timestamp high value
        partition_tools.convert_ts_tz_to_literal(ts_tz_in    => part_defs_io(n_def_idx).high_value_ts_tz,
                                                 format_in   => part_def_in.high_value_format,
                                                 literal_out => part_defs_io(n_def_idx).high_value);
        -- If current data would be stored in the definition ...
        IF (ts_current < part_defs_io(n_def_idx).high_value_ts_tz) THEN
          -- Set the index for the current definition
          n_current_idx := n_def_idx;
        ELSE
          b_done := TRUE;
        END IF;
        -- Increment i
        i := i + 1;
        -- Increment def_idx
        n_def_idx := n_def_idx - 1;
      END LOOP;
    ELSIF -- if no definition was found which would store current data, add new partitions
     n_current_idx IS NULL THEN
      -- Set the calculation factor to one more than the newest partition. All older partitions are already calculated
      i := part_def_in.auto_adjust_end + 1;
      -- Set the index to the index of the last definition plus 1
      n_def_idx := part_defs_io.last + 1;
      -- Loop until the definition which stores current data is found or not all new partitions are added
      WHILE (NOT b_done) OR (n_def_idx <= nvl(n_current_idx, n_def_idx) + part_def_in.auto_adjust_end) LOOP
        -- Assign all value of the auto adjust definition to the current partition
        part_defs_io(n_def_idx) := part_def_in;
        -- Create literal to calculate the value of the current definition and calculate timestamp high value
        partition_tools.convert_literal_to_ts_tz(literal_in => get_part_def_convert_literal(part_def_in => part_def_in, idx_in => i),
                                                 ts_tz_out  => part_defs_io(n_def_idx).high_value_ts_tz);
        -- Calculate literal of timestamp high value
        partition_tools.convert_ts_tz_to_literal(ts_tz_in    => part_defs_io(n_def_idx).high_value_ts_tz,
                                                 format_in   => part_def_in.high_value_format,
                                                 literal_out => part_defs_io(n_def_idx).high_value);
        -- If the definition which stores current data is not found and the current definition would store current data ...
        IF (NOT b_done) AND (ts_current < part_defs_io(n_def_idx).high_value_ts_tz) THEN
          -- ... set b_done true because the definition for current data is found
          b_done := TRUE;
          -- Save the index of the definition which stores current data
          n_current_idx := n_def_idx;
        END IF;
        -- Increment i
        i := i + 1;
        -- Increment def_idx
        n_def_idx := n_def_idx + 1;
      END LOOP;
    ELSE
      -- If the index of the definition which stores current data is too low add more old partitions
      IF n_current_idx < part_defs_io.first + part_def_in.auto_adjust_start THEN
        -- Set factor for calculation
        i := -part_def_in.auto_adjust_start - n_current_idx + 1;
        -- Set index for the definition
        n_def_idx := part_defs_io.first - 1;
        WHILE (n_def_idx >= n_current_idx - part_def_in.auto_adjust_start) LOOP
          -- Assign all value of the auto adjust definition to the current partition
          part_defs_io(n_def_idx) := part_def_in;
          -- Create literal to calculate the value of the current definition and calculate timestamp high value
          partition_tools.convert_literal_to_ts_tz(literal_in => get_part_def_convert_literal(part_def_in => part_def_in, idx_in => i),
                                                   ts_tz_out  => part_defs_io(n_def_idx).high_value_ts_tz);
          -- Calculate literal of timestamp high value
          partition_tools.convert_ts_tz_to_literal(ts_tz_in    => part_defs_io(n_def_idx).high_value_ts_tz,
                                                   format_in   => part_def_in.high_value_format,
                                                   literal_out => part_defs_io(n_def_idx).high_value);
          -- Dec i
          i := i - 1;
          -- Increment def_idx
          n_def_idx := n_def_idx - 1;
        END LOOP;
      ELSE
        -- Current data will be stored in one of the definitions so only new partitions must be added until enough new partitions are there
        -- Set the calculation factor to one more than the youngest partition. All older partitions are already calculated
        i := part_def_in.auto_adjust_end + 1;
        -- Set the index to add the new definition at the end
        n_def_idx := nvl(part_defs_io.last, 0) + 1;
        WHILE (n_def_idx <= n_current_idx + part_def_in.auto_adjust_end) LOOP
          -- Assign all value of the auto adjust definition to the current partition
          part_defs_io(n_def_idx) := part_def_in;
          -- Create literal to calculate the value of the current definition and calculate timestamp high value
          partition_tools.convert_literal_to_ts_tz(literal_in => get_part_def_convert_literal(part_def_in => part_def_in, idx_in => i),
                                                   ts_tz_out  => part_defs_io(n_def_idx).high_value_ts_tz);
          -- Calculate literal of timestamp high value
          partition_tools.convert_ts_tz_to_literal(ts_tz_in    => part_defs_io(n_def_idx).high_value_ts_tz,
                                                   format_in   => part_def_in.high_value_format,
                                                   literal_out => part_defs_io(n_def_idx).high_value);
          -- Increment i
          i := i + 1;
          -- Increment def_idx
          n_def_idx := n_def_idx + 1;
        END LOOP;
      END IF;
    END IF;
    -- Delete definitions which are older than current-auto_adjust_start
    i := part_defs_io.first;
    -- While i is lower than the amount of required old partitions, delete the definition
    WHILE i < n_current_idx - part_def_in.auto_adjust_start LOOP
      part_defs_io.delete(part_defs_io.first);
      i := i + 1;
    END LOOP;
    -- Save the current high value in the object definition
    IF n_current_idx IS NOT NULL THEN
      partition_config.update_high_value(obj_id_in        => part_defs_io(n_current_idx).p_obj_id,
                                         partition#_in    => part_defs_io(n_current_idx).partition#,
                                         subpartition#_in => part_defs_io(n_current_idx).subpartition#,
                                         high_value_in    => REPLACE(part_defs_io(n_current_idx).high_value, partition_constants.c_apostroph));
      COMMIT;
    END IF;
  EXCEPTION
    WHEN e_invalid_identifier THEN
      log_admin.error(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_date_format_not_recognized THEN
      log_admin.error(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
      RAISE;
  END calculate_part_defs_ts_mw;

  /*****************************************************************************
  * Calculate partition definition for auto arrange partitions based on the 
  * data type of the (sub)partition key
  *****************************************************************************/
  PROCEDURE calculate_part_defs(part_def_io IN OUT NOCOPY partition_types.t_partition_def, part_defs_io IN OUT NOCOPY partition_types.t_partition_def_table) IS
    v_module_action log_admin.t_module_action := 'calculate_part_defs';
    v_start_tmp     partition_types.t_auto_adjust_start := part_def_io.auto_adjust_start;
    v_end_tmp       partition_types.t_auto_adjust_end := part_def_io.auto_adjust_end;
  BEGIN
    log_admin.debug('Calculate definitions. ' || part_def_io.auto_adjust_end || ' future partitions, ' || part_def_io.auto_adjust_start ||
                    ' history partitions. ' || part_defs_io.count || ' other definition(s) in stack.',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    -- Generate value of auto_adjust_start
    partition_tools.convert_literal_to_number(literal_in => v_start_tmp, n_out => part_def_io.auto_adjust_start);
    -- Generate value of auto_adjust_end
    partition_tools.convert_literal_to_number(literal_in => v_end_tmp, n_out => part_def_io.auto_adjust_end);
    -- Case by data type
    IF -- NUMBER
     part_def_io.data_type = partition_constants.c_dt_number THEN
      -- Check if moving window feature is used
      IF part_def_io.moving_window != 0 THEN
        -- If no function is provided raise exception
        IF part_def_io.moving_window_function IS NULL THEN
          RAISE e_no_moving_window_function;
        END IF;
        -- Calculate current high value
        calculate_part_defs_number_mw(part_def_io => part_def_io);
      END IF;
      calculate_part_defs_number(part_def_in => part_def_io, part_defs_io => part_defs_io);
    ELSIF -- VARCHAR2
     part_def_io.data_type = partition_constants.c_dt_varchar THEN
      calculate_part_defs_varchar(part_def_in => part_def_io, part_defs_io => part_defs_io);
    ELSIF -- DATE/TIMESTAMP
     part_def_io.is_timestamp = 1 THEN
      calculate_part_defs_ts(part_def_in => part_def_io, part_defs_io => part_defs_io);
      -- Check if the definition is a MOVING WINDOW definition. They need additional actions
      IF part_def_io.moving_window != 0 THEN
        calculate_part_defs_ts_mw(part_def_in => part_def_io, part_defs_io => part_defs_io);
      END IF;
    ELSE
      RAISE e_auto_adjust_dt_not_supported;
    END IF;
  
    log_admin.debug(part_defs_io.count || ' definitions calculated', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN e_invalid_sign THEN
      log_admin.error('Partition definitions could not be prepared',
                      module_name_in                               => $$PLSQL_UNIT,
                      module_action_in                             => v_module_action,
                      sql_errm_in                                  => SQLERRM,
                      sql_code_in                                  => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_date_format_picture_wrong THEN
      log_admin.error('Date format picture ends before converting entire string',
                      module_name_in                                            => $$PLSQL_UNIT,
                      module_action_in                                          => v_module_action,
                      sql_errm_in                                               => SQLERRM,
                      sql_code_in                                               => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_auto_adjust_dt_not_supported THEN
      log_admin.error('Data type ' || part_def_io.data_type || ' not supported.',
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_invalid_configuration THEN
      log_admin.error('Invalid configuration',
                      module_name_in         => $$PLSQL_UNIT,
                      module_action_in       => v_module_action,
                      sql_errm_in            => SQLERRM,
                      sql_code_in            => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_no_moving_window_function THEN
      log_admin.error('No moving Window function provided',
                      module_name_in                      => $$PLSQL_UNIT,
                      module_action_in                    => v_module_action,
                      sql_errm_in                         => SQLERRM,
                      sql_code_in                         => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_interval_invalid THEN
      log_admin.error('Interval invalid', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action, sql_errm_in => SQLERRM, sql_code_in => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
      RAISE;
  END calculate_part_defs;

  /****************************************************************************/
  /* Convert the high values from existing partitions to real timestamps.
  /****************************************************************************/
  PROCEDURE convert_obj_defs_high_value(obj_defs_io IN OUT NOCOPY partition_types.t_partition_def_table) IS
    v_module_action log_admin.t_module_action := 'convert_obj_defs_high_value';
    l_v2_tab        partition_types.t_varchar2_tab;
    l_ts_tab        partition_types.t_timestamp_tab;
  BEGIN
    log_admin.debug('Convert ' || obj_defs_io.count || ' high values to timestamp with time zone',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  
    IF obj_defs_io.count = 0 THEN
      RETURN;
    END IF;
  
    -- Create table of varchar2 with all high values
    FOR i IN obj_defs_io.first .. obj_defs_io.last LOOP
      EXIT WHEN obj_defs_io(i).high_value IN(partition_constants.c_maxvalue, partition_constants.c_default);
    
      l_v2_tab(i) := obj_defs_io(i).high_value_convert_function || '(' || obj_defs_io(i).high_value || ', ' || obj_defs_io(i).high_value_format || ')';
    END LOOP;
  
    -- If there is nothing to convert return.
    IF l_v2_tab.count = 0 THEN
      RETURN;
    END IF;
  
    -- Convert the high values to real timestamps
    partition_tools.convert_v2_tab_to_ts_tab(v2_tab_in => l_v2_tab, ts_tab_out => l_ts_tab);
  
    -- Assign the real timestamps to the table partition information
    FOR i IN l_ts_tab.first .. l_ts_tab.last LOOP
      obj_defs_io(i).high_value_ts_tz := l_ts_tab(i);
    END LOOP;
  
    log_admin.debug(l_ts_tab.count || ' high values converted to timestamp with zime zone',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  EXCEPTION
    WHEN e_date_format_not_recognized THEN
      RAISE e_invalid_configuration;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
  END convert_obj_defs_high_value;

  /******************************************************************************
  * Handle partition name.
  * 1. If a naming function is set use it to generate the partition name, if not 
  *    use the provided one.
  * 2. Concate partition prefix, name and suffix to form the final partition name
  ******************************************************************************/
  PROCEDURE handle_partition_name(part_def_io IN OUT NOCOPY partition_types.t_partition_def) IS
    v_module_action log_admin.t_module_action := 'handle_partition_name';
  BEGIN
    log_admin.debug('Handle partition name for high value ' || part_def_io.high_value || ' (p_naming_function =  ' || part_def_io.partition_naming_function ||
                    ', p_name_format = ' || part_def_io.partition_name_format || ')',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  
    -- If a partitin naming function was defined call it to get the partition name
    IF part_def_io.partition_naming_function IS NOT NULL THEN
      IF -- NUMBER
       part_def_io.data_type = partition_constants.c_dt_number THEN
        -- If partition definition is RANGE ...
        IF part_def_io.partition_tech_id = partition_constants.c_par_tech_range_id THEN
          -- ... substrate the smallest possible number from the HIGH VALUE to get a number smaller than the HIGH VALUE
          partition_tools.convert_literal_to_varchar2(literal_in => part_def_io.partition_naming_function || '(' ||
                                                                    (part_def_io.high_value - to_number(1 / power(10, part_def_io.data_scale))) || ', ''' ||
                                                                    part_def_io.partition_name_format || ''')',
                                                      v2_out     => part_def_io.partition_name);
        ELSE
          partition_tools.convert_literal_to_varchar2(literal_in => part_def_io.partition_naming_function || '(' || part_def_io.high_value || ', ''' ||
                                                                    part_def_io.partition_name_format || ''')',
                                                      v2_out     => part_def_io.partition_name);
        END IF;
      ELSIF -- VARCHAR2
       part_def_io.data_type = partition_constants.c_dt_varchar THEN
        partition_tools.convert_literal_to_varchar2(literal_in => part_def_io.partition_naming_function || '(' || part_def_io.high_value || ', ''' ||
                                                                  part_def_io.partition_name_format || ''')',
                                                    v2_out     => part_def_io.partition_name);
      ELSIF -- DATE/TIMESTAMP
       part_def_io.is_timestamp = 1 THEN
        -- If partition definition is LIST use the exact high value
        IF part_def_io.partition_tech_id = partition_constants.c_par_tech_list_id THEN
          partition_tools.convert_ts_tz_to_literal(ts_tz_in    => part_def_io.high_value_ts_tz,
                                                   format_in   => part_def_io.partition_name_format,
                                                   literal_out => part_def_io.partition_name);
        ELSIF -- If partition definition is RANGE substrate 1s to get a value inside the range
         part_def_io.partition_tech_id = partition_constants.c_par_tech_range_id THEN
          partition_tools.convert_ts_tz_to_literal(ts_tz_in    => part_def_io.high_value_ts_tz - 1 / 86400,
                                                   format_in   => part_def_io.partition_name_format,
                                                   literal_out => part_def_io.partition_name);
        END IF; -- p_tech_id
      ELSE
        RAISE e_data_type_not_supported;
      END IF; -- data_type or is_timestamp
    END IF; -- p_naming_function is not null
  
    -- Concatenate prefix, resulting partition name and suffix
    part_def_io.partition_name := TRIM(part_def_io.partition_name_prefix) || TRIM(part_def_io.partition_name) || TRIM(part_def_io.partition_name_suffix);
  
    -- If parent_partition_name is not null then this is a subpartition
    IF part_def_io.parent_partition_name IS NOT NULL THEN
      -- The partition name is the concatenation of <PARTITION_NAME>_<SUBPARTITION_NAME>.
      -- This is the default naming by Oracle for subpartitions.
      part_def_io.partition_name := part_def_io.parent_partition_name || '_' || part_def_io.partition_name;
    END IF;
  
    log_admin.debug('Partition name is ' || part_def_io.partition_name, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN e_invalid_identifier THEN
      log_admin.error('Partition name could not be handled',
                      module_name_in                       => $$PLSQL_UNIT,
                      module_action_in                     => v_module_action,
                      sql_errm_in                          => SQLERRM,
                      sql_code_in                          => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_date_format_not_recognized THEN
      log_admin.error('Partition name could not be handled',
                      module_name_in                       => $$PLSQL_UNIT,
                      module_action_in                     => v_module_action,
                      sql_errm_in                          => SQLERRM,
                      sql_code_in                          => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_numeric_or_value_error THEN
      log_admin.error('Partition name too long',
                      module_name_in           => $$PLSQL_UNIT,
                      module_action_in         => v_module_action,
                      sql_errm_in              => SQLERRM,
                      sql_code_in              => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
      RAISE;
  END handle_partition_name;

  /*******************************************************************************
  * Handle the tablespace name.
  * 1. If a naming function is set use it to generate the tablespace name, if not 
  *    use the provided one.
  * 2. Concate tablespace prefix, name and suffix to form the final tablespace name
  ******************************************************************************/
  PROCEDURE handle_tablespace_name(part_def_io IN OUT NOCOPY partition_types.t_partition_def) IS
    v_module_action log_admin.t_module_action := 'handle_tablespace_name';
  BEGIN
    log_admin.debug('Handle tablespace name of ' || part_def_io.partition_type || ' ' || part_def_io.partition_name || ' with high value ' ||
                    part_def_io.high_value || ' (tbs_naming_function = ' || part_def_io.tablespace_naming_function || ', tbs_name_format = ' ||
                    part_def_io.tablespace_name_format || ')',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  
    -- If a tablespace naming function was defined generate the tablespace name based on this function
    IF part_def_io.tablespace_naming_function IS NOT NULL THEN
      -- Convert the high value to literal. Must be made different depending on the data type
      IF -- NUMBER
       part_def_io.data_type = partition_constants.c_dt_number THEN
        IF part_def_io.partition_tech_id = partition_constants.c_par_tech_range_id THEN
          -- ... substrate the smallest possible number from the HIGH VALUE to get a number smaller than the HIGH VALUE
          partition_tools.convert_literal_to_varchar2(literal_in => part_def_io.tablespace_naming_function || '(' ||
                                                                    (part_def_io.high_value - to_number(1 / power(10, part_def_io.data_scale))) || ', ''' ||
                                                                    part_def_io.tablespace_name_format || ''')',
                                                      v2_out     => part_def_io.tablespace_name);
        ELSE
          partition_tools.convert_literal_to_varchar2(literal_in => part_def_io.tablespace_naming_function || '(' || part_def_io.high_value || ', ''' ||
                                                                    part_def_io.tablespace_name_format || ''')',
                                                      v2_out     => part_def_io.tablespace_name);
        END IF;
      ELSIF -- VARCHAR2
       part_def_io.data_type = partition_constants.c_dt_varchar THEN
        partition_tools.convert_literal_to_varchar2(literal_in => part_def_io.tablespace_naming_function || '(' || part_def_io.high_value || ', ''' ||
                                                                  part_def_io.tablespace_name_format || ''')',
                                                    v2_out     => part_def_io.tablespace_name);
      ELSIF -- DATE/TIMESTAMP
       part_def_io.is_timestamp = 1 THEN
        IF -- If partition technique is RANGE substrate 1s to get a timestamp inside the range
         part_def_io.partition_tech_id = partition_constants.c_par_tech_range_id THEN
          partition_tools.convert_literal_to_varchar2(literal_in => part_def_io.tablespace_naming_function || '(TO_TIMESTAMP_TZ(' ||
                                                                    to_char(part_def_io.high_value_ts_tz - numtodsinterval(1, 'SECOND'),
                                                                            part_def_io.high_value_format) || ', ' || part_def_io.high_value_format || '), ''' ||
                                                                    part_def_io.tablespace_name_format || ''')',
                                                      v2_out     => part_def_io.tablespace_name);
        ELSIF -- If partition technique is LIST use the high value
         part_def_io.partition_tech_id = partition_constants.c_par_tech_list_id THEN
          partition_tools.convert_literal_to_varchar2(literal_in => part_def_io.tablespace_naming_function || '(TO_TIMESTAMP_TZ(' ||
                                                                    to_char(part_def_io.high_value_ts_tz, part_def_io.high_value_format) || ', ' ||
                                                                    part_def_io.high_value_format || '), ''' || part_def_io.tablespace_name_format || ''')',
                                                      v2_out     => part_def_io.tablespace_name);
        ELSE
          log_admin.notice('Tablespace name generation not supported for HASH partitioning',
                           module_action_in                                                => v_module_action,
                           module_name_in                                                  => $$PLSQL_UNIT);
        END IF; -- part_def_io.p_tech_id
      END IF; -- is_timestamp = 1
      --ELSE
      --RAISE e_data_type_not_supported;
    END IF; -- tbs_naming_function IS NOT NULL
  
    -- Tablespace name is PREFIX + NAME + SUFFIX
    part_def_io.tablespace_name := TRIM(part_def_io.tablespace_name_prefix) || TRIM(part_def_io.tablespace_name) || TRIM(part_def_io.tablespace_name_suffix);
    log_admin.debug('Tablespace of ' || part_def_io.partition_type || ' ' || part_def_io.partition_name || ' is ' || part_def_io.tablespace_name,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  EXCEPTION
    WHEN e_invalid_identifier THEN
      log_admin.error('Tablespace name could not be handled',
                      module_name_in                        => $$PLSQL_UNIT,
                      module_action_in                      => v_module_action,
                      sql_errm_in                           => SQLERRM,
                      sql_code_in                           => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_date_format_not_recognized THEN
      log_admin.error('Tablespace name could not be handled',
                      module_name_in                        => $$PLSQL_UNIT,
                      module_action_in                      => v_module_action,
                      sql_errm_in                           => SQLERRM,
                      sql_code_in                           => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN e_numeric_or_value_error THEN
      log_admin.error('Tablespace name too long',
                      module_name_in            => $$PLSQL_UNIT,
                      module_action_in          => v_module_action,
                      sql_errm_in               => SQLERRM,
                      sql_code_in               => SQLCODE);
      RAISE e_invalid_configuration;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
      RAISE;
  END handle_tablespace_name;

  /*****************************************************************************
  * Handle range partition
  *****************************************************************************/
  PROCEDURE handle_range_partition(part_def_io IN OUT NOCOPY partition_types.t_partition_def, parts_io IN OUT NOCOPY partition_types.t_partition_info_table) IS
    v_module_action log_admin.t_module_action := 'handle_range_partition';
  BEGIN
    -- Loop over all exising partitions
    FOR i IN parts_io.first .. parts_io.last LOOP
      -- NUMBER and VARCHAR2 can be handled in the same way. DATE and TIMESTAMP must be handled different.
      IF part_def_io.data_type IN (partition_constants.c_dt_number, partition_constants.c_dt_varchar) THEN
        -- First check if the exiting partition is the MAXVALUE partition.
        IF parts_io(i).high_value = partition_constants.c_maxvalue THEN
          -- Check if the partition definition is also the MAXVALUE partition. If so the requested partition is found
          -- If not, the MAXVALUE partition must be splitted.
          IF part_def_io.high_value = parts_io(i).high_value THEN
            part_def_io.process_action := c_pa_found;
            part_def_io.process_index  := i;
          ELSE
            part_def_io.process_action := c_pa_must_split;
            part_def_io.process_index  := i;
          END IF;
        
          EXIT; -- Partition definition was found or will be created by a split of the MAXVALUE partition so the loop can be ended.
        ELSE
          -- If the high value of the existing partition is the high value of the
          -- partition definition then the requested partition was found and the loop can be ended
          IF part_def_io.high_value = parts_io(i).high_value THEN
            part_def_io.process_action := c_pa_found;
            part_def_io.process_index  := i;
            EXIT;
          END IF;
        
          -- If the high_value of the partition definition is lower then the high value of current exisiting partition
          -- then the existing partition must be splitted and the loop can be ended.
          IF (part_def_io.data_type = partition_constants.c_dt_varchar AND part_def_io.high_value < parts_io(i).high_value) OR
             (part_def_io.data_type = partition_constants.c_dt_number AND
             (part_def_io.high_value <> partition_constants.c_maxvalue AND to_number(part_def_io.high_value) < to_number(parts_io(i).high_value))) THEN
            part_def_io.process_action := c_pa_must_split;
            part_def_io.process_index  := i;
            EXIT;
          END IF;
        END IF; -- if high_value = MAXVALUE
        -- DATE 's
      ELSIF part_def_io.is_timestamp = 1 THEN
        -- Check if the current existing partition is the MAXVALUE partition
        IF parts_io(i).high_value = partition_constants.c_maxvalue THEN
          -- If the partition definition high value is the MAXVALUE partition the partition is found and the loop can be ended.
          IF part_def_io.high_value = parts_io(i).high_value THEN
            part_def_io.process_action := c_pa_found;
            part_def_io.process_index  := i;
          ELSE
            -- Otherwise the MAXVALUE partition needs to be splitted and the loop can be ended.
            part_def_io.process_action := c_pa_must_split;
            part_def_io.process_index  := i;
          END IF;
        
          EXIT;
        ELSE
          -- Existing partition is not the MAXVALUE partition and the partition definition
          -- is not the MAXVALUE partition
          IF part_def_io.high_value != partition_constants.c_maxvalue THEN
            -- If the existing partition was once checked then the converted timestamp is already stored in high_value_ts_tz and can be used.
            -- Otherwise the literal must be converted.
            IF parts_io(i).high_value_ts_tz IS NULL THEN
              -- Convert the high value literals to real dates
              partition_tools.convert_literal_to_ts_tz(literal_in => parts_io(i).high_value, ts_tz_out => parts_io(i).high_value_ts_tz);
            END IF;
          
            -- If the existing partition high value is higher than the high value of the partition definition
            -- the exiting partition must be splitted
            IF part_def_io.high_value_ts_tz < parts_io(i).high_value_ts_tz THEN
              part_def_io.process_action := c_pa_must_split;
              part_def_io.process_index  := i;
              EXIT;
            ELSIF part_def_io.high_value_ts_tz = parts_io(i).high_value_ts_tz THEN
              part_def_io.process_action := c_pa_found;
              part_def_io.process_index  := i;
              EXIT;
            END IF; -- part_def_io.high_value = c_maxvalue
          END IF; -- part_def_io.high_value != c_maxvalue
        END IF; -- parts_io(i).high_value = c_maxvalue
      ELSE
        -- Data type
        RAISE e_data_type_not_supported;
      END IF; -- Data type
    END LOOP;
  
    -- If partition definition could not be found it must be added
    IF part_def_io.process_action = c_pa_not_found THEN
      part_def_io.process_action := c_pa_must_add;
    END IF; -- not process_action := not_found
  EXCEPTION
    WHEN e_data_type_not_supported THEN
      log_admin.notice('Data type ' || part_def_io.data_type || ' not yet supported', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END handle_range_partition;

  /****************************************************************************
  * TODO:
  * - Merge partitions
  * - Split high_value if more than one value is defined
  ****************************************************************************/
  PROCEDURE handle_list_partition(part_def_io IN OUT NOCOPY partition_types.t_partition_def, parts_io IN OUT NOCOPY partition_types.t_partition_info_table) IS
    v_module_action         log_admin.t_module_action := 'handle_list_partition';
    l_table_part_high_value partition_types.long_varchar;
    l_part_def_high_value   partition_types.mid_varchar;
    l_v2_tab                partition_types.t_varchar2_tab;
  BEGIN
    -- If table_parts is 0 than there is no existing partition which was not handled until now, so the partition was not found.
    IF parts_io.count = 0 THEN
      part_def_io.process_action := c_pa_not_found;
    ELSE
      -- Loop through all existing partitions
      FOR i IN parts_io.first .. parts_io.last LOOP
        -- Differentiate by data type
        IF -- NUMBER and VARCHAR2
         part_def_io.data_type IN (partition_constants.c_dt_number, partition_constants.c_dt_varchar) THEN
          -- Replace all white spaces and work with temp variable to make comparison easier
          l_part_def_high_value   := REPLACE(part_def_io.high_value, ' ');
          l_table_part_high_value := REPLACE(parts_io(i).high_value, ' ');
          -- If a partition with the exact List value does already exist it's found and the loop can be ended.
          IF l_part_def_high_value = l_table_part_high_value THEN
            part_def_io.process_action := c_pa_found;
            part_def_io.process_index  := i;
            EXIT; -- Partition was found. Loop can be ended.
          END IF;
        
          -- If the high value of the existing partition is a composition (like 'A,B,C') which has the high value
          -- of the partition definition inside, the partition must be splitted and the loop can be ended.
          partition_tools.convert_v2_to_v2_tab(v2_in => l_table_part_high_value, v2_tab => l_v2_tab);
          FOR j IN l_v2_tab.first .. l_v2_tab.last LOOP
            IF (l_v2_tab(j) = l_part_def_high_value) AND (l_v2_tab.count != 1) THEN
              part_def_io.process_action := c_pa_must_split;
              part_def_io.process_index  := i;
              EXIT;
            END IF;
          END LOOP;
          EXIT WHEN part_def_io.process_action = c_pa_must_split;
        ELSIF --DATE/TIMESTAMP
         part_def_io.is_timestamp = 1 THEN
          -- Check if the definition and the existing partition are the DEFAULT partition
          IF (part_def_io.high_value = partition_constants.c_default) AND (parts_io(i).high_value = partition_constants.c_default) THEN
            part_def_io.process_action := c_pa_found;
            part_def_io.process_index  := i;
          ELSIF part_def_io.high_value_ts_tz = parts_io(i).high_value_ts_tz THEN
            part_def_io.process_action := c_pa_found;
            part_def_io.process_index  := i;
          END IF;
        ELSE
          RAISE e_data_type_not_supported;
        END IF; -- data type
      END LOOP;
    END IF; -- parts_io.count = 0;
  
    -- If the partition definition was not found in the existing partition...
    IF part_def_io.process_action = c_pa_not_found THEN
      -- ... check if the DEFAULT partition can be found
      FOR i IN parts_io.first .. parts_io.last LOOP
        IF parts_io(i).high_value = partition_constants.c_default THEN
          part_def_io.process_action := c_pa_must_split;
          part_def_io.process_index  := i;
        END IF;
      END LOOP;
    
      -- If the DEFAULT partition could not be found, partition must be added.
      IF part_def_io.process_action = c_pa_not_found THEN
        part_def_io.process_action := c_pa_must_add;
      END IF;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END handle_list_partition;

  /*****************************************************************************
  * Handle a specific partition definition
  *****************************************************************************/
  PROCEDURE handle_partition_definition(obj_def_in  IN partition_object%ROWTYPE,
                                        part_def_io IN OUT NOCOPY partition_types.t_partition_def,
                                        parts_io    IN OUT NOCOPY partition_types.t_partition_info_table) IS
    v_module_action log_admin.t_module_action := 'handle_partition_definition';
  BEGIN
    log_admin.debug(obj_def_in.label_name || ' ' || part_def_io.partition_technique || ' ' || part_def_io.partition_type || ' ' || part_def_io.partition_name,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  
    -- Handle partition definition depending on partition technique
    CASE part_def_io.partition_tech_id
      WHEN partition_constants.c_par_tech_list_id THEN
        -- Handle List partition
        handle_list_partition(part_def_io => part_def_io, parts_io => parts_io);
      WHEN partition_constants.c_par_tech_range_id THEN
        -- Handle Range partition
        handle_range_partition(part_def_io => part_def_io, parts_io => parts_io);
      WHEN partition_constants.c_par_tech_hash_id THEN
        -- Handle Hash partition
        RAISE e_feature_not_implemented;
      WHEN partition_constants.c_par_tech_interval_id THEN
        -- Handle Interval partition
        RAISE e_feature_not_implemented;
      ELSE
        RAISE e_wrong_partition_technique;
    END CASE;
  
    -- Execute requested action
    CASE part_def_io.process_action
      WHEN c_pa_found THEN
        -- If the partition was found check if the partition name and the tablespace do match.
        IF part_def_io.process_action = c_pa_found THEN
          -- Check partition name. Depending on the partition type (PARTITION or SUBPARTITION) different names must be compared
          IF (part_def_io.partition_type_id = partition_constants.c_par_type_partition_id) AND
             (part_def_io.partition_name != parts_io(part_def_io.process_index).partition_name) THEN
            rename_partition(obj_def_in => obj_def_in, part_def_io => part_def_io, old_partition_name_in => parts_io(part_def_io.process_index).partition_name);
            -- Modify table of existing partitions to reflect the new name
            parts_io(part_def_io.process_index).partition_name := part_def_io.partition_name;
          ELSIF (part_def_io.partition_type_id = partition_constants.c_par_type_subpartition_id) AND
                (part_def_io.partition_name != parts_io(part_def_io.process_index).subpartition_name) THEN
            rename_partition(obj_def_in            => obj_def_in,
                             part_def_io           => part_def_io,
                             old_partition_name_in => parts_io(part_def_io.process_index).subpartition_name);
            -- Modify table of existing partitions to reflect the new name
            parts_io(part_def_io.process_index).subpartition_name := part_def_io.partition_name;
          END IF; -- Partition name
          -- Check tablespace name to see if partition must be moved. If a partition has subpartition it can't be moved (only the subpartitions can be moved)
          IF ((part_def_io.tablespace_name != parts_io(part_def_io.process_index).tablespace_name) AND
             (parts_io(part_def_io.process_index).subpartition_count = 0)) THEN
            move_partition(obj_def_in => obj_def_in, part_def_io => part_def_io);
          END IF;
        END IF; -- c_pa_found
      WHEN c_pa_must_add THEN
        -- Add partition
        add_partition(obj_def_in => obj_def_in, part_def_io => part_def_io);
      WHEN c_pa_must_split THEN
        -- Split partition
        split_partition(obj_def_in => obj_def_in, split_part_in => parts_io(part_def_io.process_index), part_def_io => part_def_io);
      WHEN c_pa_must_merge THEN
        -- Merge partitions
        NULL;
      ELSE
        RAISE e_unknown_process_action;
    END CASE;
  
  EXCEPTION
    WHEN e_unknown_process_action THEN
      log_admin.error('Unknow process action: ' || part_def_io.process_action, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    WHEN e_invalid_configuration THEN
      log_admin.error(c_config_check_required || ' p_obj_ID = ' || obj_def_in.object_id || ', p_ptype_ID = ' || part_def_io.partition_type_id ||
                      ', PARTITION# = ' || part_def_io.partition# || ', SUBPARTITION# = ' || part_def_io.subpartition#,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_code_in => SQLCODE,
                      sql_errm_in => SQLERRM);
    WHEN e_no_privilege_on_tablespace THEN
      log_admin.error(message_in       => SQLERRM,
                      module_name_in   => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in      => SQLERRM,
                      sql_code_in      => SQLCODE);
    WHEN e_tablespace_name_expected THEN
      log_admin.error(c_program_error, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action, sql_code_in => SQLCODE, sql_errm_in => SQLERRM);
    WHEN e_tablespace_does_not_exist THEN
      log_admin.error('Tablespace ' || part_def_io.tablespace_name || ' does not exist. ' || c_config_check_required || 'p_obj_ID = ' || obj_def_in.object_id ||
                      ', p_ptype_id = ' || part_def_io.partition_type_id || ', PARTITION# = ' || part_def_io.partition# || ', SUBPARTITION# = ' ||
                      part_def_io.subpartition#,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_code_in => SQLCODE,
                      sql_errm_in => SQLERRM);
    WHEN e_invalid_partition_name THEN
      log_admin.error('Partition name ' || part_def_io.partition_name || ' is not valid. ' || c_config_check_required || ' p_obj_ID = ' ||
                      obj_def_in.object_id || ', p_ptype_id = ' || part_def_io.partition_type_id || ', PARTITION# = ' || part_def_io.partition# ||
                      ', SUBPARTITION# = ' || part_def_io.subpartition#,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_code_in => SQLCODE,
                      sql_errm_in => SQLERRM);
    WHEN e_unable_to_create_init_extent THEN
      log_admin.error('Tablespace ' || part_def_io.tablespace_name || ' must be extended',
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_code_in => SQLCODE,
                      sql_errm_in => SQLERRM);
    WHEN e_unable_to_extend_segment THEN
      log_admin.error(dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_code_in => SQLCODE,
                      sql_errm_in => SQLERRM);
    WHEN e_with_time_zone_missing THEN
      log_admin.error('HIGH_VALUE_FORMAT not correct!. ' || c_config_check_required || ' p_obj_ID = ' || obj_def_in.object_id || ', p_ptype_id = ' ||
                      part_def_io.partition_type_id || ', PARTITION# = ' || part_def_io.partition# || ', SUBPARTITION# = ' || part_def_io.subpartition#,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_code_in => SQLCODE,
                      sql_errm_in => SQLERRM);
    WHEN e_dublicate_subpartition_name THEN
      log_admin.error('Subpartition name ' || part_def_io.partition_name || ' not unique',
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_code_in => SQLCODE,
                      sql_errm_in => SQLERRM);
    WHEN e_cannot_move_partition THEN
      log_admin.error('Can not move ' || part_def_io.partition_type || ' ' || part_def_io.partition_name,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_code_in => SQLCODE,
                      sql_errm_in => SQLERRM);
    WHEN e_partition_bound_element_null THEN
      log_admin.error('High value invalid. ' || c_config_check_required || ' p_obj_ID = ' || obj_def_in.object_id || ', p_ptype_id = ' ||
                      part_def_io.partition_type_id || ', PARTITION# = ' || part_def_io.partition# || ', SUBPARTITION# = ' || part_def_io.subpartition#,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_code_in => SQLCODE,
                      sql_errm_in => SQLERRM);
    WHEN e_feature_not_implemented THEN
      log_admin.notice(c_feature_not_implemented, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    WHEN e_wrong_partition_technique THEN
      log_admin.error(c_config_check_required || 'Wrong partition technique detected for ' || obj_def_in.label_name || '.',
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action);
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END handle_partition_definition;

  /*****************************************************************************
  * Handle (sub)partition definitions of an object
  *****************************************************************************/
  PROCEDURE handle_object_partitions(obj_def_in IN partition_object%ROWTYPE, part_defs_io IN OUT NOCOPY partition_types.t_partition_def_table) IS
    v_module_action  log_admin.t_module_action := 'handle_table_partitions';
    l_existing_parts partition_types.t_partition_info_table;
    n_last_idx       PLS_INTEGER;
  BEGIN
    log_admin.debug(obj_def_in.label_name, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    -- Set last index to first one
    n_last_idx := part_defs_io.first;
  
    -- Loop through all partition definitions
    FOR i IN part_defs_io.first .. part_defs_io.last LOOP
      IF (i = n_last_idx) OR (part_defs_io(n_last_idx).partition_type <> part_defs_io(i).partition_type) OR
         (part_defs_io(n_last_idx).process_action > c_pa_found) THEN
        -- Load existing partitions if the definition is a partition (subpartition# = 0)
        IF part_defs_io(i).subpartition# = 0 THEN
          load_existing_partitions(obj_def_in => obj_def_in, part_info_out => l_existing_parts, convert_ts_in => (part_defs_io(i).is_timestamp = 1));
        ELSE
          load_existing_subpartitions(obj_def_in        => obj_def_in,
                                      partition_name_in => part_defs_io(i).parent_partition_name,
                                      convert_ts_in     => (part_defs_io(i).is_timestamp = 1),
                                      part_info_out     => l_existing_parts);
        END IF;
      END IF;
      -- Handle partition definition
      handle_partition_definition(obj_def_in => obj_def_in, part_def_io => part_defs_io(i), parts_io => l_existing_parts);
      -- Increase index
      n_last_idx := i;
    END LOOP;
    log_admin.debug(obj_def_in.label_name, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN e_invalid_alter_table_option THEN
      log_admin.critical('SQL not correct.',
                         module_name_in    => $$PLSQL_UNIT,
                         module_action_in  => v_module_action,
                         sql_code_in       => SQLCODE,
                         sql_errm_in       => SQLERRM);
    WHEN e_invalid_configuration THEN
      log_admin.error(c_config_check_required || obj_def_in.label_name,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_code_in => SQLCODE,
                      sql_errm_in => SQLERRM);
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END handle_object_partitions;

  /*******************************************************************************
  * Return TRUE if a definition for the provided partition does exist, else FALSE.
  *******************************************************************************/
  FUNCTION partition_definition_exist(part_in IN partition_types.t_partition_info, part_defs_in IN partition_types.t_partition_def_table) RETURN BOOLEAN IS
    v_module_action log_admin.t_module_action := 'partition_definition_exists';
    b_result        BOOLEAN := FALSE;
  BEGIN
    log_admin.debug(message_in => part_in.label_name, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    -- Loop trough the partition definitions and check if a definition for the current existing partition can be found
    FOR i IN part_defs_in.first .. part_defs_in.last LOOP
      EXIT WHEN b_result;
      -- If the type of the current definition is not equal to the type of the existing partition then continue
      IF part_defs_in(i).partition_type <> part_in.partition_type THEN
        continue;
      END IF;
      -- If subpartitions are compared, the name of the (parent)partition must be equal
      IF part_defs_in(i).partition_type = partition_constants.c_par_type_subpartition AND part_defs_in(i).parent_partition_name <> part_in.partition_name THEN
        continue;
      END IF;
      -- Check if the definition high value does match the partition high value
      IF part_defs_in(i).data_type IN (partition_constants.c_dt_number, partition_constants.c_dt_varchar) THEN
        -- NUMBER and VARCHAR can be compared with high value
        b_result := part_defs_in(i).high_value = part_in.high_value;
      ELSIF part_defs_in(i).is_timestamp = 1 THEN
        -- TIMESTAMPS will be compared by the timestamp value. Check also if the partition is the MAXVALUE/DEFAULT partition
        b_result := (part_defs_in(i).high_value_ts_tz = part_in.high_value_ts_tz) OR
                    part_in.high_value IN (partition_constants.c_maxvalue, partition_constants.c_default);
      ELSE
        RAISE e_data_type_not_supported;
      END IF;
    END LOOP;
    -- Return FALSE if b_Result is NULL. This can happen if one of the compared values is NULL.
    RETURN nvl(b_result, FALSE);
  EXCEPTION
    WHEN e_data_type_not_supported THEN
      log_admin.error(message_in       => c_config_check_required || 'Data type not supported',
                      module_name_in   => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in      => SQLERRM,
                      sql_code_in      => SQLCODE);
    WHEN OTHERS THEN
      log_admin.error(message_in       => SQLERRM,
                      module_name_in   => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in      => SQLERRM,
                      sql_code_in      => SQLCODE);
      RAISE;
  END partition_definition_exist;

  /*****************************************************************************
  * Handle existing Range Partition
  *
  * Check if there is a definition with a lower high value and one with a higher 
  * high value. If this is true, the existing partition must be merged into the 
  * existing partition with the higher high value. If not and moving_window = 2
  * the partition can be dropped.
  *****************************************************************************/
  PROCEDURE handle_existing_range(obj_def_in       IN partition_object%ROWTYPE,
                                  part_in          IN partition_types.t_partition_info,
                                  parts_in         IN partition_types.t_partition_info_table,
                                  moving_window_in IN partition_types.t_moving_window) IS
    v_module_action   module_admin.t_module_action := 'handle_existing_range';
    b_lower_def_found BOOLEAN := FALSE;
    n_higher_def_idx  PLS_INTEGER := 0;
  BEGIN
    log_admin.debug(message_in => obj_def_in.label_name || ' ' || part_in.label_name, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    -- Loop through all existing partitions to search a (sub)partition with a lower high value
    FOR i IN parts_in.first .. parts_in.last LOOP
      -- Compare high values of existing (sub)partition 
      IF (part_in.high_value_ts_tz IS NOT NULL AND parts_in(i).high_value_ts_tz < part_in.high_value_ts_tz) OR
         (part_in.high_value_ts_tz IS NULL AND parts_in(i).high_value < part_in.high_value) THEN
        b_lower_def_found := TRUE;
        EXIT;
      END IF;
    END LOOP;
    -- If a (sub)partition with a lower high value was found, the existing (sub)partition 
    -- must be merge into the next higher one. If not, the (sub)partition is a candidate to drop.
    IF b_lower_def_found THEN
      -- Dont touch old partitions if Moving Window is set to 2
      IF moving_window_in = 2 THEN
        RETURN;
      END IF;
    
      -- Loop through all partitions to find one with the higher high value to know the partition 
      -- in which to merge
      FOR i IN parts_in.first .. parts_in.last LOOP
        -- Compare high values of existing (sub)partition 
        IF -- TIMESTAMP
         (parts_in(i).high_value_ts_tz IS NOT NULL AND parts_in(i).high_value_ts_tz > part_in.high_value_ts_tz) OR
        -- NUMBER and VARCHAR2
         (parts_in(i).high_value_ts_tz IS NULL AND parts_in(i).high_value > part_in.high_value) OR
        -- independent of the datatype check if there is a MAXVALUE definition to merge in
         (parts_in(i).high_value = partition_constants.c_maxvalue) THEN
          n_higher_def_idx := i;
          EXIT;
        END IF;
      END LOOP;
    
      -- If a (sub)partition with a higher high value was found merge the partitions
      IF n_higher_def_idx != 0 THEN
        IF part_in.subpartition_name IS NULL THEN
          merge_partitions(obj_def_in               => obj_def_in,
                           partition_type_in        => partition_constants.c_par_type_partition,
                           first_partition_name_in  => part_in.partition_name,
                           second_partition_name_in => parts_in(n_higher_def_idx).partition_name,
                           merge_partition_name_in  => parts_in(n_higher_def_idx).partition_name,
                           tablespace_name_in       => parts_in(n_higher_def_idx).tablespace_name);
        ELSE
          merge_partitions(obj_def_in               => obj_def_in,
                           partition_type_in        => partition_constants.c_par_type_subpartition,
                           first_partition_name_in  => part_in.subpartition_name,
                           second_partition_name_in => parts_in(n_higher_def_idx).subpartition_name,
                           merge_partition_name_in  => parts_in(n_higher_def_idx).subpartition_name,
                           tablespace_name_in       => parts_in(n_higher_def_idx).tablespace_name);
        END IF;
      ELSE
        -- TODO: Check what to do with the partition 
        log_admin.notice(message_in       => obj_def_in.label_name || ' ' || part_in.partition_type || ' ' || part_in.partition_name || ' ' ||
                                             part_in.subpartition_name || ': No definition found with a higher HIGH_VALUE',
                         module_name_in   => $$PLSQL_UNIT,
                         module_action_in => v_module_action);
      END IF;
    ELSE
      -- Drop old partitions only if MOVING_WINDOW is not 2
      IF moving_window_in != 2 THEN
        drop_partition(obj_def_in => obj_def_in, part_in => part_in);
      END IF;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.error(message_in       => SQLERRM,
                      module_name_in   => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in      => SQLERRM,
                      sql_code_in      => SQLCODE);
      RAISE;
  END handle_existing_range;

  /*****************************************************************************
  * Handle an existing LIST partition
  *****************************************************************************/
  PROCEDURE handle_existing_list(obj_def_in       IN partition_object%ROWTYPE,
                                 part_in          IN partition_types.t_partition_info,
                                 parts_in         IN partition_types.t_partition_info_table,
                                 moving_window_in IN partition_types.t_moving_window) IS
    v_module_action module_admin.t_module_action := 'handle_existing_list';
  BEGIN
    -- Check if the list definition has multiple values
    IF instr(part_in.high_value, ',') != 0 THEN
      log_admin.notice(obj_def_in.label_name || ' ' || part_in.label_name || ': Handling a LIST partitioning containing multiple values not implemented yet',
                       module_name_in => $$PLSQL_UNIT,
                       module_action_in => v_module_action);
      RETURN;
    END IF;
    drop_partition(obj_def_in => obj_def_in, part_in => part_in);
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.error(message_in       => SQLERRM,
                      module_name_in   => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in      => SQLERRM,
                      sql_code_in      => SQLCODE);
      RAISE;
  END handle_existing_list;

  /*****************************************************************************
  * Handle existing partition and drop or merge it if no longer required.
  *
  * List Partitions:
  * TODO: Drop and Merge partitions
  *
  * Hash Partitions:
  * NOT SUPPORTED
  *****************************************************************************/
  PROCEDURE handle_existing_partition(obj_def_in       IN partition_object%ROWTYPE,
                                      part_in          IN partition_types.t_partition_info,
                                      parts_in         IN partition_types.t_partition_info_table,
                                      moving_window_in IN partition_types.t_moving_window) IS
    v_module_action log_admin.t_module_action := 'handle_exising_partition';
  BEGIN
    log_admin.debug(message_in => obj_def_in.label_name || ' ' || part_in.label_name, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    IF -- Range Partitioning
     (part_in.subpartition_name IS NULL AND obj_def_in.partition_technique = partition_constants.c_par_tech_range) OR
     (part_in.subpartition_name IS NOT NULL AND obj_def_in.subpartition_technique = partition_constants.c_par_tech_range) THEN
      handle_existing_range(obj_def_in => obj_def_in, part_in => part_in, parts_in => parts_in, moving_window_in => moving_window_in);
    ELSIF -- List Partitioning
     (part_in.subpartition_name IS NULL AND obj_def_in.partition_technique = partition_constants.c_par_tech_list) OR
     (part_in.subpartition_name IS NOT NULL AND obj_def_in.subpartition_technique = partition_constants.c_par_tech_list) THEN
      handle_existing_list(obj_def_in => obj_def_in, part_in => part_in, parts_in => parts_in, moving_window_in => moving_window_in);
    ELSIF -- Hash Partitioning
     (part_in.subpartition_name IS NULL AND obj_def_in.partition_technique = partition_constants.c_par_tech_hash) OR
     (part_in.subpartition_name IS NOT NULL AND obj_def_in.subpartition_technique = partition_constants.c_par_tech_hash) THEN
      log_admin.notice(obj_def_in.label_name || ' ' || part_in.label_name || ': currently not implemented for LIST partitioning',
                       module_name_in => $$PLSQL_UNIT,
                       module_action_in => v_module_action);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.error(dbms_utility.format_error_backtrace(),
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE;
  END handle_existing_partition;

  /*****************************************************************************
  * Get the maximum value of moving window for a partition definition
  *
  * TODO: Handle NO_DATA_FOUND (which must not happen !!!)
  *****************************************************************************/
  FUNCTION get_max_moving_window_main(obj_def_in IN partition_object%ROWTYPE) RETURN partition_types.t_moving_window IS
    n_result partition_types.t_moving_window;
  BEGIN
    SELECT MAX(d.moving_window)
      INTO n_result
      FROM p_def d
     WHERE d.p_obj_id = obj_def_in.object_id
       AND d.subpartition# = 0;
    RETURN n_result;
  END get_max_moving_window_main;

  /*****************************************************************************
  * Get the maximum value of moving window for a subpartition definition
  * Therefore the partition definitions must be analyzed to get the PARTITION#
  * of the main partition
  *****************************************************************************/
  FUNCTION get_max_moving_window_sub(obj_def_in        IN partition_object%ROWTYPE,
                                     part_defs_in      IN partition_types.t_partition_def_table,
                                     main_part_name_in IN partition_types.t_partition_name) RETURN partition_types.t_moving_window IS
    n_partition# partition_types.t_partition_number := 0;
    n_result     partition_types.t_moving_window;
  BEGIN
    FOR i IN part_defs_in.first .. part_defs_in.last LOOP
      -- Continue if partition type is not PARTITION
      IF part_defs_in(i).partition_type != partition_constants.c_par_type_partition THEN
        continue;
      END IF;
      -- Check names to get the PARTITION#
      IF part_defs_in(i).partition_name = main_part_name_in THEN
        n_partition# := part_defs_in(i).partition#;
        EXIT;
      END IF;
    END LOOP;
    -- Get the maximum value of MOVING_WINDOW for that main partition
    SELECT MAX(d.moving_window)
      INTO n_result
      FROM p_def d
     WHERE d.p_obj_id = obj_def_in.object_id
       AND d.partition# = n_partition#
       AND d.subpartition# != 0;
    RETURN n_result;
  END get_max_moving_window_sub;

  /*****************************************************************************
  * Handle existing partitions and drop or merge partitions which are not 
  * defined. First partitions will be handled, then subpartitions.
  *
  * TODO: Check part_defs.count = 0
  *****************************************************************************/
  PROCEDURE handle_existing_partitions(obj_def_in IN partition_object%ROWTYPE, part_defs_in IN partition_types.t_partition_def_table) IS
    v_module_action     log_admin.t_module_action := 'handle_exising_partitions';
    l_parts             partition_types.t_partition_info_table; -- List of existing partitions
    l_existing_parts    partition_types.t_partition_info_table;
    l_subparts          partition_types.t_partition_info_table; -- List of existing subpartitions
    l_existing_subparts partition_types.t_partition_info_table;
    n_main_part_mv      partition_types.t_moving_window;
    n_sub_part_mv       partition_types.t_moving_window;
  BEGIN
    log_admin.debug(obj_def_in.label_name, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  
    load_existing_partitions(obj_def_in => obj_def_in, part_info_out => l_existing_parts, convert_ts_in => (part_defs_in(1).is_timestamp = 1));
    l_parts := l_existing_parts;
    -- Get the maximum value of moving window to know what to do with existing but non-defined partitions 
    n_main_part_mv := get_max_moving_window_main(obj_def_in => obj_def_in);
  
    FOR i IN l_parts.first .. l_parts.last LOOP
      IF NOT partition_definition_exist(part_in => l_parts(i), part_defs_in => part_defs_in) THEN
        handle_existing_partition(obj_def_in => obj_def_in, part_in => l_parts(i), parts_in => l_existing_parts, moving_window_in => n_main_part_mv);
        load_existing_partitions(obj_def_in => obj_def_in, part_info_out => l_existing_parts, convert_ts_in => (part_defs_in(1).is_timestamp = 1));
      END IF;
    
      IF l_parts(i).subpartition_count = 0 THEN
        continue;
      END IF;
    
      load_existing_subpartitions(obj_def_in        => obj_def_in,
                                  partition_name_in => l_parts(i).partition_name,
                                  part_info_out     => l_existing_subparts,
                                  convert_ts_in     => (part_defs_in(2)
                                                       .partition_type = partition_constants.c_par_type_subpartition AND part_defs_in(2).is_timestamp = 1));
      l_subparts := l_existing_subparts;
      -- Get the maximum value of moving window to know what to do with existing partitions 
      -- which are not defined.
      n_sub_part_mv := get_max_moving_window_sub(obj_def_in => obj_def_in, main_part_name_in => l_parts(i).partition_name, part_defs_in => part_defs_in);
      -- Loop through all subpartitions to handle them
      FOR j IN l_subparts.first .. l_subparts.last LOOP
        IF NOT partition_definition_exist(part_in => l_subparts(j), part_defs_in => part_defs_in) THEN
          handle_existing_partition(obj_def_in => obj_def_in, part_in => l_subparts(j), parts_in => l_existing_subparts, moving_window_in => n_sub_part_mv);
          load_existing_subpartitions(obj_def_in        => obj_def_in,
                                      partition_name_in => l_parts(i).partition_name,
                                      part_info_out     => l_existing_subparts,
                                      convert_ts_in     => (part_defs_in(2)
                                                           .partition_type = partition_constants.c_par_type_subpartition AND part_defs_in(2).is_timestamp = 1));
        END IF;
      END LOOP;
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
      RAISE;
  END handle_existing_partitions;

  /*****************************************************************************
  * Prepare the partition definitions. Auto Arrange definitions will be 
  * calculated and added to the final definition table.
  *****************************************************************************/
  PROCEDURE prepare_partition_definitions(obj_def_in IN partition_object%ROWTYPE, part_defs_out OUT NOCOPY partition_types.t_partition_def_table) IS
    v_module_action log_admin.t_module_action := 'prepare_partition_definitions';
    l_part_defs     partition_types.t_partition_def_table;
    l_tmp_part_defs partition_types.t_partition_def_table;
    l_sub_part_defs partition_types.t_partition_def_table;
    l_tmp_defs      partition_types.t_partition_def_table;
  BEGIN
    log_admin.debug(obj_def_in.label_name, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    -- Load partition definitions
    load_partition_defs(obj_def_in => obj_def_in, defs_out => l_part_defs);
  
    -- If the partition technique is REFERENCE quit the function
    IF obj_def_in.partition_technique = partition_constants.c_par_tech_reference THEN
      RETURN;
    END IF;
  
    -- Check if a partition definition is defined
    IF l_part_defs.count = 0 THEN
      RAISE e_no_part_def_found;
    END IF;
  
    -- Loop through all partition definitions
    FOR i IN l_part_defs.first .. l_part_defs.last LOOP
      -- If the partition definition is an auto adjust definition calculate the partitions.
      IF l_part_defs(i).auto_adjust_enabled = 1 THEN
        -- Calculate partition definitions
        calculate_part_defs(part_def_io => l_part_defs(i), part_defs_io => l_tmp_defs);
        -- Assign calculated partition definitions to result table
        FOR j IN l_tmp_defs.first .. l_tmp_defs.last LOOP
          l_tmp_part_defs(nvl(l_tmp_part_defs.last, 0) + 1) := l_tmp_defs(j);
        END LOOP;
      ELSE
        IF -- If the partition definition is a timestamp and it's not the DEFAULT/MAXVALUE partition then convert the high value to timestamp
         (l_part_defs(i).is_timestamp = 1) AND (l_part_defs(i).high_value NOT IN (partition_constants.c_default, partition_constants.c_maxvalue)) THEN
          partition_tools.convert_literal_to_ts_tz(literal_in => get_part_def_convert_literal(part_def_in => l_part_defs(i), idx_in => 0),
                                                   ts_tz_out  => l_part_defs(i).high_value_ts_tz);
        END IF;
        -- Add the definition to result table
        l_tmp_part_defs(nvl(l_tmp_part_defs.last, 0) + 1) := l_part_defs(i);
      END IF; -- auto_adjust_enabled
    END LOOP;
  
    -- Free up space
    l_part_defs.delete;
  
    -- Loop trough first result list
    FOR i IN l_tmp_part_defs.first .. l_tmp_part_defs.last LOOP
      -- Handle partition name
      handle_partition_name(part_def_io => l_tmp_part_defs(i));
      -- Handle tablespace name
      handle_tablespace_name(part_def_io => l_tmp_part_defs(i));
      -- Add definition to result list
      part_defs_out(nvl(part_defs_out.last, 0) + 1) := l_tmp_part_defs(i);
      -- Load subpartition definitions of the current partition
      load_partition_defs(obj_def_in => obj_def_in, partition#_in => l_tmp_part_defs(i).partition#, defs_out => l_sub_part_defs);
    
      IF l_sub_part_defs.count = 0 THEN
        continue;
      END IF;
    
      FOR j IN l_sub_part_defs.first .. l_sub_part_defs.last LOOP
        -- Set parent partition name
        l_sub_part_defs(j).parent_partition_name := l_tmp_part_defs(i).partition_name;
      
        -- If subpartition definition is an auto adjust definition calculate the subpartitions.
        IF (l_sub_part_defs(j).auto_adjust_enabled = 1) THEN
          -- Clear table
          l_tmp_defs.delete;
          -- Calculate partition definitions
          calculate_part_defs(part_def_io => l_sub_part_defs(j), part_defs_io => l_tmp_defs);
        
          FOR k IN l_tmp_defs.first .. l_tmp_defs.last LOOP
            -- Handle partition name
            handle_partition_name(part_def_io => l_tmp_defs(k));
            -- Handle tablespace name
            handle_tablespace_name(part_def_io => l_tmp_defs(k));
            -- Add the definition to the result list
            part_defs_out(part_defs_out.last + 1) := l_tmp_defs(k);
          END LOOP;
        ELSE
          -- If the subpartition key is a date/timestamp and the definition is not the MAXVALUE/DEFAULT partition then convert the high values to timestamps
          IF (l_sub_part_defs(j).is_timestamp = 1) AND (l_sub_part_defs(j).high_value NOT IN (partition_constants.c_default, partition_constants.c_maxvalue)) THEN
            partition_tools.convert_literal_to_ts_tz(literal_in => get_part_def_convert_literal(part_def_in => l_sub_part_defs(j), idx_in => 0),
                                                     ts_tz_out  => l_sub_part_defs(j).high_value_ts_tz);
          END IF;
          -- Handle partition name
          handle_partition_name(part_def_io => l_sub_part_defs(j));
          -- Handle tablespace name
          handle_tablespace_name(part_def_io => l_sub_part_defs(j));
          -- Add the definition to the result list
          part_defs_out(part_defs_out.last + 1) := l_sub_part_defs(j);
        END IF;
      END LOOP;
    END LOOP;
  
    log_admin.debug(obj_def_in.label_name || ':' || part_defs_out.count, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN e_no_part_def_found THEN
      log_admin.error('No partition definition found for ' || obj_def_in.label_name,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_code_in => SQLCODE,
                      sql_errm_in => SQLERRM);
      RAISE;
    WHEN e_invalid_configuration THEN
      log_admin.error('Partition definitions could not be prepared',
                      module_name_in                               => $$PLSQL_UNIT,
                      module_action_in                             => v_module_action,
                      sql_errm_in                                  => SQLERRM,
                      sql_code_in                                  => SQLCODE);
      RAISE;
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END prepare_partition_definitions;

  /*******************************************************************************
  * Check that the object is partitioned like it is defined.
  * An object is well defined if:
  *    1. The defined partition key matches the existing one
  *    2. The defined subpartition key matches the existing one
  *    3. The defined partition technique matches the existing one
  *    4. The defined subpartition technique matches the existing one
  *******************************************************************************/
  FUNCTION object_is_well_partitioned(obj_def_in IN partition_object%ROWTYPE) RETURN BOOLEAN IS
    b_result               BOOLEAN;
    v_partitioning_type    dba_part_tables.partitioning_type%TYPE;
    v_subpartitioning_type dba_part_tables.subpartitioning_type%TYPE;
  BEGIN
    -- If the object is partitioned ...
    b_result := object_admin.is_object_partitioned(owner_in       => obj_def_in.object_owner,
                                                   object_name_in => obj_def_in.object_name,
                                                   object_type_in => obj_def_in.object_type) = 1 AND
               -- ... and the partition key is correct ...
                object_admin.is_object_partition_key(owner_in         => obj_def_in.object_owner,
                                                     object_name_in   => obj_def_in.object_name,
                                                     object_type_in   => obj_def_in.object_type,
                                                     partition_key_in => obj_def_in.partition_key) = 1 AND -- ... and the subpartition key is correct.
                (obj_def_in.subpartition_key IS NULL OR
                 
                 object_admin.is_object_subpartition_key(owner_in            => obj_def_in.object_owner,
                                                         object_name_in      => obj_def_in.object_name,
                                                         object_type_in      => obj_def_in.object_type,
                                                         subpartition_key_in => obj_def_in.subpartition_key) = 1);
    IF obj_def_in.object_type = partition_constants.c_obj_type_table THEN
      object_admin.get_table_partition_types(owner_in                 => obj_def_in.object_owner,
                                             table_name_in            => obj_def_in.object_name,
                                             partitioning_type_out    => v_partitioning_type,
                                             subpartitioning_type_out => v_subpartitioning_type);
    ELSIF obj_def_in.object_type = partition_constants.c_obj_type_index THEN
      object_admin.get_index_partition_types(owner_in                 => obj_def_in.object_owner,
                                             index_name_in            => obj_def_in.object_name,
                                             partitioning_type_out    => v_partitioning_type,
                                             subpartitioning_type_out => v_subpartitioning_type);
    ELSE
      RAISE e_invalid_configuration;
    END IF;
    RETURN(b_result AND
           ((obj_def_in.partition_technique = v_partitioning_type) AND (nvl(obj_def_in.subpartition_technique, -1) = nvl(v_subpartitioning_type, -1))));
  END object_is_well_partitioned;

  /*******************************************************************************
  * Execute the PARTITION_REDEFINE package
  *******************************************************************************/
  PROCEDURE redefine_object(obj_def_in IN partition_object%ROWTYPE, part_defs_in IN partition_types.t_partition_def_table) IS
    v_module_action log_admin.t_module_action := 'redefine_object';
  BEGIN
    partition_redefine.redefine_object(obj_def_in => obj_def_in, part_defs_in => part_defs_in);
  EXCEPTION
    WHEN OTHERS THEN
      -- Log exception but don't raise it. Otherwise the process will be finished and other partitiions not handled.
      log_admin.critical('Something went wrong during online redefinition. ' || dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
  END redefine_object;

  /*******************************************************************************
  * Handle one specific object. If the object is not partitioned in the required 
  * way it will be redefined if it is a table. Indexes can't be redefined online.
  *******************************************************************************/
  PROCEDURE handle_object(obj_def_in IN partition_object%ROWTYPE) IS
    v_module_action log_admin.t_module_action := 'handle_object';
    l_part_defs     partition_types.t_partition_def_table;
  BEGIN
    -- Set session action
    dbms_application_info.set_action(action_name => obj_def_in.label_name);
    log_admin.debug(obj_def_in.label_name, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  
    -- Check if the object does exist
    IF object_admin.is_object_existing(owner_in => obj_def_in.object_owner, object_type_in => obj_def_in.object_type, object_name_in => obj_def_in.object_name) = 0 THEN
      RAISE e_object_does_not_exist;
    END IF;
  
    set_last_run_start(object_id_in => obj_def_in.object_id, last_run_start_ts_in => systimestamp);
  
    -- Check if the partition key column does exist and if the subpartition key column does exist if required
    IF (obj_def_in.partition_key_data_type IS NULL) OR (obj_def_in.subpartition_key IS NOT NULL AND obj_def_in.subpartition_key_data_type IS NULL) THEN
      RAISE e_invalid_column_name;
    END IF;
  
    -- Prepare the partition definitions
    prepare_partition_definitions(obj_def_in => obj_def_in, part_defs_out => l_part_defs);
  
    -- Check that the object is well partitioned. If not, it must be redefined.
    IF NOT object_is_well_partitioned(obj_def_in => obj_def_in) THEN
      -- If object is an index which raise exception because indexes can't be redefined
      IF obj_def_in.object_type = partition_constants.c_obj_type_index THEN
        -- Indexes can not be redefined online
        RAISE e_idx_redefinition_impossible;
      END IF;
      -- ... else redefine the object
      redefine_object(obj_def_in => obj_def_in, part_defs_in => l_part_defs);
    ELSE
      -- Handle the partition definitions only if the object is not partitioned by reference
      IF obj_def_in.partition_technique != partition_constants.c_par_tech_reference THEN
        -- Handle partitions of this object
        handle_object_partitions(obj_def_in => obj_def_in, part_defs_io => l_part_defs);
        -- Handle existing partitions
        handle_existing_partitions(obj_def_in => obj_def_in, part_defs_in => l_part_defs);
        -- Rebuild unusable indexes if the object is a table. 
        object_admin.rebuild_unusable_indexes(table_owner_in => obj_def_in.object_owner,
                                              table_name_in  => obj_def_in.object_name,
                                              parallel_in    => g_parallel_degree);
      END IF;
    END IF;
  
    set_last_run_end(object_id_in => obj_def_in.object_id, last_run_end_ts_in => systimestamp);
  
    log_admin.debug(obj_def_in.label_name, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN e_no_part_def_found THEN
      log_admin.error(c_config_check_required || obj_def_in.label_name || ' has no defined partition definitions.',
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action);
    WHEN e_object_does_not_exist THEN
      log_admin.error(c_config_check_required || obj_def_in.label_name || ' does not exist.',
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action);
    WHEN e_invalid_column_name THEN
      log_admin.error(c_config_check_required || obj_def_in.label_name || ' (sub)partition key column does not exist.',
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action);
    WHEN e_invalid_configuration THEN
      log_admin.error(c_config_check_required || 'P_OBJ.ID = ' || obj_def_in.object_id, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    WHEN e_idx_redefinition_impossible THEN
      log_admin.error('Index online redefinition not supported', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END handle_object;

  /******************************************************************************
  * Main method
  *
  * Load object definitions and handle each object.
  ******************************************************************************/
  PROCEDURE handle_partitions IS
    v_module_action log_admin.t_module_action := 'handle_partitions';
    l_obj_defs      partition_types.t_object_table;
  BEGIN
    log_admin.info(c_start_partition_handler, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    -- Load object definition(s)
    load_object_defs(obj_defs_out => l_obj_defs);
  
    -- Loop through all object definitions and handle them
    FOR i IN 1 .. l_obj_defs.count LOOP
      handle_object(obj_def_in => l_obj_defs(i));
    END LOOP;
  
    log_admin.info(c_ended_partition_handler, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END handle_partitions;

  /******************************************************************************
  * Initialization
  ******************************************************************************/
  PROCEDURE initialize IS
    v_module_action log_admin.t_module_action := 'initialize';
    n_count         BINARY_INTEGER := 0;
  BEGIN
    dbms_application_info.set_module(module_name => $$PLSQL_UNIT, action_name => v_module_action);
    module_admin.load_and_set_config(module_name_in => $$PLSQL_UNIT);
    g_parallel_degree := nvl(module_admin.get_module_value(module_name_in => $$PLSQL_UNIT, parameter_in => 'DEGREE'), c_default_parallel_degree);
    log_admin.debug('Initializing ' || gc_module_label, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    -- Set the session time zone equal to the database session time zone to make sure
    -- that the high value of partitions with time zone will be created with the database
    -- time zone information  
    sql_admin.execute_sql(sql_in => 'ALTER SESSION SET TIME_ZONE = dbtimezone');
    log_admin.info('DBTIMEZONE = ' || dbtimezone || ', SESSIONTIMEZONE = ' || sessiontimezone,
                   module_name_in => $$PLSQL_UNIT,
                   module_action_in => v_module_action);
  
    -- Set session parameter NLS_LANGUAGE to database value to make sure that automatic generated partition names will use a consitent language
    SELECT p.value INTO g_nls_lanugage FROM nls_database_parameters p WHERE p.parameter = 'NLS_LANGUAGE';
    sql_admin.execute_sql(sql_in => 'ALTER SESSION SET NLS_DATE_LANGUAGE = ' || g_nls_lanugage);
    -- Set session parameter NLS_TERRITORY to database value to ensure consitency
    SELECT p.value INTO g_nls_territory FROM nls_database_parameters p WHERE p.parameter = 'NLS_TERRITORY';
    sql_admin.execute_sql(sql_in => 'ALTER SESSION SET NLS_TERRITORY = ' || g_nls_territory);
    -- Set DDL lock timeout to make sure the a run does not break because of a lock timeout wait
    sql_admin.execute_sql(sql_in => 'ALTER SESSION SET DDL_LOCK_TIMEOUT = ' || g_ddl_lock_timeout);
    -- Try to enable event 10397 to see errors from parallel server processes
    -- User must have ALTER SESSION privileges to set the event
    SELECT COUNT(1) INTO n_count FROM user_sys_privs WHERE privilege = 'ALTER SESSION';
    IF n_count != 0 THEN
      sql_admin.execute_sql(sql_in => 'ALTER SESSION SET EVENTS ''10397 TRACE NAME CONTEXT FOREVER''');
    END IF;
    log_admin.info('Initialized ' || gc_module_label, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END initialize;

BEGIN
  initialize;
END partition_handler;
/
