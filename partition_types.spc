CREATE OR REPLACE PACKAGE partition_types IS
  /*****************************************************************************
  * Partition Types
  *
  * Author:   Thomas Krahn
  * Date:     2014-05-06
  * Version:  0.9.25
  *
  *****************************************************************************/
  gc_module_version CONSTANT module_admin.t_module_version := '0.9.25';

  SUBTYPE short_varchar IS VARCHAR2(30 CHAR);
  SUBTYPE mid_varchar IS VARCHAR2(255 CHAR);
  SUBTYPE long_varchar IS VARCHAR2(4000 CHAR);
  SUBTYPE t_timestamp_wltz IS TIMESTAMP WITH LOCAL TIME ZONE;
  -- Generals types
  SUBTYPE t_enabled IS NUMBER(1, 0);
  -- Types of P_OTYPE
  SUBTYPE t_object_type_id IS p_otype.id%TYPE;
  SUBTYPE t_object_type_name IS p_otype.name%TYPE;
  -- Types of P_OBJ
  SUBTYPE t_object_id IS p_obj.id%TYPE;
  SUBTYPE t_object_owner IS p_obj.owner%TYPE;
  SUBTYPE t_object_name IS p_obj.name%TYPE;
  SUBTYPE t_object_order IS p_obj.order#%TYPE;
  SUBTYPE t_object_last_run_start_ts IS p_obj.last_run_start_ts%TYPE;
  SUBTYPE t_object_last_run_end_ts IS p_obj.last_run_end_ts%TYPE;
  -- Types of P_KEY
  SUBTYPE t_partition_key_name IS p_key.column_name%TYPE;
  -- Types of P_PTYPE
  SUBTYPE t_partition_type_id IS p_ptype.id%TYPE;
  SUBTYPE t_partition_type_name IS p_ptype.name%TYPE;
  -- Types of P_TECH
  SUBTYPE t_partition_technique_id IS p_tech.id%TYPE;
  SUBTYPE t_partition_technique_name IS p_tech.name%TYPE;
  -- Types of P_DEF
  SUBTYPE t_high_value IS p_def.high_value%TYPE;
  --  SUBTYPE t_high_value_format IS p_def.high_value_format%TYPE; This will result in an error because '' will be added at then beginning and end
  SUBTYPE t_high_value_format IS VARCHAR2(34 CHAR);
  SUBTYPE t_high_value_convert_function IS short_varchar;
  SUBTYPE t_high_value_ts_tz IS t_timestamp_wltz;
  SUBTYPE t_partition_name IS p_def.p_name%TYPE;
  SUBTYPE t_partition_name_prefix IS p_def.p_name_prefix%TYPE;
  SUBTYPE t_partition_name_suffix IS p_def.p_name_suffix%TYPE;
  SUBTYPE t_partition_naming_function IS p_def.p_naming_function%TYPE;
  SUBTYPE t_partition_number IS p_def.partition#%TYPE;
  SUBTYPE t_subpartition_number IS p_def.subpartition#%TYPE;
  SUBTYPE t_tablespace_name IS p_def.tbs_name%TYPE;
  SUBTYPE t_tablespace_name_prefix IS p_def.tbs_name_prefix%TYPE;
  SUBTYPE t_tablespace_name_suffix IS p_def.tbs_name_suffix%TYPE;
  SUBTYPE t_tablespace_naming_function IS p_def.tbs_naming_function%TYPE;
  SUBTYPE t_auto_adjust_enabled IS p_def.auto_adjust_enabled%TYPE;
  SUBTYPE t_auto_adjust_value IS p_def.auto_adjust_value%TYPE;
  SUBTYPE t_auto_adjust_function IS p_def.auto_adjust_function%TYPE;
  SUBTYPE t_auto_adjust_end IS p_def.auto_adjust_end%TYPE;
  SUBTYPE t_auto_adjust_start IS p_def.auto_adjust_start%TYPE;
  SUBTYPE t_moving_window IS p_def.moving_window%TYPE;
  SUBTYPE t_moving_window_function IS p_def.moving_window_function%TYPE;

  TYPE t_literal_format IS RECORD(
    literal long_varchar,
    format  mid_varchar);

  TYPE t_literal_format_tab IS TABLE OF t_literal_format INDEX BY BINARY_INTEGER;

  TYPE t_ts_wltz_high_value IS RECORD(
    ts_wltz    TIMESTAMP WITH LOCAL TIME ZONE,
    high_value long_varchar);

  TYPE t_ts_wltz_high_value_tab IS TABLE OF t_ts_wltz_high_value INDEX BY BINARY_INTEGER;

  TYPE t_varchar2_tab IS TABLE OF long_varchar INDEX BY BINARY_INTEGER;

  TYPE t_timestamp_tab IS TABLE OF TIMESTAMP WITH TIME ZONE INDEX BY BINARY_INTEGER;

  /*****************************************************************************
  * T_PARTITION_DEFINITION is a record to hold all information about a partition
  * definition.
  *****************************************************************************/
  TYPE t_partition_def IS RECORD(
    partition_type_id           t_partition_type_id,
    partition_tech_id           t_partition_technique_id,
    partition_technique         t_partition_technique_name,
    p_obj_id                    t_object_id,
    partition#                  t_partition_number,
    subpartition#               t_subpartition_number,
    data_type                   VARCHAR2(106 CHAR),
    partition_type              t_partition_type_name,
    parent_partition_name       t_partition_name,
    partition_name              t_partition_name,
    partition_name_format       t_partition_name,
    partition_name_prefix       t_partition_name_prefix,
    partition_name_suffix       t_partition_name_suffix,
    partition_naming_function   t_partition_naming_function,
    tablespace_name             t_tablespace_name,
    tablespace_name_format      t_tablespace_name,
    tablespace_name_prefix      t_tablespace_name_prefix,
    tablespace_name_suffix      t_tablespace_name_suffix,
    tablespace_naming_function  t_tablespace_naming_function,
    high_value                  t_high_value,
    high_value_convert_function t_high_value_convert_function,
    high_value_format           t_high_value_format,
    high_value_ts_tz            t_high_value_ts_tz,
    is_timestamp                NUMBER(1, 0),
    auto_adjust_enabled         t_auto_adjust_enabled,
    auto_adjust_function        t_auto_adjust_function,
    auto_adjust_value           t_auto_adjust_value,
    auto_adjust_end             t_auto_adjust_end,
    auto_adjust_start           t_auto_adjust_start,
    moving_window               t_moving_window,
    moving_window_function      t_moving_window_function,
    process_action              NUMBER,
    process_index               NUMBER,
    data_scale                  NUMBER);

  /*****************************************************************************
  * T_PARTITION_INFO is a record to hold basic information about existing 
  * partitions of a table or an index
  *****************************************************************************/
  TYPE t_partition_info IS RECORD(
    partition_type     t_partition_type_name,
    partition_name     dba_tab_partitions.partition_name%TYPE,
    subpartition_name  dba_tab_subpartitions.subpartition_name%TYPE,
    tablespace_name    dba_tab_partitions.tablespace_name%TYPE,
    high_value         dba_tab_partitions.high_value%TYPE,
    high_value_ts_tz   t_high_value_ts_tz,
    subpartition_count NUMBER,
    label_name         VARCHAR2(74 CHAR));

  TYPE t_object_table IS TABLE OF partition_object%ROWTYPE INDEX BY BINARY_INTEGER;
  TYPE t_partition_def_table IS TABLE OF t_partition_def INDEX BY BINARY_INTEGER;
  TYPE t_partition_info_table IS TABLE OF t_partition_info INDEX BY BINARY_INTEGER;

END partition_types;
/
