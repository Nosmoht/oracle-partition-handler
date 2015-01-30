CREATE OR REPLACE PACKAGE partition_constants IS
  /****************************************************************************
  * Partition Constants
  *
  * Author:    Thomas Krahn
  * Date:      2013-09-23
  * Version:   0.9.17
  ****************************************************************************/

  -- Enabled and Disabled
  c_disabled CONSTANT PLS_INTEGER := 0;
  c_enabled  CONSTANT PLS_INTEGER := 1;
  -- Max partition number
  c_max_partition# CONSTANT PLS_INTEGER := 1048575;
  -- Object types
  c_obj_type_table_id CONSTANT PLS_INTEGER := 1;
  c_obj_type_table    CONSTANT p_otype.name%TYPE := 'TABLE';
  c_obj_type_index_id CONSTANT PLS_INTEGER := 2;
  c_obj_type_index    CONSTANT p_otype.name%TYPE := 'INDEX';
  -- Partition levels
  c_par_type_partition_id    CONSTANT PLS_INTEGER := 1;
  c_par_type_partition       CONSTANT p_ptype.name%TYPE := 'PARTITION';
  c_par_type_subpartition_id CONSTANT PLS_INTEGER := 2;
  c_par_type_subpartition    CONSTANT p_ptype.name%TYPE := 'SUBPARTITION';
  -- Partition techniques
  c_par_tech_list_id      CONSTANT PLS_INTEGER := 1;
  c_par_tech_list         CONSTANT p_tech.name%TYPE := 'LIST';
  c_par_tech_range_id     CONSTANT PLS_INTEGER := 2;
  c_par_tech_range        CONSTANT p_tech.name%TYPE := 'RANGE';
  c_par_tech_hash_id      CONSTANT PLS_INTEGER := 3;
  c_par_tech_hash         CONSTANT p_tech.name%TYPE := 'HASH';
  c_par_tech_interval_id  CONSTANT PLS_INTEGER := 4;
  c_par_tech_interval     CONSTANT p_tech.name%TYPE := 'INTERVAL';
  c_par_tech_reference_id CONSTANT PLS_INTEGER := 5;
  c_par_tech_reference    CONSTANT p_tech.name%TYPE := 'REFERENCE';
  -- Data types
  c_dt_date             CONSTANT VARCHAR2(10) := 'DATE';
  c_dt_number           CONSTANT VARCHAR2(10) := 'NUMBER';
  c_dt_timestamp_0      CONSTANT VARCHAR2(12) := 'TIMESTAMP(0)';
  c_dt_timestamp_0_wtz  CONSTANT VARCHAR2(27) := 'TIMESTAMP(0) WITH TIME ZONE';
  c_dt_timestamp_0_wltz CONSTANT VARCHAR2(33) := 'TIMESTAMP(0) WITH LOCAL TIME ZONE';
  c_dt_timestamp_6      CONSTANT VARCHAR2(12) := 'TIMESTAMP(6)';
  c_dt_timestamp_6_wtz  CONSTANT VARCHAR2(27) := 'TIMESTAMP(6) WITH TIME ZONE';
  c_dt_timestamp_6_wltz CONSTANT VARCHAR2(33) := 'TIMESTAMP(6) WITH LOCAL TIME ZONE';
  c_dt_timestamp_9      CONSTANT VARCHAR2(12) := 'TIMESTAMP(9)';
  c_dt_timestamp_9_wtz  CONSTANT VARCHAR2(27) := 'TIMESTAMP(9) WITH TIME ZONE';
  c_dt_timestamp_9_wltz CONSTANT VARCHAR2(33) := 'TIMESTAMP(9) WITH LOCAL TIME ZONE';
  c_dt_varchar          CONSTANT VARCHAR2(10) := 'VARCHAR2';
  -- SQL constants
  c_apostroph       CONSTANT VARCHAR2(2 CHAR) := '''';
  c_yes             CONSTANT VARCHAR2(10 CHAR) := 'YES';
  c_no              CONSTANT VARCHAR2(10 CHAR) := 'NO';
  c_default         CONSTANT VARCHAR2(10 CHAR) := 'DEFAULT';
  c_maxvalue        CONSTANT VARCHAR2(10 CHAR) := 'MAXVALUE';
  c_to_date         CONSTANT VARCHAR2(30 CHAR) := 'TO_DATE';
  c_to_timestamp    CONSTANT VARCHAR2(30 CHAR) := 'TO_TIMESTAMP';
  c_to_timestamp_tz CONSTANT VARCHAR2(30 CHAR) := 'TO_TIMESTAMP_TZ';
  c_unknown         CONSTANT VARCHAR2(30 CHAR) := 'UNKNOWN';
END partition_constants;
/
