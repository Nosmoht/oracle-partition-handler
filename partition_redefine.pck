CREATE OR REPLACE PACKAGE partition_redefine AUTHID CURRENT_USER IS
  /*******************************************************************************
  * Partition redefinition
  *
  * Author:   Thomas Krahn
  * Date:     2014-09-04
  * Version:  0.9.18
  *
  * Requires: module_admin v1.7.0
  *           log_admin v1.2.3
  *           partition_handler v0.9.13
  ******************************************************************************/
  gc_module_version CONSTANT module_admin.t_module_version := '0.9.18';
  gc_module_label   CONSTANT module_admin.t_module_label := $$PLSQL_UNIT || ' v' || gc_module_version;

  PROCEDURE redefine_object(obj_def_in IN partition_object%ROWTYPE, part_defs_in IN partition_types.t_partition_def_table);
END partition_redefine;
/
CREATE OR REPLACE PACKAGE BODY partition_redefine IS
  /*******************************************************************************
  * Constants
  *******************************************************************************/
  --gc_stattab_name dba_tables.table_name%TYPE := 'P_STATTAB';

  /******************************************************************************
  * Exceptions
  ******************************************************************************/
  -- Oracle Exceptions
  e_resource_busy EXCEPTION; -- ORA-00054 Resource Busy
  PRAGMA EXCEPTION_INIT(e_resource_busy, -00054);
  e_object_does_already_exist EXCEPTION; -- ORA-00955 name is already used by an existing object
  PRAGMA EXCEPTION_INIT(e_object_does_already_exist, -00955);
  e_column_list_already_indexed EXCEPTION; -- ORA-01408 such column list already indexed
  PRAGMA EXCEPTION_INIT(e_column_list_already_indexed, -01408);
  e_no_privilege_on_tablespace EXCEPTION; -- ORA-01950 No Privilege on tablespace
  PRAGMA EXCEPTION_INIT(e_no_privilege_on_tablespace, -01950);
  e_cons_name_already_used EXCEPTION; -- ORA-02264 name already used by an existing constraint
  PRAGMA EXCEPTION_INIT(e_cons_name_already_used, -2264);
  e_table_referenced_by_fk EXCEPTION; -- ORA-02449: unique/primary keys in table referenced by foreign keys
  PRAGMA EXCEPTION_INIT(e_table_referenced_by_fk, -02449);
  e_self_deadlock_on_validation EXCEPTION; -- ORA-4027 self-deadlock during automatic validation for object 
  PRAGMA EXCEPTION_INIT(e_self_deadlock_on_validation, -04027);
  e_cannot_redefine_without_pk EXCEPTION; -- ORA-12089 Can not online redefine without primary key
  PRAGMA EXCEPTION_INIT(e_cannot_redefine_without_pk, -12089);
  e_incomplete_part_bound_date EXCEPTION; -- ORA-14120  incompletely specified partition bound for a DATE column
  PRAGMA EXCEPTION_INIT(e_incomplete_part_bound_date, -14120);
  e_partition_bound_element_null EXCEPTION; -- ORA-14308 Partition bound element must be one of: string, datetime or interval literal, number, or NULL
  PRAGMA EXCEPTION_INIT(e_partition_bound_element_null, -14308);
  e_ref_partition_fk_not_support EXCEPTION; -- ORA-14652
  PRAGMA EXCEPTION_INIT(e_ref_partition_fk_not_support, -14652);
  e_parent_table_not_partitioned EXCEPTION; -- ORA-14653 parent table of a reference-partitioned table must be partitioned
  PRAGMA EXCEPTION_INIT(e_parent_table_not_partitioned, -14653);
  e_object_not_found_in_schema EXCEPTION; -- ORA-31603 "object \"%s\" of type %s not found in schema \"%s\""
  PRAGMA EXCEPTION_INIT(e_object_not_found_in_schema, -31603);
  -- Application exceptions
  e_no_foreign_key_found EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_no_foreign_key_found, -20001);
  e_create_sql_not_create EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_create_sql_not_create, -20002);
  e_redefinition_impossible EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_redefinition_impossible, -20003);
  e_table_could_not_be_redefined EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_table_could_not_be_redefined, -20004);

  /******************************************************************************
  * Cursors
  ******************************************************************************/
  CURSOR c_table_constraints(owner_in IN dba_constraints.owner%TYPE, table_name_in IN dba_constraints.table_name%TYPE) IS
    SELECT owner, constraint_name, constraint_type || '_' || to_char(systimestamp, 'YYYYMMDDHH24MISSFF') || '_' || rownum AS tmp_constraint_name
      FROM dba_constraints dc
     WHERE dc.owner = owner_in
       AND dc.table_name = table_name_in
       AND generated != 'GENERATED NAME';

  CURSOR c_table_indexes_ddl(owner_in IN dba_indexes.owner%TYPE, table_name_in IN dba_indexes.table_name%TYPE) IS
    SELECT di.owner,
           di.index_name,
           'I_' || to_char(systimestamp, 'YYYYMMDDHH24MISSFF') || '_' || rownum AS tmp_index_name,
           di.uniqueness,
           (SELECT wm_concat(dic.column_name)
              FROM dba_ind_columns dic
             WHERE dic.index_owner = di.owner
               AND di.index_name = dic.index_name) AS columns,
           sys.get_object_ddl(object_type_in => 'INDEX', name_in => di.index_name, schema_in => di.owner) AS create_ddl
      FROM dba_indexes di
     WHERE di.owner = owner_in
       AND di.table_name = table_name_in
       AND di.index_type != 'LOB'
       AND di.index_name NOT IN (SELECT s.index_name
                                   FROM dba_constraints s
                                  WHERE s.owner = owner_in
                                    AND s.table_name = table_name_in
                                    AND s.constraint_type IN ('P', 'U'));

  /******************************************************************************
  * Types
  ******************************************************************************/
  TYPE t_table_constraints IS TABLE OF c_table_constraints%ROWTYPE INDEX BY BINARY_INTEGER;
  TYPE t_table_index_ddls IS TABLE OF c_table_indexes_ddl%ROWTYPE INDEX BY BINARY_INTEGER;

  /******************************************************************************/
  /* Create SQL to create the temporary table
  /******************************************************************************/
  FUNCTION get_object_ddl(obj_def_in IN partition_object%ROWTYPE, part_defs_in IN partition_types.t_partition_def_table) RETURN CLOB IS
    v_module_action          log_admin.t_module_action := 'get_object_ddl';
    v_ref_cons_name          dba_constraints.constraint_name%TYPE;
    n_last_partition_type_id PLS_INTEGER := partition_constants.c_par_type_partition_id;
    l_sql                    CLOB;
  BEGIN
    log_admin.debug(obj_def_in.object_type || ' ' || obj_def_in.label_name, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    -- Get SQL to create object
    dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SEGMENT_ATTRIBUTES', TRUE);
    dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'STORAGE', TRUE);
    dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'TABLESPACE', FALSE);
    dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'PARTITIONING', FALSE);
    dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'CONSTRAINTS', TRUE);
    dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'REF_CONSTRAINTS', TRUE);
  
    -- Get the CREATE sql
    l_sql := sys.get_object_ddl(object_type_in => obj_def_in.object_type, schema_in => obj_def_in.object_owner, name_in => obj_def_in.object_name);
    -- Add partition key clause
    IF --REFERENCE partitioning
     obj_def_in.partition_technique = partition_constants.c_par_tech_reference THEN
      -- Get the foreign key name on that partition key and use it for the REFERENCE partition
      IF obj_def_in.partition_key IS NOT NULL THEN
        v_ref_cons_name := object_admin.get_foreign_key_name_on_column(owner_in       => obj_def_in.object_owner,
                                                                       table_name_in  => obj_def_in.object_name,
                                                                       column_name_in => obj_def_in.partition_key);
        IF v_ref_cons_name IS NULL THEN
          RAISE e_no_foreign_key_found;
        END IF;
        dbms_lob.append(dest_lob => l_sql, src_lob => chr(13) || 'PARTITION BY ' || obj_def_in.partition_technique || '("' || v_ref_cons_name || '")');
      END IF;
    ELSE
      -- LIST and RANGE partitioning
      dbms_lob.append(dest_lob => l_sql, src_lob => chr(13) || 'PARTITION BY ' || obj_def_in.partition_technique || '(' || obj_def_in.partition_key || ')');
    
      -- Add subpartition key clause if used
      IF obj_def_in.subpartition_key IS NOT NULL THEN
        dbms_lob.append(dest_lob => l_sql,
                        src_lob  => chr(13) || 'SUBPARTITION BY ' || obj_def_in.subpartition_technique || '(' || obj_def_in.subpartition_key || ')');
      END IF;
    
      -- Open partition clause
      dbms_lob.append(dest_lob => l_sql, src_lob => '(');
    
      -- Add partition informations
      FOR i IN part_defs_in.first .. part_defs_in.last LOOP
        -- Add symbol for the next partition depending on last and current partition type
        -- Do it only if the current definition is not the first
        IF i != part_defs_in.first THEN
          IF (n_last_partition_type_id = partition_constants.c_par_type_partition_id) AND
             (part_defs_in(i).partition_type_id = partition_constants.c_par_type_subpartition_id) THEN
            dbms_lob.append(dest_lob => l_sql, src_lob => chr(13) || '(');
          ELSIF (n_last_partition_type_id = partition_constants.c_par_type_partition_id) AND
                (part_defs_in(i).partition_type_id = partition_constants.c_par_type_partition_id) THEN
            dbms_lob.append(dest_lob => l_sql, src_lob => ',' || chr(13));
          ELSIF (n_last_partition_type_id = partition_constants.c_par_type_subpartition_id) AND
                (part_defs_in(i).partition_type_id = partition_constants.c_par_type_partition_id) THEN
            dbms_lob.append(dest_lob => l_sql, src_lob => '),' || chr(13));
          ELSIF (n_last_partition_type_id = partition_constants.c_par_type_subpartition_id) AND
                (part_defs_in(i).partition_type_id = partition_constants.c_par_type_subpartition_id) THEN
            dbms_lob.append(dest_lob => l_sql, src_lob => ',' || chr(13));
          END IF;
        END IF;
      
        -- Add VALUE clause
        dbms_lob.append(dest_lob => l_sql, src_lob => part_defs_in(i).partition_type || ' ' || part_defs_in(i).partition_name || ' VALUES ');
      
        -- If RANGE partitioning add LESS THAN clause
        IF part_defs_in(i).partition_tech_id = partition_constants.c_par_tech_range_id THEN
          dbms_lob.append(dest_lob => l_sql, src_lob => 'LESS THAN ');
        END IF;
      
        -- Open high value brackets
        dbms_lob.append(dest_lob => l_sql, src_lob => '(');
      
        -- Add high value
        IF part_defs_in(i).high_value_format IS NOT NULL THEN
          dbms_lob.append(dest_lob => l_sql,
                          src_lob  => part_defs_in(i)
                                      .high_value_convert_function || '(' || part_defs_in(i).high_value || ', ' || part_defs_in(i).high_value_format || ')');
        ELSE
          dbms_lob.append(dest_lob => l_sql, src_lob => part_defs_in(i).high_value);
        END IF;
      
        -- Close high value brackets
        dbms_lob.append(dest_lob => l_sql, src_lob => ')');
        -- Add tablespace clause
        dbms_lob.append(dest_lob => l_sql, src_lob => ' TABLESPACE ' || part_defs_in(i).tablespace_name);
        n_last_partition_type_id := part_defs_in(i).partition_type_id;
      END LOOP;
    
      -- if the last partition definition was a subpartition then an additional closed bracket must be added.
      IF part_defs_in(part_defs_in.last).partition_type_id = partition_constants.c_par_type_subpartition_id THEN
        dbms_lob.append(dest_lob => l_sql, src_lob => ')');
      END IF;
    
      -- Close partition clause
      dbms_lob.append(dest_lob => l_sql, src_lob => ')');
    END IF; -- if REFERENCE
  
    RETURN l_sql;
  EXCEPTION
    WHEN e_no_foreign_key_found THEN
      log_admin.error('Could not find a foreign key on column ' || obj_def_in.partition_key || ' of table ' || obj_def_in.label_name,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action);
      RAISE e_create_sql_not_create;
    WHEN e_object_not_found_in_schema THEN
      log_admin.error('No access to ' || obj_def_in.object_type || ' ' || obj_def_in.object_owner || '.' || obj_def_in.object_name || chr(13) ||
                      dbms_utility.format_error_backtrace,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE e_create_sql_not_create;
    WHEN OTHERS THEN
      log_admin.critical(SQLERRM, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action, sql_errm_in => SQLERRM, sql_code_in => SQLCODE);
      RAISE;
  END get_object_ddl;

  /******************************************************************************/
  /******************************************************************************/
  -- Drop the TMP$$_* constraints created by Oracle
  PROCEDURE drop_tmp_constraints(owner_in IN dba_constraints.owner%TYPE, table_name_in IN dba_constraints.table_name%TYPE) IS
    v_module_action log_admin.t_module_action := 'drop_tmp_constraints';
  BEGIN
    log_admin.debug('TABLE "' || owner_in || '"."' || table_name_in || '": Dropping constraints named TMP$$*',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    FOR i IN (SELECT constraint_name
                FROM dba_constraints
               WHERE (owner, table_name, r_owner, r_constraint_name) IN (SELECT owner, table_name, r_owner, r_constraint_name
                                                                           FROM (SELECT fk.owner, fk.table_name, fk.r_owner, fk.r_constraint_name, COUNT(1)
                                                                                   FROM dba_constraints fk
                                                                                  WHERE fk.constraint_type = 'R'
                                                                                    AND fk.owner = owner_in
                                                                                    AND fk.table_name = table_name_in
                                                                                  GROUP BY fk.owner, fk.table_name, fk.r_owner, fk.r_constraint_name
                                                                                 HAVING COUNT(1) > 1))
                 AND constraint_name LIKE 'TMP$$%') LOOP
      object_admin.drop_constraint(owner_in => owner_in, table_name_in => table_name_in, constraint_name_in => i.constraint_name);
    END LOOP;
    log_admin.debug('TABLE "' || owner_in || '"."' || table_name_in || '": Constraints dropped',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
      RAISE;
  END drop_tmp_constraints;

  /******************************************************************************/
  /* Drop foreign key constraints which points to a table
  /******************************************************************************/
  PROCEDURE drop_child_constraints(owner_in IN dba_constraints.owner%TYPE, table_name_in IN dba_constraints.table_name%TYPE) IS
    v_module_action log_admin.t_module_action := 'drop_child_constraints';
  BEGIN
    log_admin.debug('TABLE "' || owner_in || '"."' || table_name_in || '": Dropping referential constraints pointing to this table',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    FOR i IN (SELECT fk.owner, fk.table_name, fk.constraint_name
                FROM dba_constraints fk
               WHERE fk.constraint_type = 'R'
                 AND (fk.r_owner, fk.r_constraint_name) IN (SELECT pk.owner, pk.constraint_name
                                                              FROM dba_constraints pk
                                                             WHERE pk.owner = owner_in
                                                               AND pk.table_name = table_name_in
                                                               AND pk.constraint_type IN ('U', 'P'))) LOOP
      object_admin.drop_constraint(owner_in => i.owner, table_name_in => i.table_name, constraint_name_in => i.constraint_name);
    END LOOP;
    log_admin.debug('TABLE "' || owner_in || '"."' || table_name_in || '": Referential constraints pointing to this table dropped',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
      RAISE;
  END drop_child_constraints;

  /******************************************************************************/
  /* Enable all foreign keys pointing to a table
  /******************************************************************************/
  PROCEDURE validate_foreign_keys_to_table(owner_in IN dba_constraints.owner%TYPE, table_name_in IN dba_constraints.table_name%TYPE) IS
    v_module_action log_admin.t_module_action := 'validate_foreign_keys_to_table';
  BEGIN
    log_admin.debug('TABLE "' || owner_in || '"."' || table_name_in || '": Validating all foreign keys pointing to this table',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    FOR i IN (SELECT fk.owner, fk.table_name, fk.constraint_name
                FROM dba_constraints fk
                JOIN dba_constraints pk
                  ON (pk.owner = fk.r_owner AND pk.constraint_name = fk.r_constraint_name)
               WHERE fk.constraint_type = 'R'
                 AND pk.owner = owner_in
                 AND pk.table_name = table_name_in
                 AND fk.validated = 'NOT VALIDATED') LOOP
      object_admin.enable_and_validate_constraint(owner_in => i.owner, table_name_in => i.table_name, constraint_name_in => i.constraint_name);
    END LOOP;
    log_admin.debug('TABLE "' || owner_in || '"."' || table_name_in || '": All foreign keys pointing to this table validated',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical(SQLERRM || chr(13) || dbms_utility.format_error_backtrace,
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
      RAISE;
  END validate_foreign_keys_to_table;

  /*******************************************************************************
  *******************************************************************************/
  PROCEDURE get_constraint_ddls(owner_in      IN dba_constraints.owner%TYPE,
                                table_name_in IN dba_constraints.table_name%TYPE,
                                ddls_out      OUT NOCOPY t_table_constraints) IS
  BEGIN
    OPEN c_table_constraints(owner_in => owner_in, table_name_in => table_name_in);
    FETCH c_table_constraints BULK COLLECT
      INTO ddls_out;
    CLOSE c_table_constraints;
  EXCEPTION
    WHEN OTHERS THEN
      IF c_table_constraints%ISOPEN THEN
        CLOSE c_table_constraints;
      END IF;
      log_admin.critical(SQLERRM || chr(13) || dbms_utility.format_error_backtrace,
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => 'get_constraint_ddls',
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
      RAISE;
  END get_constraint_ddls;

  /*******************************************************************************
  * Return a table with the index names and DDL to create the indexes of a table
  *******************************************************************************/
  PROCEDURE get_index_ddls(owner_in IN dba_indexes.owner%TYPE, table_name_in IN dba_indexes.table_name%TYPE, ddls_out OUT NOCOPY t_table_index_ddls) IS
  BEGIN
    OPEN c_table_indexes_ddl(owner_in => owner_in, table_name_in => table_name_in);
    FETCH c_table_indexes_ddl BULK COLLECT
      INTO ddls_out;
    CLOSE c_table_indexes_ddl;
  EXCEPTION
    WHEN OTHERS THEN
      IF c_table_indexes_ddl%ISOPEN THEN
        CLOSE c_table_indexes_ddl;
      END IF;
      log_admin.critical(SQLERRM || chr(13) || dbms_utility.format_error_backtrace,
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => 'get_constraint_ddls',
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
      RAISE;
  END get_index_ddls;

  /*******************************************************************************
  *******************************************************************************/
  PROCEDURE redefine_table(owner_in              IN dba_tables.owner%TYPE,
                           table_name_in         IN dba_tables.table_name%TYPE,
                           interim_table_name_in IN dba_tables.table_name%TYPE,
                           option_in             IN BINARY_INTEGER) IS
    v_module_action log_admin.t_module_action := 'redefine_table';
    num_errors      PLS_INTEGER;
  
    PROCEDURE cleanup IS
    BEGIN
      dbms_redefinition.abort_redef_table(uname => owner_in, orig_table => table_name_in, int_table => interim_table_name_in);
      -- Drop the interims tables
      sql_admin.execute_sql('DROP TABLE "' || owner_in || '"."' || interim_table_name_in || '" PURGE');
    EXCEPTION
      WHEN OTHERS THEN
        log_admin.error('Error during cleanup of table redefinition',
                        module_name_in                              => $$PLSQL_UNIT,
                        module_action_in                            => 'get_constraint_ddls',
                        sql_errm_in                                 => SQLERRM,
                        sql_code_in                                 => SQLCODE);
        --RAISE;
    END cleanup;
  
  BEGIN
    log_admin.debug('"' || owner_in || '"."' || table_name_in || '": Start dbms_redefinition',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    dbms_redefinition.start_redef_table(uname => owner_in, orig_table => table_name_in, int_table => interim_table_name_in, options_flag => option_in);
    dbms_redefinition.sync_interim_table(uname => owner_in, orig_table => table_name_in, int_table => interim_table_name_in);
    dbms_redefinition.copy_table_dependents(uname            => owner_in,
                                            orig_table       => table_name_in,
                                            int_table        => interim_table_name_in,
                                            copy_indexes     => 0,
                                            copy_triggers    => TRUE,
                                            copy_constraints => TRUE,
                                            copy_privileges  => TRUE,
                                            ignore_errors    => TRUE,
                                            num_errors       => num_errors,
                                            copy_statistics  => FALSE,
                                            copy_mvlog       => TRUE);
    dbms_redefinition.sync_interim_table(uname => owner_in, orig_table => table_name_in, int_table => interim_table_name_in);
    dbms_redefinition.finish_redef_table(uname => owner_in, orig_table => table_name_in, int_table => interim_table_name_in);
    log_admin.debug('"' || owner_in || '"."' || table_name_in || '": End dbms_redefinition',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  EXCEPTION
    WHEN e_resource_busy THEN
      log_admin.error('Resource busy during redefinition. Try again with less load.',
                      module_name_in                                                => $$PLSQL_UNIT,
                      module_action_in                                              => v_module_action,
                      sql_code_in                                                   => SQLCODE,
                      sql_errm_in                                                   => SQLERRM);
      cleanup;
      RAISE e_table_could_not_be_redefined;
    WHEN e_no_privilege_on_tablespace THEN
      cleanup;
      log_admin.error('Redefinition not possible: No privileges on tablespace',
                      module_name_in                                          => $$PLSQL_UNIT,
                      module_action_in                                        => v_module_action,
                      sql_code_in                                             => SQLCODE,
                      sql_errm_in                                             => SQLERRM);
      RAISE e_table_could_not_be_redefined;
    WHEN OTHERS THEN
      cleanup;
      log_admin.critical(SQLERRM || chr(13) || dbms_utility.format_error_backtrace,
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END redefine_table;

  /******************************************************************************
  * Check if the table can be redefined. If the table a primary key it will be
  * checked that the redefining with primary key is possible, else the check will
  * be done with the ROWID.
  * Return value is the option which can be used for the redefinition or 0.
  ******************************************************************************/
  FUNCTION table_can_be_redefined(owner_in IN dba_tables.owner%TYPE, table_name_in IN dba_tables.table_name%TYPE) RETURN BINARY_INTEGER IS
    v_module_action log_admin.t_module_action := 'table_can_be_redefined';
    v_pk_name       dba_constraints.constraint_name%TYPE;
    n_option        BINARY_INTEGER;
  BEGIN
    log_admin.debug('"' || owner_in || '"."' || table_name_in || '": Checking that table can be redefined',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    -- Get the name of the primary key of that table
    v_pk_name := object_admin.get_table_primary_key_name(owner_in => owner_in, table_name_in => table_name_in);
    -- Set option CONS_USE_PK if a primary key exist, else CONS_USE_ROWID
    IF v_pk_name IS NULL THEN
      n_option := dbms_redefinition.cons_use_rowid;
    ELSE
      n_option := dbms_redefinition.cons_use_pk;
    END IF;
    -- Check that the table can be redefined
    dbms_redefinition.can_redef_table(uname => owner_in, tname => table_name_in, options_flag => n_option);
    log_admin.debug('"' || owner_in || '"."' || table_name_in || '": Can be redefined with option ' || n_option,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  
    RETURN n_option;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.error('"' || owner_in || '.' || table_name_in || '": Redefinition not possible with option ' || n_option,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      
                      sql_code_in => SQLCODE,
                      sql_errm_in => SQLERRM);
      RETURN 0;
  END table_can_be_redefined;

  /******************************************************************************/
  /* Rename constraints from temporary name to origin name
  /******************************************************************************/
  PROCEDURE rename_constraints(obj_def_in IN partition_object%ROWTYPE, constraints_in IN t_table_constraints) IS
    v_module_action log_admin.t_module_action := 'rename_constraints';
  BEGIN
    log_admin.debug('Renaming constraints to origin name', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    IF constraints_in.count > 0 THEN
      FOR i IN constraints_in.first .. constraints_in.last LOOP
        BEGIN
          object_admin.rename_constraint(owner_in               => constraints_in(i).owner,
                                         table_name_in          => obj_def_in.object_name,
                                         old_constraint_name_in => constraints_in(i).tmp_constraint_name,
                                         new_constraint_name_in => constraints_in(i).constraint_name);
        EXCEPTION
          WHEN e_cons_name_already_used THEN
            -- No clue why this can happen. The SQL's are not changing this. Maybe this is an Oracle Bug!
            log_admin.warning('Constraint ' || constraints_in(i).tmp_constraint_name || ' could not be renamed to ' || constraints_in(i).constraint_name ||
                              ', try to drop it.',
                              module_name_in => $$PLSQL_UNIT,
                              module_action_in => v_module_action,
                              sql_errm_in => SQLERRM,
                              sql_code_in => SQLCODE);
            object_admin.drop_constraint(owner_in           => constraints_in(i).owner,
                                         table_name_in      => obj_def_in.object_name,
                                         constraint_name_in => constraints_in(i).constraint_name);
        END;
        -- Check if there was an index affected by the constraint rename. This will occure for indexes created by a constraint definition (unique, primary key)
        IF object_admin.is_object_existing(owner_in       => obj_def_in.object_owner,
                                           object_type_in => 'INDEX',
                                           object_name_in => constraints_in(i).tmp_constraint_name) = 1 THEN
          -- Rename the index to it's original name
          object_admin.rename_index(owner_in          => constraints_in(i).owner,
                                    old_index_name_in => constraints_in(i).tmp_constraint_name,
                                    new_index_name_in => constraints_in(i).constraint_name);
        END IF;
      END LOOP;
    END IF;
    log_admin.debug('Renamed constraints to origin name', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace,
                         module_name_in                      => $$PLSQL_UNIT,
                         module_action_in                    => v_module_action,
                         sql_errm_in                         => SQLERRM,
                         sql_code_in                         => SQLCODE);
      RAISE;
  END rename_constraints;

  /******************************************************************************
  * Create all indexes of the origin table on the interim table
  *
  * TODO: Check if partition key is part of an composite index and add LOCAL.
  ******************************************************************************/
  PROCEDURE create_indexes_on_int_table(obj_def_in        IN partition_object%ROWTYPE,
                                        int_table_name_in IN partition_types.short_varchar,
                                        index_tab_in      IN OUT NOCOPY t_table_index_ddls) IS
    v_module_action partition_types.short_varchar := 'create_indexes';
  BEGIN
    log_admin.debug('Creating indexes on interim table', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    IF index_tab_in.count > 0 THEN
      FOR i IN index_tab_in.first .. index_tab_in.last LOOP
        -- Replace the origin table name with the temporary one
        index_tab_in(i).create_ddl := REPLACE(srcstr => index_tab_in(i).create_ddl,
                                              oldsub => '"' || obj_def_in.object_owner || '"."' || obj_def_in.object_name || '"',
                                              newsub => '"' || obj_def_in.object_owner || '"."' || int_table_name_in || '"');
        -- Replace the origin index name with the temporary one
        index_tab_in(i).create_ddl := REPLACE(srcstr => index_tab_in(i).create_ddl,
                                              oldsub => '"' || index_tab_in(i).index_name || '"',
                                              newsub => '"' || index_tab_in(i).tmp_index_name || '"');
        -- Add LOCAL keyword to create indexes partitioned. 
        IF -- Do this only if index is NONUNIQUE 
         ((index_tab_in(i).uniqueness = 'NONUNIQUE')
         -- or when the partition key is part of the index
         --OR ((index_tab_in(i).uniqueness = 'UNIQUE') AND
         --(dbms_lob.instr(lob_loc => index_tab_in(i).columns,
         --pattern => part_defs_in(1).partition_key) != 0))*/
         ) THEN
          dbms_lob.append(dest_lob => index_tab_in(i).create_ddl, src_lob => ' LOCAL');
        END IF;
        -- Replace the origin index name with the temporary one and create the index
        sql_admin.execute_sql(sql_in => index_tab_in(i).create_ddl);
      END LOOP;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical(dbms_utility.format_error_backtrace,
                         module_name_in                      => $$PLSQL_UNIT,
                         module_action_in                    => v_module_action,
                         sql_errm_in                         => SQLERRM,
                         sql_code_in                         => SQLCODE);
      RAISE;
  END create_indexes_on_int_table;

  /******************************************************************************/
  /* Redefine object online */
  /******************************************************************************/
  PROCEDURE redefine_object(obj_def_in IN partition_object%ROWTYPE, part_defs_in IN partition_types.t_partition_def_table) IS
    v_module_action log_admin.t_module_action := 'redefine_object';
    l_sql           CLOB;
    v_tmp_obj_name  VARCHAR2(30 CHAR) := 'TMP_' || obj_def_in.object_id;
    l_constraints   t_table_constraints;
    l_index_ddls    t_table_index_ddls;
    n_redef_option  BINARY_INTEGER;
  BEGIN
    log_admin.debug(obj_def_in.object_type || ' ' || obj_def_in.label_name,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action,
                    autonom_in => 1);
    -- Check that there is no other table named like the interim table
    IF object_admin.is_object_existing(owner_in => obj_def_in.object_owner, object_type_in => obj_def_in.object_type, object_name_in => v_tmp_obj_name) = 1 THEN
      RAISE e_object_does_already_exist;
    END IF;
    -- Check if redefinition is possible and which option to use
    n_redef_option := table_can_be_redefined(owner_in => obj_def_in.object_owner, table_name_in => obj_def_in.object_name);
    IF n_redef_option = 0 THEN
      RAISE e_redefinition_impossible;
    END IF;
    -- Get the SQL to create the interims table
    l_sql := get_object_ddl(obj_def_in => obj_def_in, part_defs_in => part_defs_in);
    -- Replace real object name with the one used temporary
    l_sql := REPLACE(srcstr => l_sql,
                     oldsub => obj_def_in.label_name,
                     newsub => obj_def_in.object_type || ' ' || obj_def_in.object_owner || '.' || v_tmp_obj_name);
    -- Replace SEGMENT CREATION IMMEDIATE with SEGMENT CREATION DEFFERED
    l_sql := REPLACE(srcstr => l_sql, oldsub => 'SEGMENT CREATION IMMEDIATE', newsub => 'SEGMENT CREATION DEFERRED');
    -- Get the SQL's to create the constraints on that table
    get_constraint_ddls(owner_in => obj_def_in.object_owner, table_name_in => obj_def_in.object_name, ddls_out => l_constraints);
    -- Create constraints on the interim table
    IF l_constraints.count > 0 THEN
      FOR i IN l_constraints.first .. l_constraints.last LOOP
        -- Replace the origin constraint name with the temporary one
        l_sql := REPLACE(srcstr => l_sql,
                         oldsub => '"' || l_constraints(i).constraint_name || '"',
                         newsub => '"' || l_constraints(i).tmp_constraint_name || '"');
        -- Replace the origin table name with the interim table name
        l_sql := REPLACE(srcstr => l_sql, oldsub => obj_def_in.label_name, newsub => obj_def_in.object_owner || '.' || v_tmp_obj_name);
      END LOOP;
    END IF;
    -- Create the interims table
    sql_admin.execute_sql(sql_in => l_sql);
  
    -- Get the SQL's to create the indexes on that table
    get_index_ddls(owner_in => obj_def_in.object_owner, table_name_in => obj_def_in.object_name, ddls_out => l_index_ddls);
    -- Create indexes on the interim table
    create_indexes_on_int_table(obj_def_in => obj_def_in, int_table_name_in => v_tmp_obj_name, index_tab_in => l_index_ddls);
    -- Redefine the existing object with the help of the interim table
    IF obj_def_in.object_type = partition_constants.c_obj_type_table THEN
      -- Export statistics 
      -- ! Executing user must have privileges to export and import statistics. Therefore i will disable this feature first and find a solution how to handle this.
      --IF object_admin.is_object_existing(owner_in => USER, object_type_in => 'TABLE', object_name_in => gc_stattab_name) = 0 THEN
      --dbms_stats.create_stat_table(ownname => USER, stattab => gc_stattab_name);
      --END IF;
      --dbms_stats.export_table_stats(ownname => obj_def_in.object_owner, tabname => obj_def_in.object_name, statown => USER, stattab => gc_stattab_name);
      -- Redefine the table
      redefine_table(owner_in              => obj_def_in.object_owner,
                     table_name_in         => obj_def_in.object_name,
                     interim_table_name_in => v_tmp_obj_name,
                     option_in             => n_redef_option);
      -- Import statistics
      --dbms_stats.import_table_stats(ownname => obj_def_in.object_owner, tabname => obj_def_in.object_name, statown => USER, stattab => gc_stattab_name);
    
      -- Drop the TMP$$_* constraints from child tables which still point to the TMP_ table
      drop_child_constraints(owner_in => obj_def_in.object_owner, table_name_in => v_tmp_obj_name);
      -- Drop the TMP$$_* constraints created by Oracle where the original constraint exist too
      drop_tmp_constraints(owner_in => obj_def_in.object_owner, table_name_in => obj_def_in.object_name);
    
      -- Enable and validate constraints
      object_admin.validate_table_constraints(owner_in => obj_def_in.object_owner, table_name_in => obj_def_in.object_name);
      -- Validate foreign keys to the table
      validate_foreign_keys_to_table(owner_in => obj_def_in.object_owner, table_name_in => obj_def_in.object_name);
      -- Compile invalid objects
      object_admin.compile_invalid_objects(owner_in => obj_def_in.object_owner);
      -- Drop trigger on interim table. They are causing ORA-04027
      object_admin.drop_table_triggers(owner_in => obj_def_in.object_owner, table_name_in => v_tmp_obj_name);
      -- Drop the interim table
      object_admin.drop_object(owner_in => obj_def_in.object_owner, object_name_in => v_tmp_obj_name, object_type_in => obj_def_in.object_type);
      -- Rename the constraints to the original name
      rename_constraints(obj_def_in => obj_def_in, constraints_in => l_constraints);
      -- Rename the indexes to the origin name
      IF l_index_ddls.count > 0 THEN
        FOR i IN l_index_ddls.first .. l_index_ddls.last LOOP
          -- Check if an index with the temporary name does exist. It can be that throuh the constraint renaming the index is also renamed.
          IF object_admin.is_object_existing(owner_in => l_index_ddls(i).owner, object_type_in => 'INDEX', object_name_in => l_index_ddls(i).tmp_index_name) = 0 THEN
            continue;
          END IF;
          object_admin.rename_index(owner_in          => l_index_ddls(i).owner,
                                    old_index_name_in => l_index_ddls(i).tmp_index_name,
                                    new_index_name_in => l_index_ddls(i).index_name);
        END LOOP;
      END IF;
    END IF;
  
    log_admin.info(obj_def_in.object_type || ' ' || obj_def_in.label_name || ' redefined', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  EXCEPTION
    WHEN e_incomplete_part_bound_date THEN
      log_admin.error('Redefinition not possible: Configuration wrong', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    WHEN e_no_privilege_on_tablespace THEN
      log_admin.error('Redefinition not possible: no privilege on tablespace', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    WHEN e_object_does_already_exist THEN
      log_admin.error('Redefinition not possible: Table ' || obj_def_in.object_owner || '.' || v_tmp_obj_name ||
                      ' does already exist. This table has to be used as interims table.',
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action);
    WHEN e_partition_bound_element_null THEN
      log_admin.error('Redefinition not possible: Configuration error detected for ' || obj_def_in.object_type || ' ' || obj_def_in.label_name,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action);
    WHEN e_create_sql_not_create THEN
      log_admin.error('Redefinition not possible: SQL statement to create interim table could not be created',
                      module_name_in                                                                         => $$PLSQL_UNIT,
                      module_action_in                                                                       => v_module_action);
    WHEN e_parent_table_not_partitioned THEN
      log_admin.error('Redefinition not possible: parent table not partitioned', module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    WHEN e_ref_partition_fk_not_support THEN
      log_admin.error('Redefinition not possible: reference partitioning foreign key is not supported',
                      module_name_in                                                                  => $$PLSQL_UNIT,
                      module_action_in                                                                => v_module_action,
                      sql_code_in                                                                     => SQLCODE,
                      sql_errm_in                                                                     => SQLERRM);
    WHEN e_table_referenced_by_fk THEN
      log_admin.error('Could not drop interim table. Table is referenced by foreign key from another table',
                      module_name_in                                                                       => $$PLSQL_UNIT,
                      module_action_in                                                                     => v_module_action,
                      sql_code_in                                                                          => SQLCODE,
                      sql_errm_in                                                                          => SQLERRM);
    WHEN e_redefinition_impossible THEN
      log_admin.error('Table can not be redefined. Neither by PK nor by ROWID',
                      module_name_in                                          => $$PLSQL_UNIT,
                      module_action_in                                        => v_module_action,
                      sql_code_in                                             => SQLCODE,
                      sql_errm_in                                             => SQLERRM);
    WHEN e_table_could_not_be_redefined THEN
      log_admin.error('Table could not be redefined. See last error message for details.',
                      module_name_in                                                     => $$PLSQL_UNIT,
                      module_action_in                                                   => v_module_action,
                      sql_code_in                                                        => SQLCODE,
                      sql_errm_in                                                        => SQLERRM);
    WHEN OTHERS THEN
      log_admin.critical(SQLERRM || chr(13) || dbms_utility.format_error_backtrace(),
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_code_in => SQLCODE,
                         sql_errm_in => SQLERRM);
      RAISE;
  END redefine_object;

  /******************************************************************************
  * Initialize
  ******************************************************************************/
  PROCEDURE initialize IS
    v_module_action log_admin.t_module_action := 'initialize';
  BEGIN
    log_admin.info('Initializing ' || gc_module_label, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    module_admin.load_and_set_config(module_name_in => $$PLSQL_UNIT);
  END initialize;

BEGIN
  initialize;
END partition_redefine;
/
