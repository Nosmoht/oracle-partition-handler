CREATE OR REPLACE PACKAGE ADMIN.partition_config IS
  /******************************************************************************
  * Partition Admin
  *
  * Author:    Thomas Krahn
  * Date:      2014-01-09
  * Version:   0.9.4
  *
  * Required:  module_admin v0.7.0
  ******************************************************************************/

  /******************************************************************************
  * Constants
  ******************************************************************************/
  gc_module_version CONSTANT module_admin.t_module_version := '0.9.4';
  gc_module_label   CONSTANT module_admin.t_module_label := $$PLSQL_UNIT || ' v' || gc_module_version;

  /******************************************************************************
  * add a new table to partition handler
  ******************************************************************************/
  PROCEDURE add_table(owner_in      IN p_obj.owner%TYPE,
                      table_name_in IN p_obj.name%TYPE,
                      enabled_in    IN p_obj.enabled%TYPE DEFAULT 0,
                      order#_in     IN p_obj.order#%TYPE DEFAULT NULL);

  /******************************************************************************/
  /* del a table
  /******************************************************************************/
  PROCEDURE del_table(owner_in IN p_obj.owner%TYPE, table_name_in IN p_obj.name%TYPE);

  /******************************************************************************/
  /* add a new table to partition handler
  /******************************************************************************/
  PROCEDURE add_index(owner_in      IN p_obj.owner%TYPE,
                      index_name_in IN p_obj.name%TYPE,
                      enabled_in    IN p_obj.enabled%TYPE DEFAULT 0,
                      order#_in     IN p_obj.order#%TYPE DEFAULT NULL);

  /******************************************************************************/
  /* del a table
  /******************************************************************************/
  PROCEDURE del_index(owner_in IN p_obj.owner%TYPE, index_name_in IN p_obj.name%TYPE);

  /******************************************************************************/
  /* Return the ID of a registred table
  /******************************************************************************/
  FUNCTION get_table_id(owner_in IN p_obj.owner%TYPE, table_name_in IN p_obj.name%TYPE) RETURN p_obj.id%TYPE;

  /******************************************************************************/
  /* Return the ID of a registred table
  /******************************************************************************/
  FUNCTION get_index_id(owner_in IN p_obj.owner%TYPE, index_name_in IN p_obj.name%TYPE) RETURN p_obj.id%TYPE;

  /******************************************************************************/
  /* Enable partition handling for a table
  /******************************************************************************/
  PROCEDURE enable_table(owner_in IN p_obj.owner%TYPE, table_name_in IN p_obj.name%TYPE);

  /******************************************************************************
  * Disable partition handling for a table
  ******************************************************************************/
  PROCEDURE disable_table(owner_in IN p_obj.owner%TYPE, table_name_in IN p_obj.name%TYPE);

  /******************************************************************************/
  /* Enable partition handling for an index
  /******************************************************************************/
  PROCEDURE enable_index(owner_in IN p_obj.owner%TYPE, index_name_in IN p_obj.name%TYPE);

  /******************************************************************************/
  /* Disable partition handling for an index
  /******************************************************************************/
  PROCEDURE disable_index(owner_in IN p_obj.owner%TYPE, index_name_in IN p_obj.name%TYPE);

  /******************************************************************************/
  /* add partition key for a table
  /******************************************************************************/
  PROCEDURE add_table_partition_key(owner_in        IN p_obj.owner%TYPE,
                                    table_name_in   IN p_obj.name%TYPE,
                                    column_name_in  IN p_key.column_name%TYPE,
                                    technique_id_in IN p_key.p_tech_id%TYPE,
                                    enabled_in      IN p_key.enabled%TYPE DEFAULT partition_constants.c_enabled);

  /******************************************************************************/
  /* add subpartition key for a table
  /******************************************************************************/
  PROCEDURE add_table_subpartition_key(owner_in        IN p_obj.owner%TYPE,
                                       table_name_in   IN p_obj.name%TYPE,
                                       column_name_in  IN p_key.column_name%TYPE,
                                       technique_id_in IN p_key.p_tech_id%TYPE,
                                       enabled_in      IN p_key.enabled%TYPE DEFAULT partition_constants.c_enabled);

  /******************************************************************************/
  /* add partition key for an index
  /******************************************************************************/
  PROCEDURE add_index_partition_key(owner_in        IN p_obj.owner%TYPE,
                                    index_name_in   IN p_obj.name%TYPE,
                                    column_name_in  IN p_key.column_name%TYPE,
                                    technique_id_in IN p_key.p_tech_id%TYPE,
                                    enabled_in      IN p_key.enabled%TYPE DEFAULT partition_constants.c_enabled);

  /******************************************************************************/
  /* add subpartition key for a table
  /******************************************************************************/
  PROCEDURE add_index_subpartition_key(owner_in        IN p_obj.owner%TYPE,
                                       index_name_in   IN p_obj.name%TYPE,
                                       column_name_in  IN p_key.column_name%TYPE,
                                       technique_id_in IN p_key.p_tech_id%TYPE,
                                       enabled_in      IN p_key.enabled%TYPE DEFAULT partition_constants.c_enabled);

  /******************************************************************************/
  /* add partition definition
  /******************************************************************************/
  PROCEDURE add_table_partition(owner_in                  IN p_obj.owner%TYPE,
                                table_name_in             IN p_obj.name%TYPE,
                                high_value_in             IN p_def.high_value%TYPE,
                                p_name_in                 IN p_def.p_name%TYPE,
                                tbs_name_in               IN p_def.tbs_name%TYPE,
                                enabled_in                IN p_def.enabled%TYPE DEFAULT 1,
                                partition#_in             IN p_def.partition#%TYPE DEFAULT NULL,
                                high_value_format_in      IN p_def.high_value_format%TYPE DEFAULT NULL,
                                p_name_prefix_in          IN p_def.p_name_prefix%TYPE DEFAULT NULL,
                                p_name_suffix_in          IN p_def.p_name_suffix%TYPE DEFAULT NULL,
                                p_naming_function_in      IN p_def.p_naming_function%TYPE DEFAULT NULL,
                                tbs_name_prefix_in        IN p_def.tbs_name_prefix%TYPE DEFAULT NULL,
                                tbs_name_suffix_in        IN p_def.tbs_name_suffix%TYPE DEFAULT NULL,
                                tbs_naming_function_in    IN p_def.tbs_naming_function%TYPE DEFAULT NULL,
                                auto_adjust_enabled_in    IN p_def.auto_adjust_enabled%TYPE DEFAULT 0,
                                auto_adjust_function_in   IN p_def.auto_adjust_function%TYPE DEFAULT NULL,
                                auto_adjust_value_in      IN p_def.auto_adjust_value%TYPE DEFAULT NULL,
                                auto_adjust_end_in        IN p_def.auto_adjust_end%TYPE DEFAULT 0,
                                auto_adjust_start_in      IN p_def.auto_adjust_start%TYPE DEFAULT 0,
                                moving_window_in          IN p_def.moving_window%TYPE DEFAULT 0,
                                moving_window_function_in IN p_def.moving_window_function%TYPE DEFAULT NULL);

  /******************************************************************************/
  /* add subpartition definition
  /******************************************************************************/
  PROCEDURE add_table_subpartition(owner_in                  IN p_obj.owner%TYPE,
                                   table_name_in             IN p_obj.name%TYPE,
                                   partition#_in             IN p_def.partition#%TYPE,
                                   high_value_in             IN p_def.high_value%TYPE,
                                   p_name_in                 IN p_def.p_name%TYPE,
                                   tbs_name_in               IN p_def.tbs_name%TYPE,
                                   enabled_in                IN p_def.enabled%TYPE DEFAULT 1,
                                   subpartition#_in          IN p_def.subpartition#%TYPE DEFAULT NULL,
                                   high_value_format_in      IN p_def.high_value_format%TYPE DEFAULT NULL,
                                   p_name_prefix_in          IN p_def.p_name_prefix%TYPE DEFAULT NULL,
                                   p_name_suffix_in          IN p_def.p_name_suffix%TYPE DEFAULT NULL,
                                   p_naming_function_in      IN p_def.p_naming_function%TYPE DEFAULT NULL,
                                   tbs_name_prefix_in        IN p_def.tbs_name_prefix%TYPE DEFAULT NULL,
                                   tbs_name_suffix_in        IN p_def.tbs_name_suffix%TYPE DEFAULT NULL,
                                   tbs_naming_function_in    IN p_def.tbs_naming_function%TYPE DEFAULT NULL,
                                   auto_adjust_enabled_in    IN p_def.auto_adjust_enabled%TYPE DEFAULT 0,
                                   auto_adjust_function_in   IN p_def.auto_adjust_function%TYPE DEFAULT NULL,
                                   auto_adjust_value_in      IN p_def.auto_adjust_value%TYPE DEFAULT NULL,
                                   auto_adjust_end_in        IN p_def.auto_adjust_end%TYPE DEFAULT 0,
                                   auto_adjust_start_in      IN p_def.auto_adjust_start%TYPE DEFAULT 0,
                                   moving_window_in          IN p_def.moving_window%TYPE DEFAULT 0,
                                   moving_window_function_in IN p_def.moving_window_function%TYPE DEFAULT NULL);

  /******************************************************************************/
  /* add index partition definition
  /******************************************************************************/
  PROCEDURE add_index_partition(owner_in                  IN p_obj.owner%TYPE,
                                index_name_in             IN p_obj.name%TYPE,
                                high_value_in             IN p_def.high_value%TYPE,
                                p_name_in                 IN p_def.p_name%TYPE,
                                tbs_name_in               IN p_def.tbs_name%TYPE,
                                enabled_in                IN p_def.enabled%TYPE DEFAULT 1,
                                partition#_in             IN p_def.partition#%TYPE DEFAULT NULL,
                                high_value_format_in      IN p_def.high_value_format%TYPE DEFAULT NULL,
                                p_name_prefix_in          IN p_def.p_name_prefix%TYPE DEFAULT NULL,
                                p_name_suffix_in          IN p_def.p_name_suffix%TYPE DEFAULT NULL,
                                p_naming_function_in      IN p_def.p_naming_function%TYPE DEFAULT NULL,
                                tbs_name_prefix_in        IN p_def.tbs_name_prefix%TYPE DEFAULT NULL,
                                tbs_name_suffix_in        IN p_def.tbs_name_suffix%TYPE DEFAULT NULL,
                                tbs_naming_function_in    IN p_def.tbs_naming_function%TYPE DEFAULT NULL,
                                auto_adjust_enabled_in    IN p_def.auto_adjust_enabled%TYPE DEFAULT 0,
                                auto_adjust_function_in   IN p_def.auto_adjust_function%TYPE DEFAULT NULL,
                                auto_adjust_value_in      IN p_def.auto_adjust_value%TYPE DEFAULT NULL,
                                auto_adjust_end_in        IN p_def.auto_adjust_end%TYPE DEFAULT 0,
                                auto_adjust_start_in      IN p_def.auto_adjust_start%TYPE DEFAULT 0,
                                moving_window_in          IN p_def.moving_window%TYPE DEFAULT 0,
                                moving_window_function_in IN p_def.moving_window_function%TYPE DEFAULT NULL);

  /******************************************************************************/
  /* add index subpartition definition
  /******************************************************************************/
  PROCEDURE add_index_subpartition(owner_in                  IN p_obj.owner%TYPE,
                                   index_name_in             IN p_obj.name%TYPE,
                                   partition#_in             IN p_def.partition#%TYPE,
                                   high_value_in             IN p_def.high_value%TYPE,
                                   p_name_in                 IN p_def.p_name%TYPE,
                                   tbs_name_in               IN p_def.tbs_name%TYPE,
                                   enabled_in                IN p_def.enabled%TYPE DEFAULT 1,
                                   subpartition#_in          IN p_def.subpartition#%TYPE DEFAULT NULL,
                                   high_value_format_in      IN p_def.high_value_format%TYPE DEFAULT NULL,
                                   p_name_prefix_in          IN p_def.p_name_prefix%TYPE DEFAULT NULL,
                                   p_name_suffix_in          IN p_def.p_name_suffix%TYPE DEFAULT NULL,
                                   p_naming_function_in      IN p_def.p_naming_function%TYPE DEFAULT NULL,
                                   tbs_name_prefix_in        IN p_def.tbs_name_prefix%TYPE DEFAULT NULL,
                                   tbs_name_suffix_in        IN p_def.tbs_name_suffix%TYPE DEFAULT NULL,
                                   tbs_naming_function_in    IN p_def.tbs_naming_function%TYPE DEFAULT NULL,
                                   auto_adjust_enabled_in    IN p_def.auto_adjust_enabled%TYPE DEFAULT 0,
                                   auto_adjust_function_in   IN p_def.auto_adjust_function%TYPE DEFAULT NULL,
                                   auto_adjust_value_in      IN p_def.auto_adjust_value%TYPE DEFAULT NULL,
                                   auto_adjust_end_in        IN p_def.auto_adjust_end%TYPE DEFAULT 0,
                                   auto_adjust_start_in      IN p_def.auto_adjust_start%TYPE DEFAULT 0,
                                   moving_window_in          IN p_def.moving_window%TYPE DEFAULT 0,
                                   moving_window_function_in IN p_def.moving_window_function%TYPE DEFAULT NULL);

  /******************************************************************************/
  /* Update the high value of a definition
  /******************************************************************************/
  PROCEDURE update_high_value(obj_id_in        IN p_def.p_obj_id%TYPE,
                              partition#_in    IN p_def.partition#%TYPE,
                              high_value_in    IN p_def.high_value%TYPE,
                              subpartition#_in IN p_def.subpartition#%TYPE DEFAULT 0);

  /******************************************************************************/
  /* Update the high value of a table partition definition
  /******************************************************************************/
  PROCEDURE update_table_high_value(owner_in         IN p_obj.owner%TYPE,
                                    table_name_in    IN p_obj.name%TYPE,
                                    partition#_in    IN p_def.partition#%TYPE,
                                    high_value_in    IN p_def.high_value%TYPE,
                                    subpartition#_in IN p_def.subpartition#%TYPE DEFAULT 0);

  /******************************************************************************/
  /* Update the high value of a table partition definition
  /******************************************************************************/
  PROCEDURE update_index_high_value(owner_in         IN p_obj.owner%TYPE,
                                    index_name_in    IN p_obj.name%TYPE,
                                    partition#_in    IN p_def.partition#%TYPE,
                                    high_value_in    IN p_def.high_value%TYPE,
                                    subpartition#_in IN p_def.subpartition#%TYPE DEFAULT 0);
END partition_config;
/
CREATE OR REPLACE PACKAGE BODY ADMIN.partition_config IS
  /*****************************************************************************
  * Exceptions
  *****************************************************************************/
  e_check_constraint_violated EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_check_constraint_violated, -02290); -- ORA-02290
  e_parent_key_not_found EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_parent_key_not_found, -02291); -- ORA-02291
  -- Exception -20000 - -20099 reserved for P_OBJ
  e_object_already_added EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_object_already_added, -20001);
  e_object_not_added EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_object_not_added, -20002);
  e_enabled_invalid EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_enabled_invalid, -20003);
  -- Exception -20100 - -20199 reserved for P_KEY
  -- Exception -20200 - -20299 reserved for P_DEF  
  e_p_naming_function_missing EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_p_naming_function_missing, -20200);

  /******************************************************************************/
  /* Add entry to P_OBJ
  /******************************************************************************/
  PROCEDURE add_object(p_otype_id_in IN p_obj.p_otype_id%TYPE,
                       owner_in      IN p_obj.owner%TYPE,
                       name_in       IN p_obj.name%TYPE,
                       enabled_in    IN p_obj.enabled%TYPE DEFAULT 0,
                       order#_in     IN p_obj.order#%TYPE DEFAULT NULL) IS
  BEGIN
    INSERT INTO p_obj
      (id, p_otype_id, owner, NAME, enabled, order#)
    VALUES
      (p_obj_id_seq.nextval, p_otype_id_in, upper(TRIM(owner_in)), upper(TRIM(name_in)), enabled_in, order#_in);
  EXCEPTION
    WHEN e_check_constraint_violated THEN
      IF instr(SQLERRM, 'P_OBJ_ID_NN') != 0 THEN
        raise_application_error(-20000, 'ID must not be NULL');
      ELSIF instr(SQLERRM, 'P_OBJ_ENABLED_NN') != 0 THEN
        raise_application_error(-20001, 'ENBALED must not be NULL');
      ELSIF instr(SQLERRM, 'P_OBJ_ENABLED_CK') != 0 THEN
        raise_application_error(-20002, 'ENABLED must be 0 or 1');
      ELSIF instr(SQLERRM, 'P_OBJ_OWNER_NN') != 0 THEN
        raise_application_error(-20003, 'OWNER must not be NULL');
      ELSIF instr(SQLERRM, 'P_OBJ_P_OTYPE_ID_NN') != 0 THEN
        raise_application_error(-20004, 'P_OTYPE_ID must not be NULL');
      ELSIF instr(SQLERRM, 'P_OBJ_NAME_NN') != 0 THEN
        raise_application_error(-20005, 'NAME must not be NULL');
      END IF;
    WHEN dup_val_on_index THEN
      IF instr(SQLERRM, 'P_OBJ_ID_UX') != 0 THEN
        raise_application_error(-20006, 'ID already used');
      ELSIF instr(SQLERRM, 'P_OBJ_P_OTYPE_ID_OWNER_NAME_UK') != 0 THEN
        raise_application_error(-20007, 'Object ' || owner_in || '.' || name_in || ' already added');
      END IF;
    WHEN OTHERS THEN
      RAISE;
  END add_object;

  /******************************************************************************/
  /* Delete entry from P_OBJ
  /******************************************************************************/
  PROCEDURE del_object(p_otype_id_in IN p_obj.p_otype_id%TYPE, owner_in IN p_obj.owner%TYPE, name_in IN p_obj.name%TYPE) IS
  BEGIN
    DELETE FROM p_obj pt
     WHERE pt.p_otype_id = p_otype_id_in
       AND pt.owner = upper(TRIM(owner_in))
       AND pt.name = upper(TRIM(name_in));
  END del_object;

  /******************************************************************************
  /* add a new table to partition handler
  /******************************************************************************/
  PROCEDURE add_table(owner_in      IN p_obj.owner%TYPE,
                      table_name_in IN p_obj.name%TYPE,
                      enabled_in    IN p_obj.enabled%TYPE DEFAULT 0,
                      order#_in     IN p_obj.order#%TYPE DEFAULT NULL) IS
  BEGIN
    add_object(p_otype_id_in => partition_constants.c_obj_type_table_id,
               owner_in      => owner_in,
               name_in       => table_name_in,
               enabled_in    => enabled_in,
               order#_in     => order#_in);
  END add_table;

  /******************************************************************************/
  /* Remove record from table p_obj
  /******************************************************************************/
  PROCEDURE del_table(owner_in IN p_obj.owner%TYPE, table_name_in IN p_obj.name%TYPE) IS
  BEGIN
    del_object(p_otype_id_in => partition_constants.c_obj_type_table_id, owner_in => owner_in, name_in => table_name_in);
  END del_table;

  /******************************************************************************
  /* add a new index to partition handler
  /******************************************************************************/
  PROCEDURE add_index(owner_in      IN p_obj.owner%TYPE,
                      index_name_in IN p_obj.name%TYPE,
                      enabled_in    IN p_obj.enabled%TYPE DEFAULT 0,
                      order#_in     IN p_obj.order#%TYPE DEFAULT NULL) IS
  BEGIN
    add_object(p_otype_id_in => partition_constants.c_obj_type_index_id,
               owner_in      => owner_in,
               name_in       => index_name_in,
               enabled_in    => enabled_in,
               order#_in     => order#_in);
  END add_index;

  /******************************************************************************/
  /* Deadd index from partition handler
  /******************************************************************************/
  PROCEDURE del_index(owner_in IN p_obj.owner%TYPE, index_name_in IN p_obj.name%TYPE) IS
  BEGIN
    del_object(p_otype_id_in => partition_constants.c_obj_type_index_id, owner_in => owner_in, name_in => index_name_in);
  END del_index;

  /******************************************************************************/
  /* Return the ID of a added object
  /******************************************************************************/
  FUNCTION get_object_id(p_otype_id_in IN p_obj.p_otype_id%TYPE, owner_in IN p_obj.owner%TYPE, name_in IN p_obj.name%TYPE) RETURN p_obj.id%TYPE IS
    n_result p_obj.id%TYPE;
  BEGIN
    SELECT o.id
      INTO n_result
      FROM p_obj o
     WHERE o.p_otype_id = p_otype_id_in
       AND o.owner = upper(TRIM(owner_in))
       AND o.name = upper(TRIM(name_in));
  
    RETURN n_result;
  EXCEPTION
    WHEN no_data_found THEN
      RAISE e_object_not_added;
    WHEN OTHERS THEN
      RAISE;
  END get_object_id;

  /******************************************************************************/
  /* Return the ID of a registred table
  /******************************************************************************/
  FUNCTION get_table_id(owner_in IN p_obj.owner%TYPE, table_name_in IN p_obj.name%TYPE) RETURN p_obj.id%TYPE IS
  BEGIN
    RETURN get_object_id(p_otype_id_in => partition_constants.c_obj_type_table_id, owner_in => owner_in, name_in => table_name_in);
  END get_table_id;

  /******************************************************************************/
  /* Return the ID of a registred index
  /******************************************************************************/
  FUNCTION get_index_id(owner_in IN p_obj.owner%TYPE, index_name_in IN p_obj.name%TYPE) RETURN p_obj.id%TYPE IS
  BEGIN
    RETURN get_object_id(p_otype_id_in => partition_constants.c_obj_type_index_id, owner_in => owner_in, name_in => index_name_in);
  END get_index_id;

  /******************************************************************************/
  /* Update record in table p_obj
  /******************************************************************************/
  PROCEDURE update_p_obj_enabled(p_obj_id_in IN p_obj.id%TYPE, enabled_in IN p_obj.enabled%TYPE) IS
  BEGIN
    UPDATE p_obj SET enabled = enabled_in WHERE id = p_obj_id_in;
  
    IF SQL%ROWCOUNT = 0 THEN
      RAISE e_object_not_added;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;
  END update_p_obj_enabled;

  /******************************************************************************/
  /* Enable partition handling for a table
  /******************************************************************************/
  PROCEDURE enable_table(owner_in IN p_obj.owner%TYPE, table_name_in IN p_obj.name%TYPE) IS
    n_p_obj_id p_obj.id%TYPE := get_table_id(owner_in => owner_in, table_name_in => table_name_in);
  BEGIN
    update_p_obj_enabled(p_obj_id_in => n_p_obj_id, enabled_in => partition_constants.c_enabled);
  EXCEPTION
    WHEN e_object_not_added THEN
      raise_application_error(-20002, 'Table ' || owner_in || '.' || table_name_in || ' not added');
    WHEN OTHERS THEN
      RAISE;
  END enable_table;

  /******************************************************************************/
  /* Disabled partition handling for a table
  /******************************************************************************/
  PROCEDURE disable_table(owner_in IN p_obj.owner%TYPE, table_name_in IN p_obj.name%TYPE) IS
    n_p_obj_id p_obj.id%TYPE := get_table_id(owner_in => owner_in, table_name_in => table_name_in);
  BEGIN
    update_p_obj_enabled(p_obj_id_in => n_p_obj_id, enabled_in => partition_constants.c_disabled);
  EXCEPTION
    WHEN e_object_not_added THEN
      raise_application_error(-20002, 'Table ' || owner_in || '.' || table_name_in || ' not added');
    WHEN OTHERS THEN
      RAISE;
  END disable_table;

  /******************************************************************************/
  /* Enable partition handling for an index
  /******************************************************************************/
  PROCEDURE enable_index(owner_in IN p_obj.owner%TYPE, index_name_in IN p_obj.name%TYPE) IS
    n_p_obj_id p_obj.id%TYPE := get_index_id(owner_in => owner_in, index_name_in => index_name_in);
  BEGIN
    update_p_obj_enabled(p_obj_id_in => n_p_obj_id, enabled_in => partition_constants.c_enabled);
  EXCEPTION
    WHEN e_object_not_added THEN
      raise_application_error(-20003, 'Index ' || owner_in || '.' || index_name_in || ' not added');
    WHEN OTHERS THEN
      RAISE;
  END enable_index;

  /******************************************************************************/
  /* Disable partition handling for an index
  /******************************************************************************/
  PROCEDURE disable_index(owner_in IN p_obj.owner%TYPE, index_name_in IN p_obj.name%TYPE) IS
    n_p_obj_id p_obj.id%TYPE := get_index_id(owner_in => owner_in, index_name_in => index_name_in);
  BEGIN
    update_p_obj_enabled(p_obj_id_in => n_p_obj_id, enabled_in => partition_constants.c_disabled);
  EXCEPTION
    WHEN e_object_not_added THEN
      raise_application_error(-20002, 'Index ' || owner_in || '.' || index_name_in || ' not added');
    WHEN OTHERS THEN
      RAISE;
  END disable_index;

  /******************************************************************************/
  /* add partition key for a table
  /******************************************************************************/
  PROCEDURE add_key(p_obj_id_in    IN p_key.p_obj_id%TYPE,
                    column_name_in IN p_key.column_name%TYPE,
                    p_type_id_in   IN p_key.p_ptype_id%TYPE,
                    p_tech_id_in   IN p_key.p_tech_id%TYPE,
                    enabled_in     IN p_key.enabled%TYPE DEFAULT partition_constants.c_enabled) IS
  BEGIN
    INSERT INTO p_key
      (p_obj_id, p_ptype_id, p_tech_id, column_name, enabled)
    VALUES
      (p_obj_id_in, p_type_id_in, p_tech_id_in, upper(TRIM(column_name_in)), enabled_in);
  EXCEPTION
    WHEN e_check_constraint_violated THEN
      IF instr(SQLERRM, 'P_KEY_P_OBJ_ID_NN') != 0 THEN
        raise_application_error(20100, 'P_OBJ_ID must not be NULL');
      ELSIF instr(SQLERRM, 'P_KEY_P_PTYPE_ID_NN') != 0 THEN
        raise_application_error(-20101, 'P_PTYPE_ID must not be NULL');
      ELSIF instr(SQLERRM, 'P_KEY_P_TECH_ID_NN') != 0 THEN
        raise_application_error(-20102, 'P_TECH_ID must not be NULL');
      ELSIF instr(SQLERRM, 'P_KEY_COLUMN_NAME_NN') != 0 THEN
        raise_application_error(-20103, 'COLUMN_NAME must not be NULL');
      ELSIF instr(SQLERRM, 'P_KEY_ENABLED_NN') != 0 THEN
        raise_application_error(-20104, 'ENABLED must not be NULL');
      END IF;
    WHEN dup_val_on_index THEN
      IF instr(SQLERRM, 'P_KEY_PK') > 0 THEN
        raise_application_error(-20104, 'Partition key with level ' || p_type_id_in || ' for object p_obj_ID = ' || p_obj_id_in || ' already added');
      ELSIF instr(SQLERRM, 'P_KEY_P_OBJ_ID_COLUMN_NAME_UK') > 0 THEN
        raise_application_error(-20105, 'Partition key ' || column_name_in || ' already added for object P_OBJ_ID = ' || p_obj_id_in);
      END IF;
    WHEN e_parent_key_not_found THEN
      IF instr(SQLERRM, 'P_KEY_P_OBJ_FK') != 0 THEN
        raise_application_error(-20106, 'P_OBJ_ID invalid');
      ELSIF instr(SQLERRM, 'P_KEY_P_PTYPE_FK') != 0 THEN
        raise_application_error(-20107, 'P_PTYPE_ID invalid');
      ELSIF instr(SQLERRM, 'P_KEY_P_TECH_FK') != 0 THEN
        raise_application_error(-20108, 'P_TECH_ID invalid');
      END IF;
    WHEN OTHERS THEN
      RAISE;
  END add_key;

  /******************************************************************************/
  /* add partition key for a table
  /******************************************************************************/
  PROCEDURE add_table_partition_key(owner_in        IN p_obj.owner%TYPE,
                                    table_name_in   IN p_obj.name%TYPE,
                                    column_name_in  IN p_key.column_name%TYPE,
                                    technique_id_in IN p_key.p_tech_id%TYPE,
                                    enabled_in      IN p_key.enabled%TYPE DEFAULT partition_constants.c_enabled) IS
    n_p_obj_id p_obj.id%TYPE := get_table_id(owner_in => owner_in, table_name_in => table_name_in);
  BEGIN
    add_key(p_obj_id_in    => n_p_obj_id,
            p_type_id_in   => partition_constants.c_par_type_partition_id,
            p_tech_id_in   => technique_id_in,
            column_name_in => column_name_in,
            enabled_in     => enabled_in);
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;
  END add_table_partition_key;

  /******************************************************************************/
  /* add subpartition key for a table
  /******************************************************************************/
  PROCEDURE add_table_subpartition_key(owner_in        IN p_obj.owner%TYPE,
                                       table_name_in   IN p_obj.name%TYPE,
                                       column_name_in  IN p_key.column_name%TYPE,
                                       technique_id_in IN p_key.p_tech_id%TYPE,
                                       enabled_in      IN p_key.enabled%TYPE DEFAULT partition_constants.c_enabled) IS
    n_p_obj_id p_obj.id%TYPE := get_table_id(owner_in => owner_in, table_name_in => table_name_in);
  BEGIN
    add_key(p_obj_id_in    => n_p_obj_id,
            p_type_id_in   => partition_constants.c_par_type_subpartition_id,
            p_tech_id_in   => technique_id_in,
            column_name_in => column_name_in,
            enabled_in     => enabled_in);
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;
  END add_table_subpartition_key;

  /******************************************************************************/
  /* add partition key for an index
  /******************************************************************************/
  PROCEDURE add_index_partition_key(owner_in        IN p_obj.owner%TYPE,
                                    index_name_in   IN p_obj.name%TYPE,
                                    column_name_in  IN p_key.column_name%TYPE,
                                    technique_id_in IN p_key.p_tech_id%TYPE,
                                    enabled_in      IN p_key.enabled%TYPE DEFAULT partition_constants.c_enabled) IS
    n_p_obj_id p_obj.id%TYPE := get_index_id(owner_in => owner_in, index_name_in => index_name_in);
  BEGIN
    add_key(p_obj_id_in    => n_p_obj_id,
            p_type_id_in   => partition_constants.c_par_type_partition_id,
            p_tech_id_in   => technique_id_in,
            column_name_in => column_name_in,
            enabled_in     => enabled_in);
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;
  END add_index_partition_key;

  /******************************************************************************/
  /* add subpartition key for a table
  /******************************************************************************/
  PROCEDURE add_index_subpartition_key(owner_in        IN p_obj.owner%TYPE,
                                       index_name_in   IN p_obj.name%TYPE,
                                       column_name_in  IN p_key.column_name%TYPE,
                                       technique_id_in IN p_key.p_tech_id%TYPE,
                                       enabled_in      IN p_key.enabled%TYPE DEFAULT partition_constants.c_enabled) IS
    n_p_obj_id p_obj.id%TYPE := get_index_id(owner_in => owner_in, index_name_in => index_name_in);
  BEGIN
    add_key(p_obj_id_in    => n_p_obj_id,
            p_type_id_in   => partition_constants.c_par_type_subpartition_id,
            p_tech_id_in   => technique_id_in,
            column_name_in => column_name_in,
            enabled_in     => enabled_in);
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;
  END add_index_subpartition_key;

  /******************************************************************************/
  /* add definition
  /******************************************************************************/
  PROCEDURE add_definition(p_obj_id_in               IN p_def.p_obj_id%TYPE,
                           partition#_in             IN p_def.partition#%TYPE,
                           subpartition#_in          IN p_def.subpartition#%TYPE DEFAULT 0,
                           high_value_in             IN p_def.high_value%TYPE,
                           p_name_in                 IN p_def.p_name%TYPE,
                           tbs_name_in               IN p_def.tbs_name%TYPE,
                           enabled_in                IN p_def.enabled%TYPE DEFAULT NULL,
                           high_value_format_in      IN p_def.high_value_format%TYPE DEFAULT NULL,
                           p_name_prefix_in          IN p_def.p_name_prefix%TYPE DEFAULT NULL,
                           p_name_suffix_in          IN p_def.p_name_suffix%TYPE DEFAULT NULL,
                           p_naming_function_in      IN p_def.p_naming_function%TYPE DEFAULT NULL,
                           tbs_name_prefix_in        IN p_def.tbs_name_prefix%TYPE DEFAULT NULL,
                           tbs_name_suffix_in        IN p_def.tbs_name_suffix%TYPE DEFAULT NULL,
                           tbs_naming_function_in    IN p_def.tbs_naming_function%TYPE DEFAULT NULL,
                           auto_adjust_enabled_in    IN p_def.auto_adjust_enabled%TYPE DEFAULT 0,
                           auto_adjust_function_in   IN p_def.auto_adjust_function%TYPE DEFAULT NULL,
                           auto_adjust_value_in      IN p_def.auto_adjust_value%TYPE DEFAULT NULL,
                           auto_adjust_end_in        IN p_def.auto_adjust_end%TYPE DEFAULT 0,
                           auto_adjust_start_in      IN p_def.auto_adjust_start%TYPE DEFAULT 0,
                           moving_window_in          IN p_def.moving_window%TYPE DEFAULT 0,
                           moving_window_function_in IN p_def.moving_window_function%TYPE DEFAULT NULL) IS
  BEGIN
    INSERT INTO p_def
      (p_obj_id,
       partition#,
       subpartition#,
       enabled,
       high_value,
       high_value_format,
       p_name,
       p_name_prefix,
       p_name_suffix,
       p_naming_function,
       tbs_name,
       tbs_name_prefix,
       tbs_name_suffix,
       tbs_naming_function,
       auto_adjust_enabled,
       auto_adjust_function,
       auto_adjust_value,
       auto_adjust_end,
       auto_adjust_start,
       moving_window,
       moving_window_function)
    VALUES
      (p_obj_id_in,
       partition#_in,
       subpartition#_in,
       enabled_in,
       REPLACE(high_value_in, partition_constants.c_apostroph),
       high_value_format_in,
       upper(TRIM(p_name_in)),
       upper(TRIM(p_name_prefix_in)),
       upper(TRIM(p_name_suffix_in)),
       upper(TRIM(p_naming_function_in)),
       upper(TRIM(tbs_name_in)),
       upper(TRIM(tbs_name_prefix_in)),
       upper(TRIM(tbs_name_suffix_in)),
       upper(TRIM(tbs_naming_function_in)),
       auto_adjust_enabled_in,
       upper(TRIM(auto_adjust_function_in)),
       auto_adjust_value_in,
       auto_adjust_end_in,
       auto_adjust_start_in,
       moving_window_in,
       upper(TRIM(moving_window_function_in)));
  EXCEPTION
    WHEN e_check_constraint_violated THEN
      IF instr(SQLERRM, 'P_DEF_P_NAMING_FUNCTION_CK') != 0 THEN
        raise_application_error(-20200, 'Partition naming function must be set for auto adjust definitions');
      ELSE
        RAISE;
      END IF;
    WHEN OTHERS THEN
      RAISE;
  END add_definition;

  /******************************************************************************/
  /* Get the next available partition# for a table
  /******************************************************************************/
  FUNCTION get_next_partition#(p_obj_id_in IN p_def.p_obj_id%TYPE) RETURN p_def.partition#%TYPE IS
    n_result p_def.partition#%TYPE;
  BEGIN
    SELECT nvl(MAX(d.partition#) + 1, 1) INTO n_result FROM p_def d WHERE d.p_obj_id = p_obj_id_in;
    RETURN n_result;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;
  END get_next_partition#;

  /******************************************************************************/
  /* Get the next available partition# for a table
  /******************************************************************************/
  FUNCTION get_next_subpartition#(p_obj_id_in IN p_def.p_obj_id%TYPE, partition#_in IN p_def.partition#%TYPE) RETURN p_def.subpartition#%TYPE IS
    n_result p_def.subpartition#%TYPE;
  BEGIN
    SELECT nvl(MAX(d.subpartition#) + 1, 1)
      INTO n_result
      FROM p_def d
     WHERE d.p_obj_id = p_obj_id_in
       AND d.partition# = partition#_in;
  
    RETURN n_result;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;
  END get_next_subpartition#;

  /******************************************************************************/
  /* add table partition definition
  /******************************************************************************/
  PROCEDURE add_table_partition(owner_in                  IN p_obj.owner%TYPE,
                                table_name_in             IN p_obj.name%TYPE,
                                high_value_in             IN p_def.high_value%TYPE,
                                p_name_in                 IN p_def.p_name%TYPE,
                                tbs_name_in               IN p_def.tbs_name%TYPE,
                                enabled_in                IN p_def.enabled%TYPE DEFAULT 1,
                                partition#_in             IN p_def.partition#%TYPE DEFAULT NULL,
                                high_value_format_in      IN p_def.high_value_format%TYPE DEFAULT NULL,
                                p_name_prefix_in          IN p_def.p_name_prefix%TYPE DEFAULT NULL,
                                p_name_suffix_in          IN p_def.p_name_suffix%TYPE DEFAULT NULL,
                                p_naming_function_in      IN p_def.p_naming_function%TYPE DEFAULT NULL,
                                tbs_name_prefix_in        IN p_def.tbs_name_prefix%TYPE DEFAULT NULL,
                                tbs_name_suffix_in        IN p_def.tbs_name_suffix%TYPE DEFAULT NULL,
                                tbs_naming_function_in    IN p_def.tbs_naming_function%TYPE DEFAULT NULL,
                                auto_adjust_enabled_in    IN p_def.auto_adjust_enabled%TYPE DEFAULT 0,
                                auto_adjust_function_in   IN p_def.auto_adjust_function%TYPE DEFAULT NULL,
                                auto_adjust_value_in      IN p_def.auto_adjust_value%TYPE DEFAULT NULL,
                                auto_adjust_end_in        IN p_def.auto_adjust_end%TYPE DEFAULT 0,
                                auto_adjust_start_in      IN p_def.auto_adjust_start%TYPE DEFAULT 0,
                                moving_window_in          IN p_def.moving_window%TYPE DEFAULT 0,
                                moving_window_function_in IN p_def.moving_window_function%TYPE DEFAULT NULL) IS
    n_obj_id     p_obj.id%TYPE := get_table_id(owner_in => owner_in, table_name_in => table_name_in);
    n_partition# p_def.partition#%TYPE;
  BEGIN
    -- If no partition# is set get the next available
    n_partition# := nvl(partition#_in, get_next_partition#(p_obj_id_in => n_obj_id));
    -- add partition definition
    add_definition(p_obj_id_in               => n_obj_id,
                   partition#_in             => n_partition#,
                   enabled_in                => enabled_in,
                   high_value_in             => high_value_in,
                   high_value_format_in      => high_value_format_in,
                   p_name_in                 => p_name_in,
                   p_name_prefix_in          => p_name_prefix_in,
                   p_name_suffix_in          => p_name_suffix_in,
                   p_naming_function_in      => p_naming_function_in,
                   tbs_name_in               => tbs_name_in,
                   tbs_name_prefix_in        => tbs_name_prefix_in,
                   tbs_name_suffix_in        => tbs_name_suffix_in,
                   tbs_naming_function_in    => tbs_naming_function_in,
                   auto_adjust_enabled_in    => auto_adjust_enabled_in,
                   auto_adjust_function_in   => auto_adjust_function_in,
                   auto_adjust_value_in      => auto_adjust_value_in,
                   auto_adjust_end_in        => auto_adjust_end_in,
                   auto_adjust_start_in      => auto_adjust_start_in,
                   moving_window_in          => moving_window_in,
                   moving_window_function_in => moving_window_function_in);
  END add_table_partition;

  /*****************************************************************************
  * add table subpartition definition
  *****************************************************************************/
  PROCEDURE add_table_subpartition(owner_in                  IN p_obj.owner%TYPE,
                                   table_name_in             IN p_obj.name%TYPE,
                                   partition#_in             IN p_def.partition#%TYPE,
                                   high_value_in             IN p_def.high_value%TYPE,
                                   p_name_in                 IN p_def.p_name%TYPE,
                                   tbs_name_in               IN p_def.tbs_name%TYPE,
                                   enabled_in                IN p_def.enabled%TYPE DEFAULT 1,
                                   subpartition#_in          IN p_def.subpartition#%TYPE DEFAULT NULL,
                                   high_value_format_in      IN p_def.high_value_format%TYPE DEFAULT NULL,
                                   p_name_prefix_in          IN p_def.p_name_prefix%TYPE DEFAULT NULL,
                                   p_name_suffix_in          IN p_def.p_name_suffix%TYPE DEFAULT NULL,
                                   p_naming_function_in      IN p_def.p_naming_function%TYPE DEFAULT NULL,
                                   tbs_name_prefix_in        IN p_def.tbs_name_prefix%TYPE DEFAULT NULL,
                                   tbs_name_suffix_in        IN p_def.tbs_name_suffix%TYPE DEFAULT NULL,
                                   tbs_naming_function_in    IN p_def.tbs_naming_function%TYPE DEFAULT NULL,
                                   auto_adjust_enabled_in    IN p_def.auto_adjust_enabled%TYPE DEFAULT 0,
                                   auto_adjust_function_in   IN p_def.auto_adjust_function%TYPE DEFAULT NULL,
                                   auto_adjust_value_in      IN p_def.auto_adjust_value%TYPE DEFAULT NULL,
                                   auto_adjust_end_in        IN p_def.auto_adjust_end%TYPE DEFAULT 0,
                                   auto_adjust_start_in      IN p_def.auto_adjust_start%TYPE DEFAULT 0,
                                   moving_window_in          IN p_def.moving_window%TYPE DEFAULT 0,
                                   moving_window_function_in IN p_def.moving_window_function%TYPE DEFAULT NULL) IS
    n_obj_id        p_obj.id%TYPE := get_table_id(owner_in => owner_in, table_name_in => table_name_in);
    n_subpartition# p_def.subpartition#%TYPE;
  BEGIN
    -- If no partition# is set get the next available
    n_subpartition# := nvl(subpartition#_in, get_next_subpartition#(p_obj_id_in => n_obj_id, partition#_in => partition#_in));
    -- add partition definition
    add_definition(p_obj_id_in               => n_obj_id,
                   partition#_in             => partition#_in,
                   subpartition#_in          => n_subpartition#,
                   enabled_in                => enabled_in,
                   high_value_in             => high_value_in,
                   high_value_format_in      => high_value_format_in,
                   p_name_in                 => p_name_in,
                   p_name_prefix_in          => p_name_prefix_in,
                   p_name_suffix_in          => p_name_suffix_in,
                   p_naming_function_in      => p_naming_function_in,
                   tbs_name_in               => tbs_name_in,
                   tbs_name_prefix_in        => tbs_name_prefix_in,
                   tbs_name_suffix_in        => tbs_name_suffix_in,
                   tbs_naming_function_in    => tbs_naming_function_in,
                   auto_adjust_enabled_in    => auto_adjust_enabled_in,
                   auto_adjust_function_in   => auto_adjust_function_in,
                   auto_adjust_value_in      => auto_adjust_value_in,
                   auto_adjust_end_in        => auto_adjust_end_in,
                   auto_adjust_start_in      => auto_adjust_start_in,
                   moving_window_in          => moving_window_in,
                   moving_window_function_in => moving_window_function_in);
  END add_table_subpartition;

  /******************************************************************************/
  /* add index partition definition
  /******************************************************************************/
  PROCEDURE add_index_partition(owner_in                  IN p_obj.owner%TYPE,
                                index_name_in             IN p_obj.name%TYPE,
                                high_value_in             IN p_def.high_value%TYPE,
                                p_name_in                 IN p_def.p_name%TYPE,
                                tbs_name_in               IN p_def.tbs_name%TYPE,
                                enabled_in                IN p_def.enabled%TYPE DEFAULT 1,
                                partition#_in             IN p_def.partition#%TYPE DEFAULT NULL,
                                high_value_format_in      IN p_def.high_value_format%TYPE DEFAULT NULL,
                                p_name_prefix_in          IN p_def.p_name_prefix%TYPE DEFAULT NULL,
                                p_name_suffix_in          IN p_def.p_name_suffix%TYPE DEFAULT NULL,
                                p_naming_function_in      IN p_def.p_naming_function%TYPE DEFAULT NULL,
                                tbs_name_prefix_in        IN p_def.tbs_name_prefix%TYPE DEFAULT NULL,
                                tbs_name_suffix_in        IN p_def.tbs_name_suffix%TYPE DEFAULT NULL,
                                tbs_naming_function_in    IN p_def.tbs_naming_function%TYPE DEFAULT NULL,
                                auto_adjust_enabled_in    IN p_def.auto_adjust_enabled%TYPE DEFAULT 0,
                                auto_adjust_function_in   IN p_def.auto_adjust_function%TYPE DEFAULT NULL,
                                auto_adjust_value_in      IN p_def.auto_adjust_value%TYPE DEFAULT NULL,
                                auto_adjust_end_in        IN p_def.auto_adjust_end%TYPE DEFAULT 0,
                                auto_adjust_start_in      IN p_def.auto_adjust_start%TYPE DEFAULT 0,
                                moving_window_in          IN p_def.moving_window%TYPE DEFAULT 0,
                                moving_window_function_in IN p_def.moving_window_function%TYPE DEFAULT NULL) IS
    n_obj_id     p_obj.id%TYPE := get_index_id(owner_in => owner_in, index_name_in => index_name_in);
    n_partition# p_def.partition#%TYPE;
  BEGIN
    -- If no partition# is set get the next available
    n_partition# := nvl(partition#_in, get_next_partition#(p_obj_id_in => n_obj_id));
    -- add partition definition
    add_definition(p_obj_id_in               => n_obj_id,
                   partition#_in             => n_partition#,
                   enabled_in                => enabled_in,
                   high_value_in             => high_value_in,
                   high_value_format_in      => high_value_format_in,
                   p_name_in                 => p_name_in,
                   p_name_prefix_in          => p_name_prefix_in,
                   p_name_suffix_in          => p_name_suffix_in,
                   p_naming_function_in      => p_naming_function_in,
                   tbs_name_in               => tbs_name_in,
                   tbs_name_prefix_in        => tbs_name_prefix_in,
                   tbs_name_suffix_in        => tbs_name_suffix_in,
                   tbs_naming_function_in    => tbs_naming_function_in,
                   auto_adjust_enabled_in    => auto_adjust_enabled_in,
                   auto_adjust_function_in   => auto_adjust_function_in,
                   auto_adjust_value_in      => auto_adjust_value_in,
                   auto_adjust_end_in        => auto_adjust_end_in,
                   auto_adjust_start_in      => auto_adjust_start_in,
                   moving_window_in          => moving_window_in,
                   moving_window_function_in => moving_window_function_in);
  END add_index_partition;

  /******************************************************************************/
  /* add index subpartition definition
  /******************************************************************************/
  PROCEDURE add_index_subpartition(owner_in                  IN p_obj.owner%TYPE,
                                   index_name_in             IN p_obj.name%TYPE,
                                   partition#_in             IN p_def.partition#%TYPE,
                                   high_value_in             IN p_def.high_value%TYPE,
                                   p_name_in                 IN p_def.p_name%TYPE,
                                   tbs_name_in               IN p_def.tbs_name%TYPE,
                                   enabled_in                IN p_def.enabled%TYPE DEFAULT 1,
                                   subpartition#_in          IN p_def.subpartition#%TYPE DEFAULT NULL,
                                   high_value_format_in      IN p_def.high_value_format%TYPE DEFAULT NULL,
                                   p_name_prefix_in          IN p_def.p_name_prefix%TYPE DEFAULT NULL,
                                   p_name_suffix_in          IN p_def.p_name_suffix%TYPE DEFAULT NULL,
                                   p_naming_function_in      IN p_def.p_naming_function%TYPE DEFAULT NULL,
                                   tbs_name_prefix_in        IN p_def.tbs_name_prefix%TYPE DEFAULT NULL,
                                   tbs_name_suffix_in        IN p_def.tbs_name_suffix%TYPE DEFAULT NULL,
                                   tbs_naming_function_in    IN p_def.tbs_naming_function%TYPE DEFAULT NULL,
                                   auto_adjust_enabled_in    IN p_def.auto_adjust_enabled%TYPE DEFAULT 0,
                                   auto_adjust_function_in   IN p_def.auto_adjust_function%TYPE DEFAULT NULL,
                                   auto_adjust_value_in      IN p_def.auto_adjust_value%TYPE DEFAULT NULL,
                                   auto_adjust_end_in        IN p_def.auto_adjust_end%TYPE DEFAULT 0,
                                   auto_adjust_start_in      IN p_def.auto_adjust_start%TYPE DEFAULT 0,
                                   moving_window_in          IN p_def.moving_window%TYPE DEFAULT 0,
                                   moving_window_function_in IN p_def.moving_window_function%TYPE DEFAULT NULL) IS
    n_obj_id        p_obj.id%TYPE := get_index_id(owner_in => owner_in, index_name_in => index_name_in);
    n_subpartition# p_def.subpartition#%TYPE;
  BEGIN
    -- If no partition# is set get the next available
    n_subpartition# := nvl(subpartition#_in, get_next_subpartition#(p_obj_id_in => n_obj_id, partition#_in => partition#_in));
    -- add partition definition
    add_definition(p_obj_id_in               => n_obj_id,
                   partition#_in             => partition#_in,
                   subpartition#_in          => n_subpartition#,
                   enabled_in                => enabled_in,
                   high_value_in             => high_value_in,
                   high_value_format_in      => high_value_format_in,
                   p_name_in                 => p_name_in,
                   p_name_prefix_in          => p_name_prefix_in,
                   p_name_suffix_in          => p_name_suffix_in,
                   p_naming_function_in      => p_naming_function_in,
                   tbs_name_in               => tbs_name_in,
                   tbs_name_prefix_in        => tbs_name_prefix_in,
                   tbs_name_suffix_in        => tbs_name_suffix_in,
                   tbs_naming_function_in    => tbs_naming_function_in,
                   auto_adjust_enabled_in    => auto_adjust_enabled_in,
                   auto_adjust_function_in   => auto_adjust_function_in,
                   auto_adjust_value_in      => auto_adjust_value_in,
                   auto_adjust_end_in        => auto_adjust_end_in,
                   auto_adjust_start_in      => auto_adjust_start_in,
                   moving_window_in          => moving_window_in,
                   moving_window_function_in => moving_window_function_in);
  END add_index_subpartition;

  /******************************************************************************/
  /* Update the high value of a definition
  /******************************************************************************/
  PROCEDURE update_high_value(obj_id_in        IN p_def.p_obj_id%TYPE,
                              partition#_in    IN p_def.partition#%TYPE,
                              high_value_in    IN p_def.high_value%TYPE,
                              subpartition#_in IN p_def.subpartition#%TYPE DEFAULT 0) IS
  BEGIN
    UPDATE p_def d
       SET d.high_value = high_value_in
     WHERE d.p_obj_id = obj_id_in
       AND d.partition# = partition#_in
       AND nvl2(subpartition#_in, d.subpartition#, -1) = nvl2(subpartition#_in, subpartition#_in, -1);
  END update_high_value;

  /******************************************************************************/
  /* Update the high value of a table partition definition
  /******************************************************************************/
  PROCEDURE update_table_high_value(owner_in         IN p_obj.owner%TYPE,
                                    table_name_in    IN p_obj.name%TYPE,
                                    partition#_in    IN p_def.partition#%TYPE,
                                    high_value_in    IN p_def.high_value%TYPE,
                                    subpartition#_in IN p_def.subpartition#%TYPE DEFAULT 0) IS
    n_obj_id p_obj.id%TYPE := get_table_id(owner_in => owner_in, table_name_in => table_name_in);
  BEGIN
    update_high_value(obj_id_in => n_obj_id, partition#_in => partition#_in, high_value_in => high_value_in, subpartition#_in => subpartition#_in);
  END update_table_high_value;

  /******************************************************************************/
  /* Update the high value of an index partition definition
  /******************************************************************************/
  PROCEDURE update_index_high_value(owner_in         IN p_obj.owner%TYPE,
                                    index_name_in    IN p_obj.name%TYPE,
                                    partition#_in    IN p_def.partition#%TYPE,
                                    high_value_in    IN p_def.high_value%TYPE,
                                    subpartition#_in IN p_def.subpartition#%TYPE DEFAULT 0) IS
    n_obj_id p_obj.id%TYPE := get_index_id(owner_in => owner_in, index_name_in => index_name_in);
  BEGIN
    update_high_value(obj_id_in => n_obj_id, partition#_in => partition#_in, high_value_in => high_value_in, subpartition#_in => subpartition#_in);
  END update_index_high_value;

  /******************************************************************************
  * Initialize
  ******************************************************************************/
  PROCEDURE initialize IS
    v_module_action VARCHAR2(30 CHAR) := 'initialize';
  BEGIN
    log_admin.info('Initializing ' || gc_module_label, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    module_admin.load_and_set_config(module_name_in => $$PLSQL_UNIT);
  END initialize;

BEGIN
  initialize;
END partition_config;
/
