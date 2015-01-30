CREATE OR REPLACE PACKAGE ADMIN.partition_tools IS
  /*****************************************************************************
  * Partition Tools
  *
  * Author:    Thomas Krahn
  * Date:      2013-12-31
  * Version:   0.9.9
  *
  * Requires:  log_admin v1.2.3
  *            module_admin v0.7.0
  *****************************************************************************/
  gc_module_version CONSTANT module_admin.t_module_version := '0.9.9';
  gc_module_label   CONSTANT module_admin.t_module_label := $$PLSQL_UNIT || ' v' || gc_module_version;

  /****************************************************************************
  * Convert literal to VARCHAR2
  ****************************************************************************/
  PROCEDURE convert_literal_to_varchar2(literal_in IN VARCHAR2, v2_out OUT NOCOPY VARCHAR2);

  /****************************************************************************
  * Convert literal to NUMBER
  ****************************************************************************/
  PROCEDURE convert_literal_to_number(literal_in IN VARCHAR2, n_out OUT NOCOPY NUMBER);

  /****************************************************************************/
  /* Convert timestamp with time zone to literal
  /****************************************************************************/
  PROCEDURE convert_ts_tz_to_literal(ts_tz_in IN TIMESTAMP WITH TIME ZONE, format_in IN VARCHAR2, literal_out OUT VARCHAR2);

  /****************************************************************************
  * Convert literal to timestamp. This function is used to compare timestamp literals.
  ****************************************************************************/
  PROCEDURE convert_literal_to_ts_tz(literal_in IN VARCHAR2, ts_tz_out OUT NOCOPY TIMESTAMP WITH TIME ZONE);

  /****************************************************************************/
  /* Convert table of varchar2(4000 char) into table of timestamp with zime zone
  /****************************************************************************/
  PROCEDURE convert_v2_tab_to_ts_tab(v2_tab_in IN partition_types.t_varchar2_tab, ts_tab_out OUT NOCOPY partition_types.t_timestamp_tab);

  /****************************************************************************/
  /* Convert t_literal_format_tab into table of t_ts_wltz_high_value_tab
  /****************************************************************************/
  PROCEDURE convert_lf_tab_to_ts_hv_tab(lf_tab_in IN partition_types.t_literal_format_tab, ts_hv_tab_out OUT NOCOPY partition_types.t_ts_wltz_high_value_tab);

  /*******************************************************************************
  * Convert varchar2 comma seperated list into table of varchar2(4000)
  *******************************************************************************/
  PROCEDURE convert_v2_to_v2_tab(v2_in IN VARCHAR2, v2_tab OUT partition_types.t_varchar2_tab);
END partition_tools;
/
CREATE OR REPLACE PACKAGE BODY ADMIN.partition_tools IS

  /******************************************************************************
  * Convert literal to VARCHAR2
  ******************************************************************************/
  PROCEDURE convert_literal_to_varchar2(literal_in IN VARCHAR2, v2_out OUT VARCHAR2) IS
    v_module_action log_admin.t_module_action := 'convert_literal_to_varchar2';
    v_sql           VARCHAR2(4000 CHAR);
  BEGIN
    -- Create SQL to execute
    v_sql := 'SELECT ' || literal_in || ' FROM DUAL';
    -- Log debug message
    log_admin.debug(v_sql, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  
    EXECUTE IMMEDIATE v_sql
      INTO v2_out;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.error('Error while converting literal ' || literal_in || ' to VARCHAR2: ' || dbms_utility.format_error_backtrace,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE;
  END convert_literal_to_varchar2;

  /******************************************************************************
  * Convert literal to NUMBER
  ******************************************************************************/
  PROCEDURE convert_literal_to_number(literal_in IN VARCHAR2, n_out OUT NOCOPY NUMBER) IS
    v_module_action log_admin.t_module_action := 'convert_literal_to_number';
    v_sql           VARCHAR2(4000 CHAR);
  BEGIN
    -- Create SQL to execute
    v_sql := 'SELECT ' || literal_in || ' FROM DUAL';
    -- Log debug message
    log_admin.debug(v_sql, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  
    EXECUTE IMMEDIATE v_sql
      INTO n_out;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.error('Error while converting literal ' || literal_in || ' to NUMBER: ' || dbms_utility.format_error_backtrace,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE;
  END convert_literal_to_number;

  /******************************************************************************
  * Convert timestamp with time zone to literal
  ******************************************************************************/
  PROCEDURE convert_ts_tz_to_literal(ts_tz_in IN TIMESTAMP WITH TIME ZONE, format_in IN VARCHAR2, literal_out OUT VARCHAR2) IS
    v_module_action log_admin.t_module_action := 'convert_ts_tz_to_literal';
  BEGIN
    log_admin.debug('Try to convert TIMESTAMP WITH TIME ZONE ''' || ts_tz_in || ''' into literal of format ''' || format_in || '''.',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    -- Convert
    literal_out := to_char(ts_tz_in, format_in);
  
    log_admin.debug('Timestamp with time zone ''' || ts_tz_in || ''' converted into literal ' || literal_out,
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical('Error while converting TIMESTAMP WITH TIME ZONE ' || ts_tz_in || ' to literal. ' || dbms_utility.format_error_backtrace,
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
      RAISE;
  END convert_ts_tz_to_literal;

  /******************************************************************************
  * Convert a literal into a timestamp. 
  ******************************************************************************/
  PROCEDURE convert_literal_to_ts_tz(literal_in IN VARCHAR2, ts_tz_out OUT NOCOPY TIMESTAMP WITH TIME ZONE) IS
    v_module_action log_admin.t_module_action := 'convert_literal_to_ts_tz';
    v_sql           VARCHAR2(4000 CHAR);
  BEGIN
    log_admin.debug('Try to convert literal ''' || literal_in || ''' to TIMESTAMP WITH TIME ZONE',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
    -- Create SQL to execute
    v_sql := 'SELECT ' || literal_in || ' FROM DUAL';
    -- Log SQL for debugging
    log_admin.debug(v_sql, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
    EXECUTE IMMEDIATE v_sql
      INTO ts_tz_out;
  
    log_admin.debug('Literal ''' || literal_in || ''' converted to TIMESTAMP WITH TIME ZONE',
                    module_name_in => $$PLSQL_UNIT,
                    module_action_in => v_module_action);
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.critical('Error while converting literal ''' || literal_in || ''' to TIMESTAMP WITH TIME ZONE. ' || dbms_utility.format_error_backtrace,
                         module_name_in => $$PLSQL_UNIT,
                         module_action_in => v_module_action,
                         sql_errm_in => SQLERRM,
                         sql_code_in => SQLCODE);
      RAISE;
  END convert_literal_to_ts_tz;

  /******************************************************************************
  * Convert table of VARCHAR2 into table of TIMESTAMP WITH TIME ZONE
  ******************************************************************************/
  PROCEDURE convert_v2_tab_to_ts_tab(v2_tab_in IN partition_types.t_varchar2_tab, ts_tab_out OUT NOCOPY partition_types.t_timestamp_tab) IS
    v_module_action log_admin.t_module_action := 'convert_v2_tab_to_ts_tab';
    l_lob           CLOB;
  BEGIN
    IF v2_tab_in.count = 0 THEN
      RETURN;
    END IF;
    -- Create lob
    dbms_lob.createtemporary(l_lob, TRUE);
    -- Loop trough all entries
    FOR i IN v2_tab_in.first .. v2_tab_in.last LOOP
      -- Add UNION ALL if it's not the first entry
      IF i > v2_tab_in.first THEN
        dbms_lob.append(l_lob, chr(13) || 'UNION ALL' || chr(13));
      END IF;
      -- Add the entry
      dbms_lob.append(dest_lob => l_lob, src_lob => 'SELECT ' || v2_tab_in(i) || ' FROM DUAL');
    END LOOP;
  
    -- Only execute the convertion if there is something to convert.
    IF dbms_lob.getlength(l_lob) = 0 THEN
      RETURN;
    END IF;
  
    -- Debug Message
    log_admin.debug(l_lob, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  
    -- Execute the SQL
    EXECUTE IMMEDIATE l_lob BULK COLLECT
      INTO ts_tab_out;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.error('Error while converting table of VARCHAR2 into table of TIMESTAMP WITH TIME ZONE. ' || dbms_utility.format_error_backtrace,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE;
  END convert_v2_tab_to_ts_tab;

  /******************************************************************************
  * Convert t_literal_format_tab into table of t_ts_wltz_high_value_tab
  ******************************************************************************/
  PROCEDURE convert_lf_tab_to_ts_hv_tab(lf_tab_in IN partition_types.t_literal_format_tab, ts_hv_tab_out OUT NOCOPY partition_types.t_ts_wltz_high_value_tab) IS
    v_module_action log_admin.t_module_action := 'convert_lf_tab_to_ts_hv_tab';
    l_lob           CLOB;
  BEGIN
    IF lf_tab_in.count = 0 THEN
      RETURN;
    END IF;
    -- Create the lob
    dbms_lob.createtemporary(l_lob, TRUE);
  
    -- Iterate through all entries
    FOR i IN lf_tab_in.first .. lf_tab_in.last LOOP
      -- Add UNION ALL if it's not the first entry
      IF i > lf_tab_in.first THEN
        dbms_lob.append(l_lob, chr(13) || 'UNION ALL' || chr(13));
      END IF;
      -- Add entry
      dbms_lob.append(dest_lob => l_lob,
                      src_lob  => 'SELECT ' || lf_tab_in(i).literal || ', TO_CHAR(' || lf_tab_in(i).literal || ', ' || lf_tab_in(i).format || ') FROM DUAL');
    END LOOP;
  
    -- Only execute the convertion if there is something to convert.
    IF dbms_lob.getlength(l_lob) = 0 THEN
      RETURN;
    END IF;
  
    -- Debug Message
    log_admin.debug(l_lob, module_name_in => $$PLSQL_UNIT, module_action_in => v_module_action);
  
    -- Execute the SQL
    EXECUTE IMMEDIATE l_lob BULK COLLECT
      INTO ts_hv_tab_out;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.error('Error while converting table of T_LITERAL_FORMAT_TAB into table of T_TS_WLTZ_HIGH_VALUE_TAB. ' || dbms_utility.format_error_backtrace,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE;
  END convert_lf_tab_to_ts_hv_tab;

  /*******************************************************************************
  * Convert varchar2 comma seperated list into table of varchar2(4000)
  *******************************************************************************/
  PROCEDURE convert_v2_to_v2_tab(v2_in IN VARCHAR2, v2_tab OUT NOCOPY partition_types.t_varchar2_tab) IS
    v_module_action log_admin.t_module_action := 'convert_v2_to_v2_tab';
    n_pos           PLS_INTEGER;
    l_v             partition_types.long_varchar := v2_in;
  BEGIN
    n_pos := instr(l_v, ',');
    WHILE n_pos != 0 LOOP
      v2_tab(v2_tab.count + 1) := substr(l_v, 1, n_pos - 1);
      l_v := substr(l_v, n_pos + 1);
      n_pos := instr(l_v, ',');
    END LOOP;
    v2_tab(v2_tab.count + 1) := l_v;
  EXCEPTION
    WHEN OTHERS THEN
      log_admin.error('Error while converting VARCHAR2(' || v2_in || ') to T_VARCHAR2_TAB' || dbms_utility.format_error_backtrace,
                      module_name_in => $$PLSQL_UNIT,
                      module_action_in => v_module_action,
                      sql_errm_in => SQLERRM,
                      sql_code_in => SQLCODE);
      RAISE;
  END convert_v2_to_v2_tab;

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
END partition_tools;
/
