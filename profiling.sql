DECLARE
  v_dir      dba_directories.directory_name%TYPE := 'FILES_DIR';
  v_filename VARCHAR2(30 CHAR) := 'partition_handler.trace';
  run_id     NUMBER;
BEGIN
  dbms_hprof.start_profiling(v_dir, v_filename);
  partition_handler.handle_partitions;
  dbms_hprof.stop_profiling;
  run_id := dbms_hprof.analyze(v_dir, v_filename);
  dbms_output.put_line('run_id: ' || run_id);
END;
