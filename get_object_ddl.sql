CREATE OR REPLACE FUNCTION sys.get_object_ddl(object_type_in IN VARCHAR2, name_in IN VARCHAR2, schema_in IN VARCHAR2 DEFAULT NULL) RETURN CLOB IS
BEGIN
  RETURN dbms_metadata.get_ddl(object_type => object_type_in, NAME => name_in, SCHEMA => schema_in);
END get_object_ddl;
/
