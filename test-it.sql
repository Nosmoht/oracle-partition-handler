PL/SQL Developer Test script 3.0
9
-- Created on 24.07.2013 by TKRAH 
declare 
  -- Local variables here
  i integer;
begin
  -- Test statements here
  admin.log_admin.truncate_log_entry;
  admin.partition_handler.handle_partitions;
end;
0
6
module_name_in
v_result
context_name_in
v_context_name
attribute_in
value_in
