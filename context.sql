CREATE OR REPLACE CONTEXT partition_handler_ctx USING config_admin;

BEGIN
   module_admin.register_module (module_name_in    => 'PARTITION_HANDLER',
                                 context_name_in   => 'PARTITION_HANDLER_CTX',
                                 enabled_in        => 1);
   module_admin.register_module (module_name_in    => 'PARTITION_TOOLS',
                                 context_name_in   => 'PARTITION_HANDLER_CTX',
                                 enabled_in        => 1);
   module_admin.register_module (module_name_in    => 'PARTITION_REDEFINE',
                                 context_name_in   => 'PARTITION_HANDLER_CTX',
                                 enabled_in        => 1);
   COMMIT;
END;
/