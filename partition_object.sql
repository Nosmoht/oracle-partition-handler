CREATE OR REPLACE FORCE VIEW PARTITION_OBJECT
(
   OBJECT_ID,
   ENABLED,
   ORDER#,
   OBJECT_TYPE,
   OBJECT_OWNER,
   OBJECT_NAME,
   LABEL_NAME,
   PARTITION_KEY,
   PARTITION_KEY_DATA_TYPE,
   PARTITION_TECHNIQUE,
   SUBPARTITION_KEY,
   SUBPARTITION_KEY_DATA_TYPE,
   SUBPARTITION_TECHNIQUE
)
AS
     SELECT o.id AS object_id,
            o.enabled,
            o.order#,
            ot.name AS object_type,
            o.owner AS object_owner,
            o.name object_name,
            ot.name || ' "' || o.owner || '"."' || o.name || '"' AS label_name,
            pk.column_name AS partition_key,
            DECODE (
               ot.name,
               'TABLE', (SELECT tc.data_type
                           FROM dba_tab_columns tc
                          WHERE     tc.owner = o.owner
                                AND tc.table_name = o.name
                                AND tc.column_name = pk.column_name),
               'INDEX', (SELECT tc.data_type
                           FROM dba_tab_columns tc
                          WHERE (tc.owner, tc.table_name, tc.column_name) =
                                   (SELECT ic.table_owner,
                                           ic.table_name,
                                           ic.column_name
                                      FROM dba_ind_columns ic
                                     WHERE     ic.index_owner = o.owner
                                           AND ic.index_name = o.name
                                           AND ic.column_name = pk.column_name)),
               NULL)
               AS partition_key_data_type,
            (SELECT pt.name
               FROM p_tech pt
              WHERE pt.id = pk.p_tech_id)
               AS partition_technique,
            sk.column_name AS subpartition_key,
            DECODE (
               ot.name,
               'TABLE', (SELECT tc.data_type
                           FROM dba_tab_columns tc
                          WHERE     tc.owner = o.owner
                                AND tc.table_name = o.name
                                AND tc.column_name = sk.column_name),
               'INDEX', (SELECT tc.data_type
                           FROM dba_tab_columns tc
                          WHERE (tc.owner, tc.table_name, tc.column_name) =
                                   (SELECT ic.table_owner,
                                           ic.table_name,
                                           ic.column_name
                                      FROM dba_ind_columns ic
                                     WHERE     ic.index_owner = o.owner
                                           AND ic.index_name = o.name
                                           AND ic.column_name = sk.column_name)),
               NULL)
               AS subpartition_key_data_type,
            (SELECT pt.name
               FROM p_tech pt
              WHERE pt.id = sk.p_tech_id)
               AS subpartition_technique
       FROM p_obj o
            JOIN p_otype ot ON (ot.id = o.p_otype_id)
            JOIN p_key pk ON (pk.p_obj_id = o.id AND pk.p_ptype_id = 1)
            LEFT OUTER JOIN p_key sk
               ON (sk.p_obj_id = o.id AND sk.p_ptype_id = 2)
   ORDER BY o.owner, o.name;
/
