CREATE TABLE p_obj_log
(
   p_obj_id   NUMBER (38, 0) CONSTRAINT p_obj_log_p_obj_id_nn NOT NULL,
   start_ts   TIMESTAMP WITH LOCAL TIME ZONE
                 CONSTRAINT p_obj_log_start_ts_nn NOT NULL,
   end_ts     TIMESTAMP WITH LOCAL TIME ZONE
                 CONSTRAINT p_obj_log_end_ts_nn NOT NULL,
   duration   INTERVAL DAY TO SECOND GENERATED ALWAYS AS (end_ts - start_ts)
)
TABLESPACE &DATA_TBS.;

CREATE INDEX p_obj_log_p_obj_id_ix
   ON p_obj_log (p_obj_id)
   TABLESPACE &INDEX_TBS.;

ALTER TABLE p_obj_log ADD CONSTRAINT p_obj_log_p_obj_fk FOREIGN KEY (p_obj_id) REFERENCES p_obj (id);

CREATE OR REPLACE TRIGGER p_obj_bur
   BEFORE UPDATE OF last_run_start_ts
   ON p_obj
   FOR EACH ROW
   WHEN (old.last_run_start_ts IS NOT NULL)
BEGIN
   INSERT INTO p_obj_log (p_obj_id, start_ts, end_ts)
        VALUES (:old.id, :old.last_run_start_ts, :old.last_run_end_ts);
END p_obj_bur;
/