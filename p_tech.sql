CREATE TABLE P_TECH
(
   ID            NUMBER (38) CONSTRAINT P_TECH_ID_NN NOT NULL,
   NAME          VARCHAR2 (10 CHAR) CONSTRAINT P_TECH_NAME_NN NOT NULL,
   DESCRIPTION   VARCHAR2 (255 CHAR)
                    CONSTRAINT P_TECH_DESCRIPTION_NN NOT NULL
)
TABLESPACE &DATA_TBS.;

INSERT INTO p_tech (id, name, description)
     VALUES (1, 'LIST', 'Partitioned by List');

INSERT INTO p_tech (id, name, description)
     VALUES (2, 'RANGE', 'Partitioned by Range');

INSERT INTO p_tech (id, name, description)
     VALUES (3, 'HASH', 'Partitioned by Hash');

INSERT INTO p_tech (id, name, description)
     VALUES (4, 'INTERVAL', 'Partitioned by Interval');

INSERT INTO p_tech (id, name, description)
     VALUES (5, 'REFERENCE', 'Partitioned by Reference');

ALTER TABLE P_TECH
READ ONLY;

COMMENT ON TABLE P_TECH IS 'Partition technique';

COMMENT ON COLUMN P_TECH.ID IS 'Technical unique identifier';

COMMENT ON COLUMN P_TECH.NAME IS 'Name of the partitionm technique';

COMMENT ON COLUMN P_TECH.DESCRIPTION IS 'Description';



CREATE UNIQUE INDEX P_TECH_ID_UX
   ON P_TECH (ID)
   TABLESPACE &INDEX_TBS.;

CREATE UNIQUE INDEX P_TECH_NAME_UX
   ON P_TECH (NAME)
   TABLESPACE &INDEX_TBS.;

ALTER TABLE P_TECH ADD
  CONSTRAINT P_TECH_PK
  PRIMARY KEY
  (ID);

ALTER TABLE P_TECH ADD CONSTRAINT P_TECH_NAME_UK
  UNIQUE (NAME);