CREATE TABLE P_PTYPE
(
   ID            NUMBER (38) CONSTRAINT P_PTYPE_ID_NN NOT NULL,
   NAME          VARCHAR2 (12 CHAR) CONSTRAINT P_PTYPE_NAME_NN NOT NULL,
   DESCRIPTION   VARCHAR2 (255 CHAR)
                    CONSTRAINT P_PTYPE_DESCRIPTION_NN NOT NULL
)
TABLESPACE &DATA_TBS.;

INSERT INTO &OWNER_USER..p_ptype (id, name, description)
     VALUES (1, 'PARTITION', 'Partition');

INSERT INTO &OWNER_USER..p_ptype (id, name, description)
     VALUES (2, 'SUBPARTITION', 'Subpartition');

ALTER TABLE P_PTYPE
READ ONLY;

CREATE UNIQUE INDEX P_PTYPE_ID_UX
   ON P_PTYPE (ID)
   TABLESPACE &INDEX_TBS.;

CREATE UNIQUE INDEX P_PTYPE_NAME_UX
   ON P_PTYPE (NAME)
   TABLESPACE &INDEX_TBS.;

ALTER TABLE P_PTYPE ADD  CONSTRAINT P_PTYPE_PK
  PRIMARY KEY
  (ID);
ALTER TABLE P_PTYPE ADD  CONSTRAINT P_PTYPE_NAME_UK
  UNIQUE (NAME);