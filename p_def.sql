CREATE TABLE P_DEF
(
   P_OBJ_ID                 NUMBER (38) CONSTRAINT P_DEF_P_OBJ_ID_NN NOT NULL,
   PARTITION#               NUMBER (7) CONSTRAINT P_DEF_PARTITION#_NN NOT NULL,
   SUBPARTITION#            NUMBER (7)
                               DEFAULT 0
                               CONSTRAINT P_DEF_SUBPARTITION#_NN NOT NULL,
   ENABLED                  NUMBER (1) DEFAULT 0 CONSTRAINT P_DEF_ENABLED_NN NOT NULL,
   HIGH_VALUE               VARCHAR2 (255 CHAR)
                               CONSTRAINT P_DEF_HIGH_VALUE_NN NOT NULL,
   HIGH_VALUE_FORMAT        VARCHAR2 (30 CHAR),
   P_NAME                   VARCHAR2 (30 CHAR) CONSTRAINT P_DEF_P_NAME_NN NOT NULL,
   P_NAME_PREFIX            VARCHAR2 (30 CHAR),
   P_NAME_SUFFIX            VARCHAR2 (30 CHAR),
   P_NAMING_FUNCTION        VARCHAR2 (30 CHAR),
   TBS_NAME                 VARCHAR2 (30 CHAR)
                               CONSTRAINT P_DEF_TBS_NAME_NN NOT NULL,
   TBS_NAME_PREFIX          VARCHAR2 (30 CHAR),
   TBS_NAME_SUFFIX          VARCHAR2 (30 CHAR),
   TBS_NAMING_FUNCTION      VARCHAR2 (30 CHAR),
   AUTO_ADJUST_ENABLED      NUMBER (1)
                               DEFAULT 0
                               CONSTRAINT P_DEF_AUTO_ADJUST_ENABLED_NN NOT NULL,
   AUTO_ADJUST_FUNCTION     VARCHAR2 (30 CHAR),
   AUTO_ADJUST_VALUE        VARCHAR2 (30 CHAR),
   AUTO_ADJUST_END          VARCHAR2 (255 CHAR)
                               DEFAULT '0'
                               CONSTRAINT P_DEF_AUTO_ADJUST_END_NN NOT NULL,
   AUTO_ADJUST_START        VARCHAR2 (255 CHAR)
                               DEFAULT '0'
                               CONSTRAINT P_DEF_AUTO_ADJUST_START_NN NOT NULL,
   MOVING_WINDOW            NUMBER (1)
                               DEFAULT 0
                               CONSTRAINT P_DEF_MOVING_WINDOW_NN NOT NULL,
   MOVING_WINDOW_FUNCTION   VARCHAR2 (255 CHAR)
)
TABLESPACE &DATA_TBS.;

COMMENT ON TABLE P_DEF IS 'Partition definition';

COMMENT ON COLUMN P_DEF.P_OBJ_ID IS
   'Reference to table the definition belongs to';

COMMENT ON COLUMN P_DEF.PARTITION# IS 'Partition number';

COMMENT ON COLUMN P_DEF.ENABLED IS
   'Indicates if the partition definition is enabled or not';

COMMENT ON COLUMN P_DEF.HIGH_VALUE IS 'High value of the partition';

COMMENT ON COLUMN P_DEF.HIGH_VALUE_FORMAT IS
   'Format in which the high value is defined';

COMMENT ON COLUMN P_DEF.P_NAME IS 'Partition name';

COMMENT ON COLUMN P_DEF.P_NAME_PREFIX IS
   'Prefix which will be used on partition name generation';

COMMENT ON COLUMN P_DEF.P_NAME_SUFFIX IS
   'Suffix which will be used on partition name generation';

COMMENT ON COLUMN P_DEF.P_NAMING_FUNCTION IS
   'Function to generate the tablespace_name';

COMMENT ON COLUMN P_DEF.TBS_NAME IS
   'Tablespace name the partition is stored in. For auto adjust partitions this is the format used with TBS_NAMING_FUNCTION.';

COMMENT ON COLUMN P_DEF.TBS_NAME_PREFIX IS
   'Prefix which will be used on tablespace name generation';

COMMENT ON COLUMN P_DEF.TBS_NAME_SUFFIX IS
   'Suffix which will be used on tablespace name generation';

COMMENT ON COLUMN P_DEF.TBS_NAMING_FUNCTION IS
   'Function which will be used on tablespace generation';

COMMENT ON COLUMN P_DEF.AUTO_ADJUST_ENABLED IS
   'Determines if the auto adjust feature is enabled';
COMMENT ON COLUMN P_DEF.AUTO_ADJUST_FUNCTION IS
   'Function to use to calculate old and new partition definitions';
COMMENT ON COLUMN P_DEF.AUTO_ADJUST_VALUE IS
   'Value to use for auto adjust calculation';
COMMENT ON COLUMN P_DEF.AUTO_ADJUST_END IS
   'Amount of new partitions for auto adjusted partitions';
COMMENT ON COLUMN P_DEF.AUTO_ADJUST_START IS
   'Amount of old partitions for auto adjusted partitions';
COMMENT ON COLUMN P_DEF.MOVING_WINDOW IS
   'Determines if the moving window feature is enabled';
COMMENT ON COLUMN P_DEF.MOVING_WINDOW_FUNCTION IS
   'Function to use to calculate the current high value in a moving window';

CREATE INDEX P_DEF_ENABLED_IX
   ON P_DEF (ENABLED)
   TABLESPACE &INDEX_TBS.;

CREATE UNIQUE INDEX P_DEF_P_OBJ_P#_SP#_UX
   ON P_DEF (P_OBJ_ID, PARTITION#, SUBPARTITION#)
   TABLESPACE &INDEX_TBS.;

ALTER TABLE P_DEF ADD
  CONSTRAINT P_DEF_AUTO_ADJUST_ENABLED_CK
  CHECK (auto_adjust_enabled IN (0, 1))
  ENABLE VALIDATE;
ALTER TABLE P_DEF ADD  CONSTRAINT P_DEF_AUTO_ADJUST_VALUE_CK
  CHECK ( (auto_adjust_enabled = 0) OR (auto_adjust_value IS NOT NULL))
  ENABLE VALIDATE;
ALTER TABLE P_DEF ADD  CONSTRAINT P_DEF_ENABLED_CK
  CHECK (enabled IN (0, 1))
  ENABLE VALIDATE;
ALTER TABLE P_DEF ADD  CONSTRAINT P_DEF_MOVING_WINDOW_CK
  CHECK (moving_window BETWEEN 0 AND 2)
  ENABLE VALIDATE;
ALTER TABLE P_DEF ADD CONSTRAINT P_DEF_PARTITION#_CK
  CHECK (partition# > 0)
  ENABLE VALIDATE;
ALTER TABLE P_DEF ADD  CONSTRAINT P_DEF_P_NAMING_FUNCTION_CK
  CHECK ( (auto_adjust_enabled = 0) OR (p_naming_function IS NOT NULL))
  ENABLE VALIDATE;
ALTER TABLE P_DEF ADD  CONSTRAINT P_DEF_SUBPARTITION#_CK
  CHECK (subpartition# >= 0)
  ENABLE VALIDATE;
ALTER TABLE P_DEF ADD  CONSTRAINT P_DEF_PK
  PRIMARY KEY
  (P_OBJ_ID, PARTITION#, SUBPARTITION#)
  USING INDEX P_DEF_P_OBJ_P#_SP#_UX
  ENABLE VALIDATE;

ALTER TABLE P_DEF ADD
  CONSTRAINT P_DEF_P_OBJ_FK
  FOREIGN KEY (P_OBJ_ID)
  REFERENCES P_OBJ (ID)
  ON DELETE CASCADE
  ENABLE VALIDATE;