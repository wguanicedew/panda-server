--Expand the dataset contents table to include minramcount in order to be able to
--increase the RAM requirements at job level (before it was at task level) 
alter table ATLAS_PANDA.jedi_dataset_contents add
(
    ramcount number(10) default 0
)

------------------------------------------------------------------

--RETRY ACTIONS SEQUENCE FOR PK
CREATE SEQUENCE ATLAS_PANDA.RETRYACTIONS_ID_SEQ INCREMENT BY 1 
MAXVALUE 999999999999 MINVALUE 1 CACHE 10 ;
  GRANT
  SELECT
    ON ATLAS_PANDA.RETRYACTIONS_ID_SEQ TO ATLAS_PANDA_WRITER ;
  GRANT
  SELECT
    ON ATLAS_PANDA.RETRYACTIONS_ID_SEQ TO ATLAS_PANDA_READER ;
  GRANT
  SELECT
    ON ATLAS_PANDA.RETRYACTIONS_ID_SEQ TO ATLAS_PANDA_WRITEROLE ;
  GRANT
  SELECT
    ON ATLAS_PANDA.RETRYACTIONS_ID_SEQ TO ATLAS_PANDA_READROLE ;

--RETRY ACTIONS TABLE DEFINITION
CREATE TABLE ATLAS_PANDA.RETRYACTIONS
( ID number(10) NOT NULL,
  Name varchar2(50 BYTE) NOT NULL,
  Description varchar2(250 BYTE),
  Active char default 'Y' NOT NULL 
);

--RETRY ACTIONS INDEX ON ID
CREATE UNIQUE INDEX ATLAS_PANDA.RETRYACTIONS_ID_PK ON ATLAS_PANDA.RETRYACTIONS (ID ASC);

--RETRY ACTIONS PRIMARY KEY DEFINITION
ALTER TABLE ATLAS_PANDA.RETRYACTIONS ADD CONSTRAINT RETRYACTIONS_ID_PK PRIMARY KEY ( ID )
USING INDEX ATLAS_PANDA.RETRYACTIONS_ID_PK;

--RETRY ACTIONS PERMISSIONS
GRANT DELETE, INSERT, SELECT, UPDATE
ON ATLAS_PANDA.RETRYACTIONS TO ATLAS_PANDA_WRITER;

GRANT SELECT
ON ATLAS_PANDA.RETRYACTIONS TO ATLAS_PANDA_READER;

GRANT DELETE, INSERT, SELECT, UPDATE
ON ATLAS_PANDA.RETRYACTIONS TO ATLAS_PANDA_WRITEROLE;

GRANT SELECT
ON ATLAS_PANDA.RETRYACTIONS TO ATLAS_PANDA_READROLE;

--EXAMPLES OF INSERTS
/*
insert into ATLAS_PANDA.RETRYACTIONS (ID, name, description)
VALUES (ATLAS_PANDA.RETRYACTIONS_ID_SEQ.nextval, 
           'no_retry', 'This action will prevent PanDA server from retrying the job again. It is considered a final error.');

insert into ATLAS_PANDA.RETRYACTIONS (ID, name, description)
VALUES (ATLAS_PANDA.RETRYACTIONS_ID_SEQ.nextval, 
           'increase_memory', 'Job ran out of memory. Increase memory setting for next retry.');

insert into ATLAS_PANDA.RETRYACTIONS (ID, name, description)
VALUES (ATLAS_PANDA.RETRYACTIONS_ID_SEQ.nextval, 
           'limit_retry', 'Set the number of max retries.');
*/




------------------------------------------------------------------

--ERROR DEFINITIONS SEQUENCE FOR PK
CREATE SEQUENCE ATLAS_PANDA.RETRYERRORS_ID_SEQ INCREMENT BY 1 
MAXVALUE 999999999999 MINVALUE 1 CACHE 10 ;
  GRANT
  SELECT
    ON ATLAS_PANDA.RETRYERRORS_ID_SEQ TO ATLAS_PANDA_WRITER ;
  GRANT
  SELECT
    ON ATLAS_PANDA.RETRYERRORS_ID_SEQ TO ATLAS_PANDA_READER ;
  GRANT
  SELECT
    ON ATLAS_PANDA.RETRYERRORS_ID_SEQ TO ATLAS_PANDA_WRITEROLE ;
  GRANT
  SELECT
    ON ATLAS_PANDA.RETRYERRORS_ID_SEQ TO ATLAS_PANDA_READROLE ;

--RETRY ERRORS TABLE DEFINITION
CREATE TABLE ATLAS_PANDA.RETRYERRORS
( ID number(10) NOT NULL,
  ErrorSource varchar2(256 BYTE) NOT NULL,
  ErrorCode number(10) NOT NULL,
  ErrorDiag varchar2(256 BYTE),
  RetryAction_FK number(10) NOT NULL,
  Parameters varchar2(256 BYTE),
  Architecture varchar2 (256 BYTE),
  Release VARCHAR2 (64 BYTE),
  WorkQueue_ID NUMBER(5),
  Description varchar2(250 BYTE),
  Expiration_date timestamp,
  Active char default 'Y' NOT NULL 
);

--RETRY ACTIONS INDEX ON ID
CREATE UNIQUE INDEX ATLAS_PANDA.RETRYERRORS_ID_PK ON ATLAS_PANDA.RETRYERRORS (ID ASC);

--RETRY ACTIONS PRIMARY KEY DEFINITION
ALTER TABLE ATLAS_PANDA.RETRYERRORS ADD CONSTRAINT RETRYERRORS_ID_PK PRIMARY KEY ( ID )
USING INDEX ATLAS_PANDA.RETRYERRORS_ID_PK;

ALTER TABLE ATLAS_PANDA.RETRYERRORS ADD CONSTRAINT
RETRYERRORS_RETRYACTION_FK FOREIGN KEY ( RetryAction_FK ) REFERENCES
ATLAS_PANDA.RETRYACTIONS ( ID ) NOT DEFERRABLE ;

--RETRY ACTIONS PERMISSIONS
GRANT DELETE, INSERT, SELECT, UPDATE
ON ATLAS_PANDA.RETRYERRORS TO ATLAS_PANDA_WRITER;

GRANT SELECT
ON ATLAS_PANDA.RETRYERRORS TO ATLAS_PANDA_READER;

GRANT DELETE, INSERT, SELECT, UPDATE
ON ATLAS_PANDA.RETRYERRORS TO ATLAS_PANDA_WRITEROLE;

GRANT SELECT
ON ATLAS_PANDA.RETRYERRORS TO ATLAS_PANDA_READROLE;

--EXAMPLES OF INSERTS
/*
insert into ATLAS_PANDA.RETRYERRORS (ID, ErrorSource, ErrorCode, RetryAction_FK, Parameters, Architecture, Release)
VALUES (ATLAS_PANDA.RETRYERRORS_ID_SEQ.nextval, 
           'pilotErrorCode', 1, 4, NULL, NULL, NULL);

insert into ATLAS_PANDA.RETRYERRORS (ID, ErrorSource, ErrorCode, RetryAction_FK, Parameters, Architecture, Release)
VALUES (ATLAS_PANDA.RETRYERRORS_ID_SEQ.nextval, 
           'pilotErrorCode', 2, 5, NULL, NULL, NULL);
           
insert into ATLAS_PANDA.RETRYERRORS (ID, ErrorSource, ErrorCode, RetryAction_FK, Parameters, Architecture, Release)
VALUES (ATLAS_PANDA.RETRYERRORS_ID_SEQ.nextval, 
           'pilotErrorCode', 3, 6, NULL, NULL, NULL);
*/
