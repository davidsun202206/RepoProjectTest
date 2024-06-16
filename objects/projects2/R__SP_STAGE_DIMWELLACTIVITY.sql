CREATE OR REPLACE PROCEDURE EDW.SP_STAGING_DIMWELLACTIVITY(
DebugFlag BOOLEAN DEFAULT FALSE,
ProcessStartDateTime TIMESTAMP_NTZ DEFAULT NULL,
ProcessEndDateTime TIMESTAMP_NTZ DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  StagingSchemaName VARCHAR;
  StagingTableName VARCHAR;
  CteSql VARCHAR;
  Transformation_query VARCHAR;
  Type1ColString VARCHAR;
  Type1Colsortedlist VARCHAR;
  Type2Colsortedlist VARCHAR;
  BusinesskeycolString VARCHAR;
  Businesskeysortedlist VARCHAR;
  InsertCols VARCHAR;
  InsertSql VARCHAR;
  InsertCount INT := 0;
  Res RESULTSET;
BEGIN

/* TO BE DEFINED/DECLARED BY THE DEVELOPER : START */ 

StagingSchemaName := 'EDWSTAGE';
StagingTableName := 'DIMWELLACTIVITYDAILY';
BusinesskeycolString := 'WellActivityDailyGUID';
--Type1 columns
Type1ColString := 'WellActivityDailyGUID,WellActivityGUID,WellGUID,WellBoreGUID,WellActivityPhaseGUID,BoreholeConditions,AreaConditions,RoadConditions,Temperature,WaveConditions,Weather,Wind,ContactInformation,TVDProjectionMethod,EndDateTimeTZ,StartDateTimeTZ,NextReportingPeriod,Remarks,ReportingDay,ReportNumber,RigName,OperationsAtReportTime,FinalStatus,OperationalSummary,TimeLogSummaryCode1,TimeLogSummaryCode2,CoringSummary,WellSiteSupervisor,PrimaryContractor';

/* TO BE DEFINED/DECLARED BY THE DEVELOPER : END */ 

LET FnStagingSchemaName VARCHAR := :StagingSchemaName;
LET FnStagingTableName VARCHAR := :StagingTableName;

-- Call function to set session log level .
EXECUTE IMMEDIATE 'SELECT EDWSTAGE.FN_ALTER_SESSION_LOG_LEVEL()';

--Get Sorted TYPE1COLUMNS list.
SELECT EDWSTAGE.FN_SORT_COLUMNS(:Type1ColString) INTO :Type1Colsortedlist;

END
$$;
