-- USE DATABASE SANDBOX; -- Remove this line in real CICD
DECLARE
    UPDATECOUNT INT;
    INSERTCOUNT INT;
BEGIN
    -- ED PRD environment
    IF (CURRENT_DATABASE() = 'DB_ED_PRD') THEN
        SET SUBSCRIPTIONID = '87da0fea-d9ac-4549-b024-d98acf6649b5';
        SET RESOURCEGROUP = 'edpdevarmrgp004';
        SET ERRORNOTIFICATIONEMAILS = 'dummy@suncor.com';
    -- ED QUT environment
    ELSEIF (CURRENT_DATABASE() = 'DB_ED_QUT') THEN
        SET SUBSCRIPTIONID = '87da0fea-d9ac-4549-b024-d98acf6649b5';
        SET RESOURCEGROUP = 'edpdevarmrgp004';
        SET ERRORNOTIFICATIONEMAILS = 'dummy@suncor.com';
    -- ED DEV environment
    ELSEIF (CURRENT_DATABASE() = 'DB_ED_DEV') THEN
        SET SUBSCRIPTIONID = '87da0fea-d9ac-4549-b024-d98acf6649b5';
        SET RESOURCEGROUP = 'edpdevarmrgp004';
        SET ERRORNOTIFICATIONEMAILS = 'dummy@suncor.com';
    -- PROJECT QUT environment
    ELSEIF (CURRENT_DATABASE() = 'DB_PROJECT2_QUT') THEN
        SET SUBSCRIPTIONID = '87da0fea-d9ac-4549-b024-d98acf6649b5';
        SET RESOURCEGROUP = 'edpdevarmrgp004';
        SET ERRORNOTIFICATIONEMAILS = 'dummy@suncor.com';
    -- PROJECT DEV environment
    ELSEIF (CURRENT_DATABASE() = 'DB_PROJECT2_DEV') THEN
        SET SUBSCRIPTIONID = '87da0fea-d9ac-4549-b024-d98acf6649b4';
        SET RESOURCEGROUP = 'edpdevarmrgp003';
        SET ERRORNOTIFICATIONEMAILS = 'davsun@suncor.com';
    ELSE
        RETURN 'Error: Unknown Database!';
    END IF;

    SET UPDATEDATETIMEUTC = LEFT(SYSDATE(), 19);

    CREATE OR REPLACE TEMPORARY TABLE ORCHESTRATION.MAINCONTROLTABLETMP LIKE ORCHESTRATION.MAINCONTROLTABLE;
    
    EXECUTE IMMEDIATE $$
        INSERT INTO ORCHESTRATION.MAINCONTROLTABLETMP
               (PROJECT
                ,GOLDTABLE
                ,TRIGGERNAME
                ,TASKID
                ,NEXTSTARTDATETIMEUTC
                ,NEXTENDDATETIMEUTC
                ,ENABLEDFLAG
                ,ERRORNOTIFICATIONEMAILS
                ,SUBSCRIPTIONID
                ,RESOURCEGROUP
                ,CONNECTIONSETTING
                ,SNOWFLAKESTOREDPROCEDURE
                ,ADDITIONALPARAMETERS
                ,UPDATEDATETIMEUTC
    		   )
         VALUES
    	       ('DWDM'
               ,'DimWell'
               ,'TR_DWDM_Daily'
               ,120
               ,'1900-01-01 00:00:00'
               ,'2999-12-31 00:00:00'
    		   ,1
    		   ,$ERRORNOTIFICATIONEMAILS
    		   ,$SUBSCRIPTIONID
    		   ,$RESOURCEGROUP
               ,'ls_snowflake_gold_layer'
    		   ,'CALL EDW.SP_DIMWELL_LOAD();'
               ,''
               ,$UPDATEDATETIMEUTC
    		   ),
    	       ('DWDM'
               ,'DimWellActivity'
               ,'TR_DWDM_Daily'
               ,120
               ,'1900-01-01 00:00:00'
               ,'2999-12-31 00:00:00'
    		   ,1
    		   ,$ERRORNOTIFICATIONEMAILS
    		   ,$SUBSCRIPTIONID
    		   ,$RESOURCEGROUP
               ,'ls_snowflake_gold_layer'
    		   ,'CALL EDW.SP_DIMWELLACTIVITY_LOAD();'
               ,''
               ,$UPDATEDATETIMEUTC
    		   ),
    	       ('DWDM'
               ,'DimWellActivityDaily'
               ,'TR_DWDM_Daily'
               ,120
               ,'1900-01-01 00:00:00'
               ,'2999-12-31 00:00:00'
    		   ,1
    		   ,$ERRORNOTIFICATIONEMAILS
    		   ,$SUBSCRIPTIONID
    		   ,$RESOURCEGROUP
               ,'ls_snowflake_gold_layer'
    		   ,'CALL EDW.SP_DIMWELLACTIVITYDAILY_LOAD();'
               ,''
               ,$UPDATEDATETIMEUTC
    		   ),
    	       ('DWDM'
               ,'DimRigActivity'
               ,'TR_DWDM_Daily'
               ,120
               ,'1900-01-01 00:00:00'
               ,'2999-12-31 00:00:00'
               ,1
    		   ,$ERRORNOTIFICATIONEMAILS
    		   ,$SUBSCRIPTIONID
    		   ,$RESOURCEGROUP
               ,'ls_snowflake_gold_layer'
    		   ,'CALL EDW.SP_DIMRIGACTIVITY_LOAD();'
               ,''
               ,$UPDATEDATETIMEUTC
    		   ),
    	       ('DWDM'
               ,'DimWellCoring'
               ,'TR_DWDM_Daily'
               ,120
               ,'1900-01-01 00:00:00'
               ,'2999-12-31 00:00:00'
    		   ,1
    		   ,$ERRORNOTIFICATIONEMAILS
    		   ,$SUBSCRIPTIONID
    		   ,$RESOURCEGROUP
               ,'ls_snowflake_gold_layer'
    		   ,'CALL EDW.SP_DIMWELLCORING_LOAD();'
               ,''
               ,$UPDATEDATETIMEUTC
    		   ),
    	       ('DWDM'
               ,'DimWellBore'
               ,'TR_DWDM_Daily'
               ,120
               ,'1900-01-01 00:00:00'
               ,'2999-12-31 00:00:00'
    		   ,1
    		   ,$ERRORNOTIFICATIONEMAILS
    		   ,$SUBSCRIPTIONID
    		   ,$RESOURCEGROUP
               ,'ls_snowflake_gold_layer'
    		   ,'CALL EDW.SP_DIMWELLLBORE_LOAD();'
               ,''
               ,$UPDATEDATETIMEUTC
    		   ),
    	       ('DWDM'
               ,'DimWellBoreGeoformation'
               ,'TR_DWDM_Daily'
               ,120
               ,'1900-01-01 00:00:00'
               ,'2999-12-31 00:00:00'
               ,1
    		   ,$ERRORNOTIFICATIONEMAILS
    		   ,$SUBSCRIPTIONID
    		   ,$RESOURCEGROUP
               ,'ls_snowflake_gold_layer'
    		   ,'CALL EDW.SP_DIMWELLBOREGEOFORMATION_LOAD();'
               ,''
               ,$UPDATEDATETIMEUTC
    		   ),
    	       ('DWDM'
               ,'FactWellActivityProblem'
               ,'TR_DWDM_Daily'
               ,180
               ,'1900-01-01 00:00:00'
               ,'2999-12-31 00:00:00'
    		   ,1
    		   ,$ERRORNOTIFICATIONEMAILS
    		   ,$SUBSCRIPTIONID
    		   ,$RESOURCEGROUP
               ,'ls_snowflake_gold_layer'
    		   ,'CALL EDW.SP_FACTWELLACTIVITYPROBLEM_LOAD(ProcessStartDateTime=>ProcessStartDateTimePlaceholder,ProcessEndDateTime=>ProcessEndDateTimePlaceholder);'
               ,''
               ,$UPDATEDATETIMEUTC
    		   ),
    	       ('DWDM'
               ,'FactWellCoreActivity'
               ,'TR_DWDM_Daily'
               ,180
               ,'1900-01-01 00:00:00'
               ,'2999-12-31 00:00:00'
    		   ,1
    		   ,$ERRORNOTIFICATIONEMAILS
    		   ,$SUBSCRIPTIONID
    		   ,$RESOURCEGROUP
               ,'ls_snowflake_gold_layer'
    		   ,'CALL EDW.SP_FACTWELLCOREACTIVITY_LOAD();'
               ,''
               ,$UPDATEDATETIMEUTC
    		   )
        $$;
        MERGE INTO ORCHESTRATION.MAINCONTROLTABLE target
            USING ORCHESTRATION.MAINCONTROLTABLETMP source 
            ON target.PROJECT = source.PROJECT AND target.GOLDTABLE = source.GOLDTABLE
            WHEN MATCHED AND NOT(
                target.TRIGGERNAME = source.TRIGGERNAME
                AND target.TASKID = source.TASKID
                AND target.ENABLEDFLAG = source.ENABLEDFLAG
                AND target.ERRORNOTIFICATIONEMAILS = source.ERRORNOTIFICATIONEMAILS
                AND target.SUBSCRIPTIONID = source.SUBSCRIPTIONID
                AND target.RESOURCEGROUP = source.RESOURCEGROUP
                AND target.CONNECTIONSETTING = source.CONNECTIONSETTING
                AND target.SNOWFLAKESTOREDPROCEDURE = source.SNOWFLAKESTOREDPROCEDURE
				AND target.ADDITIONALPARAMETERS = source.ADDITIONALPARAMETERS
                )
                THEN UPDATE SET
                target.TRIGGERNAME = source.TRIGGERNAME
                ,target.TASKID = source.TASKID
                ,target.ENABLEDFLAG = source.ENABLEDFLAG
                ,target.ERRORNOTIFICATIONEMAILS = source.ERRORNOTIFICATIONEMAILS
                ,target.SUBSCRIPTIONID = source.SUBSCRIPTIONID
                ,target.RESOURCEGROUP = source.RESOURCEGROUP
                ,target.CONNECTIONSETTING = source.CONNECTIONSETTING
                ,target.SNOWFLAKESTOREDPROCEDURE = source.SNOWFLAKESTOREDPROCEDURE
				,target.ADDITIONALPARAMETERS = source.ADDITIONALPARAMETERS
                ,target.UPDATEDATETIMEUTC = source.UPDATEDATETIMEUTC
            WHEN NOT MATCHED 
                THEN INSERT (
                PROJECT
    			,GOLDTABLE
    			,TRIGGERNAME
    			,TASKID
    			,NextStartDateTimeUTC
    			,NextEndDateTimeUTC
    			,ENABLEDFLAG
    			,ERRORNOTIFICATIONEMAILS
    			,SUBSCRIPTIONID
    			,RESOURCEGROUP
				,CONNECTIONSETTING
    			,SNOWFLAKESTOREDPROCEDURE
				,ADDITIONALPARAMETERS
                ,UPDATEDATETIMEUTC
    		    ) 
                VALUES (
                source.PROJECT
				,source.GOLDTABLE
				,source.TRIGGERNAME
				,source.TASKID
				,source.NextStartDateTimeUTC
				,source.NextEndDateTimeUTC
				,source.ENABLEDFLAG
				,source.ERRORNOTIFICATIONEMAILS
				,source.SUBSCRIPTIONID
				,source.RESOURCEGROUP
				,source.CONNECTIONSETTING
				,source.SNOWFLAKESTOREDPROCEDURE
				,source.ADDITIONALPARAMETERS
                ,source.UPDATEDATETIMEUTC
                );
    SELECT "number of rows inserted", "number of rows updated" INTO INSERTCOUNT, UPDATECOUNT FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    RETURN 'number of rows inserted: ' || INSERTCOUNT || ', number of rows updated: ' || UPDATECOUNT;
END;