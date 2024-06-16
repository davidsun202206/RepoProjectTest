-- USE DATABASE SANDBOX; -- Remove this line in real CICD 
DECLARE
    UPDATECOUNT INT;
    INSERTCOUNT INT;
BEGIN
    SET SNOWFLAKEACCOUNT = CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();
    SET SNOWFLAKEDATABASE = CURRENT_DATABASE();
    SET CONNECTIONSETTING = 'ls_snowflake_gold_layer';
    SET UPDATEDATETIMEUTC = LEFT(SYSDATE(), 19);

    -- ED PRD environment
    IF (CURRENT_DATABASE() = 'DB_ED_PRD') THEN
        SET SNOWFLAKEWAREHOUSE = 'dummy_wh';
        SET SNOWFLAKEUSER = 'dummy_user';
        SET SNOWFLAKEPASSWORDSECRET = 'dummy_secret';
        SET SNOWFLAKEROLE = 'dummy_role';
    -- ED QUT environment
    ELSEIF (CURRENT_DATABASE() = 'DB_ED_QUT') THEN
        SET SNOWFLAKEWAREHOUSE = 'dummy_wh';
        SET SNOWFLAKEUSER = 'dummy_user';
        SET SNOWFLAKEPASSWORDSECRET = 'dummy_secret';
        SET SNOWFLAKEROLE = 'dummy_role';
    -- ED DEV environment
    ELSEIF (CURRENT_DATABASE() = 'DB_ED_DEV') THEN
        SET SNOWFLAKEWAREHOUSE = 'dummy_wh';
        SET SNOWFLAKEUSER = 'dummy_user';
        SET SNOWFLAKEPASSWORDSECRET = 'dummy_secret';
        SET SNOWFLAKEROLE = 'dummy_role';
    -- PROJECT QUT environment
    ELSEIF (CURRENT_DATABASE() = 'DB_PROJECT2_QUT') THEN
        SET SNOWFLAKEWAREHOUSE = 'dummy_wh';
        SET SNOWFLAKEUSER = 'dummy_user';
        SET SNOWFLAKEPASSWORDSECRET = 'dummy_secret';
        SET SNOWFLAKEROLE = 'dummy_role';
    -- PROJECT DEV environment
    ELSEIF (CURRENT_DATABASE() = 'DB_PROJECT2_DEV') THEN
        SET SNOWFLAKEWAREHOUSE = 'demo_wh';
        SET SNOWFLAKEUSER = 'davsun';
        SET SNOWFLAKEPASSWORDSECRET = 'sf--password';
        SET SNOWFLAKEROLE = 'ACCOUNTADMIN';
    ELSE
        RETURN 'Error: Unknown Database!';
    END IF;
    
    CREATE OR REPLACE TEMPORARY TABLE ORCHESTRATION.CONNECTIONCONTROLTABLETMP LIKE ORCHESTRATION.CONNECTIONCONTROLTABLE;
    
    EXECUTE IMMEDIATE $$
    INSERT INTO ORCHESTRATION.CONNECTIONCONTROLTABLETMP
               (CONNECTIONSETTING
                ,SNOWFLAKEACCOUNT
                ,SNOWFLAKEDATABASE
                ,SNOWFLAKEWAREHOUSE
                ,SNOWFLAKEUSER
                ,SNOWFLAKEPASSWORDSECRET
                ,SNOWFLAKEROLE
                ,UPDATEDATETIMEUTC
    		   )
         VALUES
    	       ($CONNECTIONSETTING
               ,$SNOWFLAKEACCOUNT
               ,$SNOWFLAKEDATABASE
               ,$SNOWFLAKEWAREHOUSE
               ,$SNOWFLAKEUSER
               ,$SNOWFLAKEPASSWORDSECRET
               ,$SNOWFLAKEROLE
               ,$UPDATEDATETIMEUTC
    		   )
        $$;
        MERGE INTO ORCHESTRATION.CONNECTIONCONTROLTABLE target
            USING ORCHESTRATION.CONNECTIONCONTROLTABLETMP source 
            ON target.CONNECTIONSETTING = source.CONNECTIONSETTING
            WHEN MATCHED AND NOT(
                target.SNOWFLAKEACCOUNT = source.SNOWFLAKEACCOUNT
                AND target.SNOWFLAKEDATABASE = source.SNOWFLAKEDATABASE
                AND target.SNOWFLAKEWAREHOUSE = source.SNOWFLAKEWAREHOUSE
                AND target.SNOWFLAKEUSER = source.SNOWFLAKEUSER
                AND target.SNOWFLAKEPASSWORDSECRET = source.SNOWFLAKEPASSWORDSECRET
                AND target.SNOWFLAKEROLE = source.SNOWFLAKEROLE
                )
                THEN UPDATE SET
                target.SNOWFLAKEACCOUNT = source.SNOWFLAKEACCOUNT
                ,target.SNOWFLAKEDATABASE = source.SNOWFLAKEDATABASE
                ,target.SNOWFLAKEWAREHOUSE = source.SNOWFLAKEWAREHOUSE
                ,target.SNOWFLAKEUSER = source.SNOWFLAKEUSER
                ,target.SNOWFLAKEPASSWORDSECRET = source.SNOWFLAKEPASSWORDSECRET
                ,target.SNOWFLAKEROLE = source.SNOWFLAKEROLE
                ,target.UPDATEDATETIMEUTC = source.UPDATEDATETIMEUTC
            WHEN NOT MATCHED 
                THEN INSERT (
                CONNECTIONSETTING
                ,SNOWFLAKEACCOUNT
                ,SNOWFLAKEDATABASE
                ,SNOWFLAKEWAREHOUSE
                ,SNOWFLAKEUSER
                ,SNOWFLAKEPASSWORDSECRET
                ,SNOWFLAKEROLE
                ,UPDATEDATETIMEUTC
    		    ) 
                VALUES (
                source.CONNECTIONSETTING
                ,source.SNOWFLAKEACCOUNT
                ,source.SNOWFLAKEDATABASE
                ,source.SNOWFLAKEWAREHOUSE
                ,source.SNOWFLAKEUSER
                ,source.SNOWFLAKEPASSWORDSECRET
                ,source.SNOWFLAKEROLE
                ,source.UPDATEDATETIMEUTC
                );
    SELECT "number of rows inserted", "number of rows updated" INTO INSERTCOUNT, UPDATECOUNT FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    RETURN 'number of rows inserted: ' || INSERTCOUNT || ', number of rows updated: ' || UPDATECOUNT;
END;


