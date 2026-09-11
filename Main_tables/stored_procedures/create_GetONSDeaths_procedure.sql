---------------------------------------------------------------------------
-- create_GetONSDeaths_procedure.sql
--
-- Create procedure to return ONS dates of death and reg dates, and derive
-- a new date of death field as:
-- ONS date of death field where an NHS number present, else CLD date of date
-- field (NB either/both fields may be NULL)
--
-- The ONS mortality record chosen for each unique NHS number is selected
-- according to the following criteria:
-- 1) latest registration of death (as corrections expected in dataset)
-- 2) latest date of death (as lower impact if value is incorrect)
--
-- Note:
-- - Input table must contain Der_NHS_Number_Traced_Pseudo (traced NHS number)
--   and Der_NHS_Number_Pseudo (LA-submitted NHS number)
-- - Saves snapshot of ONS deaths if @SaveDeathsSnapshot parameter specified
--   (by default does not save)
-- - Reads from existing snapshot if @InputDeathsSnapshot parameter specified
--   (by default uses raw Mortality table)
--
-- Returns table of same format as input table plus ONS date of death & reg
-- date (where exists) and a derived date of death field
--
-- See example executions of procedure below
---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS ASC_Sandbox.GetONSDeaths
GO

CREATE PROCEDURE ASC_Sandbox.GetONSDeaths
  @InputTable SYSNAME = NULL,
  @OutputTable AS NVARCHAR(100),
  @InputDeathsSnapshot SYSNAME = NULL, 
  @SaveDeathsSnapshot AS NVARCHAR(1) = 'N'
AS
BEGIN

  ---------------------------------------------------------------------------
  -- Create input/output row count variables and synonym for input table
  ---------------------------------------------------------------------------

  SET NOCOUNT ON;

  DECLARE @InputRows BIGINT;
  DECLARE @OutputRows BIGINT;
  DECLARE @Query NVARCHAR(MAX)
  SET @Query = N'
    SELECT @Rows = SUM(rows)
    FROM sys.partitions
    WHERE object_id = OBJECT_ID(''' + @InputTable + N''')
    AND index_id IN (0,1);

    DROP TABLE IF EXISTS ' + @OutputTable + N';

    DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable_GetONSDeaths;
    CREATE SYNONYM ASC_Sandbox.InputTable_GetONSDeaths
    FOR ' + @InputTable + N';
    
    DROP SYNONYM IF EXISTS ASC_Sandbox.DeathsSnapshot';

  EXEC sp_executesql
    @Query,
    N'@Rows BIGINT OUTPUT',
    @Rows = @InputRows OUTPUT;

  ---------------------------------------------------------------------------
  -- Get ONS deaths (from raw Mortality table OR a saved snapshot)
  ---------------------------------------------------------------------------

  DROP TABLE IF EXISTS #ONSDeaths;

  CREATE TABLE #ONSDeaths
  (
    Der_DHSC_Pseudo_NHS_Number VARCHAR(64),
    Date_of_Death_ONS DATE,
    Death_Reg_Date DATE
  );

  IF @InputDeathsSnapshot IS NULL
  ---------------------------------------------------------------------------
  -- Get and deduplicate ONS deaths records from raw Mortality table
  ---------------------------------------------------------------------------
  BEGIN

    INSERT INTO #ONSDeaths
    (
      Der_DHSC_Pseudo_NHS_Number,
      Date_of_Death_ONS,
      Death_Reg_Date
    )
    SELECT
      Der_DHSC_Pseudo_NHS_Number,
      Date_of_Death_ONS,
      Death_Reg_Date
    FROM (
      SELECT
        Der_DHSC_Pseudo_NHS_Number,
        CAST(reg_date_of_death AS DATE) AS Date_of_Death_ONS,
        CAST(reg_date AS DATE) AS Death_Reg_Date,
        ROW_NUMBER() OVER (
          PARTITION BY Der_DHSC_Pseudo_NHS_Number
          ORDER BY 
            CAST(reg_date AS DATE) DESC,         -- latest registration of death (as corrections expected in dataset)
            CAST(reg_date_of_death AS DATE) DESC -- latest date of death (as lower impact if value is incorrect)
        ) AS rn
      FROM DHSC_Mortality.Mortality
      WHERE Der_DHSC_Pseudo_NHS_Number IS NOT NULL
      AND Der_DHSC_Pseudo_NHS_Number != 'CBCAE5BC1DABCA4E69B63AC2D6672C9E6C2FB2726D06B14200A4E0C6D979B862'
      -- exclude NHS number with approaching 1000 death registrations (likely to be an invalid NHS number)
    ) AS a
    WHERE rn = 1;

    IF @SaveDeathsSnapshot = 'Y'
    ---------------------------------------------------
    -- Save snapshot of ONS deaths for reproducibility
    -- Mortality table updated weekly on Tuesdays
    ---------------------------------------------------
    BEGIN
      DECLARE @OutputSnapshotTable VARCHAR(256) =
        'ASC_Sandbox.ONS_Deaths_Snapshot_' + FORMAT(GETDATE(), 'yyMMdd');

      SET @Query = N'
        DROP TABLE IF EXISTS ' + @OutputSnapshotTable + ';
        SELECT *
        INTO ' + @OutputSnapshotTable + N'
        FROM #ONSDeaths;';

      EXEC sp_executesql @Query;
    END

  END
  ELSE
  ---------------------------------------------------------------------------
  -- Get deduplicated ONS deaths from snapshot
  ---------------------------------------------------------------------------
  BEGIN

    BEGIN TRY

      SET @Query = N'
        INSERT INTO #ONSDeaths
        (
          Der_DHSC_Pseudo_NHS_Number,
          Date_of_Death_ONS,
          Death_Reg_Date
        )
        SELECT
          Der_DHSC_Pseudo_NHS_Number,
          Date_of_Death_ONS,
          Death_Reg_Date
        FROM ' + @InputDeathsSnapshot + N';';

      EXEC sp_executesql @Query;

      IF NOT EXISTS (SELECT 1 FROM #ONSDeaths)
        BEGIN
          PRINT 'Deaths snapshot table is empty';
          RETURN;
        END;

    END TRY
    BEGIN CATCH

      PRINT 'Unable to load deaths snapshot table: ' + ISNULL(@InputDeathsSnapshot, 'NULL');
      PRINT ERROR_MESSAGE();
      RETURN;

    END CATCH;

  END;

  CREATE CLUSTERED INDEX IX_ONSDeaths
  ON #ONSDeaths (Der_DHSC_Pseudo_NHS_Number);

  ---------------------------------------------------------------------------
  -- Add ONS death records to RawSubmissions table
  ---------------------------------------------------------------------------

  DROP TABLE IF EXISTS #OutputTable;

  SELECT
    b.*,
    Date_of_Death AS Date_of_Death_Raw,
    d.Date_of_Death_ONS,
    d.Death_Reg_Date,
    CASE
      WHEN COALESCE(b.Der_NHS_Number_Traced_Pseudo, b.Der_NHS_Number_Pseudo) IS NOT NULL THEN d.Date_of_Death_ONS
      ELSE b.Date_of_Death
    END AS Der_Date_of_Death 
  INTO #OutputTable
  FROM ASC_Sandbox.InputTable_GetONSDeaths AS b
  LEFT JOIN #ONSDeaths AS d
  ON COALESCE(b.Der_NHS_Number_Traced_Pseudo, b.Der_NHS_Number_Pseudo) = d.Der_DHSC_Pseudo_NHS_Number

  -- Drop original date of death field to highlight Der_ and _Raw fields
  ALTER TABLE #OutputTable
  DROP COLUMN Date_of_Death;

  -- Write output table
  SET @Query = N'SELECT * INTO ' + @OutputTable + N' FROM #OutputTable;'
  EXEC(@Query);

  -- Get output table row count
  SET @Query = N'
    SELECT @Rows = SUM(rows)
    FROM sys.partitions
    WHERE object_id = OBJECT_ID(''' + @OutputTable + N''')
    AND index_id IN (0,1);';

  EXEC sp_executesql
    @Query,
    N'@Rows BIGINT OUTPUT',
    @Rows = @OutputRows OUTPUT;

  -- Check output table row count matches input table row count
  IF @InputRows = @OutputRows
  BEGIN
    PRINT '';
    PRINT '=====================================================';
    PRINT '> Input rows: ' + CAST(@InputRows AS VARCHAR(20));
    PRINT '> Output rows: ' + CAST(@OutputRows AS VARCHAR(20));
    PRINT '';
    PRINT '> ONS deaths added' 
  END
  ELSE
  BEGIN
    PRINT '';
    PRINT '=====================================================';
    PRINT '';
    PRINT '> WARNING: GetONSDeaths row count mismatch';
    PRINT '> Input rows : ' + CAST(@InputRows AS VARCHAR(20));
    PRINT '> Output rows: ' + CAST(@OutputRows AS VARCHAR(20));
  END

  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable_GetONSDeaths;

END
GO