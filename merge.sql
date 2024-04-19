DECLARE 
  dest_org_act_id VARCHAR2(36) := '495bd823-56e7-4269-83f6-ba030d3ef0f2';
  src_org_act_id VARCHAR2(36) := '59d114dd-3c04-4146-820d-83de4b66ab51';
  dest_org_addr_id VARCHAR2(36) := '495bd823-56e7-4269-83f6-ba030d3ef0f2';  -- Destination organization address ID (INTO Lucile Packard Children's Hospital)
  src_org_addr_id VARCHAR2(36) := '59d114dd-3c04-4146-820d-83de4b66ab51';  
  countholder NUMBER := 0;  -- Variable to hold counts.
  stmt VARCHAR2(1000);  -- Variable for SQL statements.
  cnt_stmt VARCHAR2(1000);  -- Variable for count SQL statements.
  updt_stmt VARCHAR2(1000);  -- Variable for update SQL statements.
  start_time TIMESTAMP;  -- Variable to store start time.
  end_time TIMESTAMP;  -- Variable to store end time.

-- Cursor Declarations:
CURSOR fk_actor IS -- This cursor selects foreign key constraints related to actors.
  SELECT 
    a.* 
  FROM 
    all_cons_columns a 
    JOIN all_constraints c ON a.constraint_name = c.constraint_name 
  WHERE 
    c.constraint_type = 'R' -- R denotes referential integrity (foreign key)
    AND r_constraint_name = 'PK_ACTOR';  -- 'PK_ACTOR' is the primary key constraint name.

CURSOR fk_addr IS -- This cursor selects foreign key constraints related to addresses.
  SELECT 
    a.* 
  FROM 
    all_cons_columns a 
    JOIN all_constraints c ON a.constraint_name = c.constraint_name 
  WHERE 
    c.constraint_type = 'R' -- R denotes referential integrity (foreign key)
    AND r_constraint_name = 'PK_ADDRESS';  -- 'PK_ADDRESS' is the primary key constraint name.

-- Procedure to print a line with timestamp:
PROCEDURE println (
  line VARCHAR2, now TIMESTAMP := systimestamp
) IS BEGIN 
  dbms_output.put_line(to_char(now, 'DD-MON-YYYY HH24:MI:SS') || '|' || line);
END println;

-- Procedure to print a line with start and end timestamps:
PROCEDURE printsw (
  line VARCHAR2, start_time TIMESTAMP := systimestamp, 
  end_time TIMESTAMP := systimestamp
) IS BEGIN 
  println(line || '|' || to_char(end_time - start_time, 'SSSS.FF'));
END printsw;

-- Function to build an SQL update statement:
FUNCTION buildupdate (
  tabowner IN VARCHAR2, tabname IN VARCHAR2, 
  tabcol IN VARCHAR2
) RETURN VARCHAR2 AS 
  stmt VARCHAR2(4000);
BEGIN 
  stmt := 'update ' || tabowner || '.' || tabname || ' set ' || tabcol || '=:1' || ' where ' || tabcol || '=:2';
  RETURN stmt;
END buildupdate;

-- Function to build an SQL select statement:
FUNCTION buildsql (
  tabowner IN VARCHAR2, tabname IN VARCHAR2, 
  tabcol IN VARCHAR2, action IN VARCHAR2
) RETURN VARCHAR2 AS 
  stmt VARCHAR2(4000) := 'select ';
BEGIN 
  IF action IS NOT NULL THEN 
    CASE action 
      WHEN 'COUNT' THEN stmt := stmt || ' count(1) ';
      WHEN 'SELECT' THEN stmt := stmt || ' * ';
      ELSE stmt := buildupdate(tabowner, tabname, tabcol);
    END CASE;
    stmt := stmt || ' from ' || tabowner || '.' || tabname || ' where ' || tabcol || '=:1';
  END IF;
  RETURN stmt;
END buildsql;

-- Procedure to backup table data:
PROCEDURE backup_table (
  tabowner IN VARCHAR2, tabname IN VARCHAR2, 
  tabcol IN VARCHAR2, src_owner_id IN VARCHAR2, 
  jira IN VARCHAR2 DEFAULT 'ADMNTL_451'
) AS 
  backup_table_name VARCHAR2(1000);
  table_count NUMBER;
BEGIN 
  -- Generate a unique backup table name (you can modify the naming convention as needed)
  backup_table_name := tabowner || '_' || tabname || '_' || jira;
  -- Check if the backup table exists
  SELECT COUNT(1) INTO table_count FROM user_tables WHERE table_name = backup_table_name;
  -- If the backup table doesn't exist, create it and copy data from the original table
  IF table_count = 0 THEN 
    EXECUTE IMMEDIATE 'CREATE TABLE ' || backup_table_name || ' AS SELECT * FROM ' || tabowner || '.' || tabname || ' WHERE 1 = 0';
  END IF;
  -- Copy data from original table to backup table
  println('INSERT INTO ' || backup_table_name || ' ' || buildsql(tabowner, tabname, tabcol, 'SELECT'));
  EXECUTE IMMEDIATE 'INSERT INTO ' || backup_table_name || ' ' || buildsql(tabowner, tabname, tabcol, 'SELECT') USING src_owner_id;
EXCEPTION 
  WHEN OTHERS THEN println('-----backup_table-------' || sqlerrm || '------------->' || backup_table_name);
END backup_table;

BEGIN 
  
  IF dest_org_addr_id IS NULL THEN 
    SELECT id INTO dest_org_addr_id FROM assurtrk.address a WHERE a.owner_id = dest_org_act_id AND ROWNUM = 1;
  END IF;
  println('dest_org_addr_id:=' || dest_org_addr_id);
  
  -- If source address ID is null, attempt to get it from the database
  IF src_org_addr_id IS NULL THEN 
    BEGIN 
      SELECT id INTO src_org_addr_id FROM assurtrk.address a WHERE a.owner_id = src_org_act_id;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
  END IF;
  
  -- Loop through foreign key constraints related to actors
  FOR f_act IN fk_actor LOOP 
    cnt_stmt := buildsql(f_act.owner, f_act.table_name, f_act.column_name, 'COUNT');
    start_time := systimestamp;
    println(f_act.table_name || '.' || f_act.column_name);
    EXECUTE IMMEDIATE cnt_stmt INTO countholder USING src_org_act_id;
    end_time := systimestamp;
    printsw(f_act.table_name || '.' || f_act.column_name, start_time, end_time);
    -- If there are related records, perform updates
    IF countholder > 0 THEN 
      println(cnt_stmt || ' [' || src_org_act_id || ']' || '|src|' || countholder);
      start_time := systimestamp;
      println(f_act.table_name || '.' || f_act.column_name);
      EXECUTE IMMEDIATE cnt_stmt INTO countholder USING dest_org_act_id;
      end_time := systimestamp;
      printsw(f_act.table_name || '.' || f_act.column_name, start_time, end_time);
      println(cnt_stmt || ' [' || dest_org_act_id || ']' || '|dest|' || countholder);
      start_time := systimestamp;
      updt_stmt := buildsql(f_act.owner, f_act.table_name, f_act.column_name, 'UPDATE');
      BEGIN 
        backup_table(f_act.owner, f_act.table_name, f_act.column_name, src_org_act_id);
        EXECUTE IMMEDIATE updt_stmt USING dest_org_act_id, src_org_act_id;
      EXCEPTION 
        WHEN dup_val_on_index THEN println('------------DUP_VAL_ON_INDEX------------->' || updt_stmt);
        WHEN OTHERS THEN println('------------' || sqlerrm || '------------->' || updt_stmt);
      END;
      start_time := systimestamp;
      println(f_act.table_name || '.' || f_act.column_name);
      EXECUTE IMMEDIATE cnt_stmt INTO countholder USING src_org_act_id;
      end_time := systimestamp;
      printsw(f_act.table_name || '.' || f_act.column_name, start_time, end_time);
      println('POST: ' || cnt_stmt || ' [' || src_org_act_id || ']' || '|src|' || countholder);
      start_time := systimestamp;
      println(f_act.table_name || '.' || f_act.column_name);
      EXECUTE IMMEDIATE cnt_stmt INTO countholder USING dest_org_act_id;
      end_time := systimestamp;
      printsw(f_act.table_name || '.' || f_act.column_name, start_time, end_time);
      println('POST: ' || cnt_stmt || ' [' || dest_org_act_id || ']' || '|dest|' || countholder);
    END IF;
  END LOOP;
  
  -- Loop through foreign key constraints related to addresses
  FOR f_addr IN fk_addr LOOP 
    cnt_stmt := buildsql(f_addr.owner, f_addr.table_name, f_addr.column_name, 'COUNT');
    start_time := systimestamp;
    println(f_addr.table_name || '.' || f_addr.column_name);
    EXECUTE IMMEDIATE cnt_stmt INTO countholder USING src_org_addr_id;
    end_time := systimestamp;
    println(to_char(end_time - start_time, 'SSSS.FF') || '|' || f_addr.table_name || '.' || f_addr.column_name);
    -- If there are related records, perform updates
    IF countholder > 0 THEN 
      println(cnt_stmt || ' [' || src_org_addr_id || ']' || '|src|' || countholder);
      start_time := systimestamp;
      println(to_char(start_time, 'DD-MON-YYYY HH24:MI:SS') || f_addr.table_name || '.' || f_addr.column_name);
      EXECUTE IMMEDIATE cnt_stmt INTO countholder USING dest_org_addr_id;
      end_time := systimestamp;
      printsw(f_addr.table_name || '.' || f_addr.column_name, start_time, end_time);
      println(cnt_stmt || ' [' || dest_org_addr_id || ']' || '|dest|' || countholder);
      updt_stmt := buildsql(f_addr.owner, f_addr.table_name, f_addr.column_name, 'UPDATE');
      BEGIN 
        backup_table(f_addr.owner, f_addr.table_name, f_addr.column_name, src_org_addr_id);
        EXECUTE IMMEDIATE updt_stmt USING dest_org_addr_id, src_org_addr_id;
        start_time := systimestamp;
        println(f_addr.table_name || '.' || f_addr.column_name);
        EXECUTE IMMEDIATE cnt_stmt INTO countholder USING src_org_addr_id;
        end_time := systimestamp;
        printsw(f_addr.table_name || '.' || f_addr.column_name, start_time, end_time);
        println('POST: ' || cnt_stmt || ' [' || src_org_addr_id || ']' || '|src|' || countholder);
        start_time := systimestamp;
        println(f_addr.table_name || '.' || f_addr.column_name);
        EXECUTE IMMEDIATE cnt_stmt INTO countholder USING dest_org_addr_id;
        end_time := systimestamp;
        printsw(f_addr.table_name || '.' || f_addr.column_name, start_time, end_time);
        println('POST: ' || cnt_stmt || ' [' || dest_org_addr_id || ']' || '|dest|' || countholder);
      EXCEPTION 
        WHEN dup_val_on_index THEN println('------------DUP_VAL_ON_INDEX------------->' || updt_stmt);
        WHEN OTHERS THEN println('------------' || sqlerrm || '------------->' || updt_stmt);
      END;
    END IF;
  END LOOP;
  
  -- Update the deleted timestamp for the source actor
  UPDATE assurtrk.actor a SET a.deleted_ts = sysdate WHERE a.id = src_org_act_id;
  
  -- Insert a log entry for the deleted actor
  INSERT INTO assurtrk.logactivity l 
  VALUES (
    fmt_guid(), 
    'Actor', 
    systimestamp, 
    src_org_act_id, 
    TO_TIMESTAMP(to_char((SELECT systimestamp AT TIME ZONE 'UTC' FROM dual), 'dd-mm-yyyy HH24:mi:ss.FF3'), 'dd-mm-yyyy HH24:mi:ss.FF3'), 
    'Deleted', 
    '00000000-0000-0000-0000-000000000000', 
    NULL, 
    NULL, 
    NULL, 
    0, 
    NULL
  );
  
  COMMIT;  -- Rollback the transaction
END;
/