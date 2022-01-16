CREATE OR REPLACE FUNCTION ${graph_name}.shortest_path(
    IN source BIGINT, 
    IN target BIGINT, 
    IN max_length INTEGER,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER := 1;
    current_loop INTEGER := 1;
    target_reached INTEGER := 0;
BEGIN
    DROP TABLE IF EXISTS paths1;
    CREATE TEMPORARY TABLE paths1(
        path BIGINT[],
        edges_type VARCHAR[],
        tail_vid BIGINT
    ) DISTRIBUTED BY (tail_vid);
    DROP TABLE IF EXISTS paths2;
    CREATE TEMPORARY TABLE paths2(
        path BIGINT[],
        edges_type VARCHAR[],
        head_vid BIGINT
    ) DISTRIBUTED BY (head_vid);
    CREATE TEMPORARY TABLE discovered_vertices1(
        vid BIGINT
    ) WITH (appendoptimized=TRUE,orientation=row) 
    ON COMMIT DROP DISTRIBUTED BY (vid);
    INSERT INTO discovered_vertices1 VALUES(source);
    CREATE TEMPORARY TABLE discovered_vertices2(
        vid BIGINT
    ) WITH (appendoptimized=TRUE,orientation=row) 
    ON COMMIT DROP DISTRIBUTED BY (vid);
    INSERT INTO discovered_vertices2 VALUES(target);
        
    INSERT INTO paths1 VALUES(ARRAY[source], ARRAY[]::VARCHAR[], source);
    INSERT INTO paths2 VALUES(ARRAY[]::BIGINT[], ARRAY[]::VARCHAR[], target);

    WHILE num_growing_path > 0 AND target_reached = 0 AND current_loop <= max_length LOOP
        IF current_loop % 2 = 1 THEN
            CREATE TEMPORARY TABLE paths_tmp1 AS
            SELECT paths1.path||edge_all.dst_id AS path, paths1.edges_type||edge_all.etype AS edges_type, edge_all.dst_id AS tail_vid
            FROM paths1 JOIN ${graph_name}.edge_all
            ON paths1.tail_vid = edge_all.src_id 
            AND edge_all.dst_id NOT IN (SELECT vid FROM discovered_vertices1)
            DISTRIBUTED BY (tail_vid);
            DROP TABLE paths1;
            ALTER TABLE paths_tmp1 RENAME TO paths1;

            INSERT INTO discovered_vertices1
            SELECT DISTINCT tail_vid FROM paths1;
            -- check whether leaf has reached
            SELECT COUNT(1) INTO num_growing_path FROM paths1;
        ELSE
            CREATE TEMPORARY TABLE paths_tmp2 AS
            SELECT edge_all.dst_id||paths2.path AS path, edge_all.etype||paths2.edges_type AS edges_type, edge_all.src_id AS head_vid
            FROM paths2 JOIN ${graph_name}.edge_all
            ON paths2.head_vid = edge_all.dst_id 
            AND edge_all.src_id NOT IN (SELECT vid FROM discovered_vertices2)
            DISTRIBUTED BY (head_vid);
            DROP TABLE paths2;
            ALTER TABLE paths_tmp2 RENAME TO paths2;

            INSERT INTO discovered_vertices2
            SELECT DISTINCT head_vid FROM paths2;
            -- check whether leaf has reached
            SELECT COUNT(1) INTO num_growing_path FROM paths2;
        END IF;
                
        -- check whether target has reached
        SELECT COUNT(1) INTO target_reached FROM paths1 JOIN paths2 ON paths1.tail_vid = paths2.head_vid;

        current_loop := current_loop + 1;

    END LOOP;

    EXECUTE 'CREATE TEMP TABLE '||result_table_name||' AS
    SELECT paths1.path||paths2.path AS path, paths1.edges_type||paths2.edges_type AS edges_type
    FROM paths1 JOIN paths2 ON paths1.tail_vid = paths2.head_vid
    DISTRIBUTED RANDOMLY';
    DROP TABLE IF EXISTS paths1, paths2;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${graph_name}.shortest_path(
    IN source BIGINT, 
    IN target BIGINT, 
    IN max_length INTEGER,
    IN edge_type VARCHAR,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER := 1;
    current_loop INTEGER := 1;
    target_reached INTEGER := 0;
BEGIN
    DROP TABLE IF EXISTS paths1;
    CREATE TEMPORARY TABLE paths1(
        path BIGINT[],
        tail_vid BIGINT
    ) DISTRIBUTED BY (tail_vid);
    DROP TABLE IF EXISTS paths2;
    CREATE TEMPORARY TABLE paths2(
        path BIGINT[],
        head_vid BIGINT
    ) DISTRIBUTED BY (head_vid);
    CREATE TEMPORARY TABLE discovered_vertices1(
        vid BIGINT
    ) WITH (appendoptimized=TRUE,orientation=row) 
    ON COMMIT DROP DISTRIBUTED BY (vid);
    INSERT INTO discovered_vertices1 VALUES(source);
    CREATE TEMPORARY TABLE discovered_vertices2(
        vid BIGINT
    ) WITH (appendoptimized=TRUE,orientation=row) 
    ON COMMIT DROP DISTRIBUTED BY (vid);
    INSERT INTO discovered_vertices2 VALUES(target);
        
    INSERT INTO paths1 VALUES(ARRAY[source], source);
    INSERT INTO paths2 VALUES(ARRAY[]::BIGINT[], target);

    WHILE num_growing_path > 0 AND target_reached = 0 AND current_loop <= max_length LOOP
        IF current_loop % 2 = 1 THEN
            EXECUTE
            'CREATE TEMPORARY TABLE paths_tmp1 AS
            SELECT paths1.path||edge_'||edge_type||'.dst_id AS path, edge_'||edge_type||'.dst_id AS tail_vid
            FROM paths1 JOIN ${graph_name}.edge_'||edge_type||
            ' ON paths1.tail_vid = edge_'||edge_type||'.src_id 
            AND edge_'||edge_type||'.dst_id NOT IN (SELECT vid FROM discovered_vertices1)
            DISTRIBUTED BY (tail_vid)';
            DROP TABLE paths1;
            ALTER TABLE paths_tmp1 RENAME TO paths1;

            INSERT INTO discovered_vertices1
            SELECT DISTINCT tail_vid FROM paths1;
            -- check whether leaf has reached
            SELECT COUNT(1) INTO num_growing_path FROM paths1;
        ELSE
            EXECUTE
            'CREATE TEMPORARY TABLE paths_tmp2 AS
            SELECT edge_'||edge_type||'.dst_id||paths2.path AS path, edge_'||edge_type||'.src_id AS head_vid
            FROM paths2 JOIN ${graph_name}.edge_'||edge_type||'
            ON paths2.head_vid = edge_'||edge_type||'.dst_id 
            AND edge_'||edge_type||'.src_id NOT IN (SELECT vid FROM discovered_vertices2)
            DISTRIBUTED BY (head_vid)';
            DROP TABLE paths2;
            ALTER TABLE paths_tmp2 RENAME TO paths2;

            INSERT INTO discovered_vertices2
            SELECT DISTINCT head_vid FROM paths2;
            -- check whether leaf has reached
            SELECT COUNT(1) INTO num_growing_path FROM paths2;
        END IF;
                
        -- check whether target has reached
        SELECT COUNT(1) INTO target_reached FROM paths1 JOIN paths2 ON paths1.tail_vid = paths2.head_vid;

        current_loop := current_loop + 1;

    END LOOP;

    EXECUTE 'CREATE TEMP TABLE '||result_table_name||' AS
    SELECT paths1.path||paths2.path AS path
    FROM paths1 JOIN paths2 ON paths1.tail_vid = paths2.head_vid
    DISTRIBUTED RANDOMLY';
    DROP TABLE IF EXISTS paths1, paths2;
END;
$$ LANGUAGE plpgsql;