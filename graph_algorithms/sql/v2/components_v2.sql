CREATE OR REPLACE FUNCTION scc_bfs_from(
    IN vertex_id INTEGER
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN

    -- init bfs edges table
    INSERT INTO bfs_edges
    SELECT vertex_id, edges.dst_id, 0 AS flag
    FROM edges WHERE edges.src_id = vertex_id;

    -- init discovered_vertices
    INSERT INTO discovered_vertices VALUES (vertex_id);
    INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_edges
        SELECT DISTINCT t1.src_id,  t1.dst_id, 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, edges.dst_id FROM
        bfs_edges JOIN edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.src_id) AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.vid
        WHERE discovered_vertices.vid is NULL;

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION scc_bfs_from(
    IN vertex_id INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN

    -- init bfs edges table
    EXECUTE
    'INSERT INTO bfs_edges SELECT ' || vertex_id || ', edges.dst_id, 0 AS flag
    FROM edges WHERE edges.src_id = ' || vertex_id ||
    ' AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

    -- init discovered_vertices
    INSERT INTO discovered_vertices VALUES (vertex_id);
    INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        EXECUTE
        'INSERT INTO bfs_edges SELECT DISTINCT t1.src_id,  t1.dst_id, 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, edges.dst_id FROM
        bfs_edges JOIN edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.src_id 
        AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.vid
        WHERE discovered_vertices.vid is NULL';

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION scc_reversed_bfs_from(
    IN vertex_id INTEGER
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN

    -- init bfs edges table
    INSERT INTO bfs_edges SELECT vertex_id, edges.src_id, 0 AS flag
    FROM edges WHERE edges.dst_id = vertex_id;

    -- init discovered_vertices_reversed
    INSERT INTO discovered_vertices_reversed VALUES (vertex_id);
    INSERT INTO discovered_vertices_reversed SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_edges
        SELECT DISTINCT t1.dst_id,  t1.src_id, 0 FROM
        (SELECT bfs_edges.dst_id, edges.src_id FROM
        bfs_edges JOIN edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.dst_id) AS t1
        LEFT JOIN discovered_vertices_reversed ON t1.src_id = discovered_vertices_reversed.vid
        WHERE discovered_vertices_reversed.vid is NULL;

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices_reversed SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION scc_reversed_bfs_from(
    IN vertex_id INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN

    -- init bfs edges table
    EXECUTE
    'INSERT INTO bfs_edges SELECT ' || vertex_id || ', edges.src_id, 0 AS flag
    FROM edges WHERE edges.dst_id = ' || vertex_id ||
    ' AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

    -- init discovered_vertices_reversed
    INSERT INTO discovered_vertices_reversed VALUES (vertex_id);
    INSERT INTO discovered_vertices_reversed SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        EXECUTE
        'INSERT INTO bfs_edges SELECT DISTINCT t1.dst_id,  t1.src_id, 0 FROM
        (SELECT bfs_edges.dst_id, edges.src_id FROM
        bfs_edges JOIN edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.dst_id 
        AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        LEFT JOIN discovered_vertices_reversed ON t1.src_id = discovered_vertices_reversed.vid
        WHERE discovered_vertices_reversed.vid is NULL';

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices_reversed SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION strongly_connected_components(
    IN vertex_id INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    result_count INTEGER;
BEGIN
    -- create bfs edges table
    CREATE TEMPORARY TABLE bfs_edges(src_id INTEGER, dst_id INTEGER, flag INTEGER)
    ON COMMIT DROP DISTRIBUTED BY (dst_id);

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vid INTEGER)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

        -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices_reversed(vid INTEGER)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

    PERFORM scc_bfs_from(vertex_id);
    PERFORM scc_reversed_bfs_from(vertex_id);

    EXECUTE
    'CREATE TABLE ' || result_table_name || '(vid INTEGER) DISTRIBUTED BY (vid)';

    EXECUTE
    'INSERT INTO ' || result_table_name || ' SELECT discovered_vertices.vid
    FROM discovered_vertices JOIN discovered_vertices_reversed
    ON discovered_vertices.vid = discovered_vertices_reversed.vid';

    EXECUTE 'SELECT COUNT(1) FROM ' || result_table_name INTO result_count;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION strongly_connected_components(
    IN vertex_id INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    result_count INTEGER;
BEGIN
    -- create bfs edges table
    CREATE TEMPORARY TABLE bfs_edges(src_id INTEGER, dst_id INTEGER, flag INTEGER)
    ON COMMIT DROP DISTRIBUTED BY (dst_id);

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vid INTEGER)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

        -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices_reversed(vid INTEGER)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

    PERFORM scc_bfs_from(vertex_id, edge_type_column, edge_type_value);
    PERFORM scc_reversed_bfs_from(vertex_id, edge_type_column, edge_type_value);

    EXECUTE
    'CREATE TABLE ' || result_table_name || '(vid INTEGER) DISTRIBUTED BY (vid)';

    EXECUTE
    'INSERT INTO ' || result_table_name || ' SELECT discovered_vertices.vid
    FROM discovered_vertices JOIN discovered_vertices_reversed
    ON discovered_vertices.vid = discovered_vertices_reversed.vid';

    EXECUTE 'SELECT COUNT(1) FROM ' || result_table_name INTO result_count;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION weakly_connected_components(
    IN vertex_id INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    result_count INTEGER;
BEGIN

    -- create bfs edges table
    CREATE TEMPORARY TABLE bfs_edges(src_id INTEGER, dst_id INTEGER, flag INTEGER)
    ON COMMIT DROP DISTRIBUTED BY (dst_id);

    -- init bfs edges table
    INSERT INTO bfs_edges
    SELECT vertex_id, edges.src_id, 0 AS flag
    FROM public.edges WHERE edges.dst_id = vertex_id
    UNION SELECT vertex_id, edges.dst_id, 0 AS flag
    FROM public.edges WHERE edges.src_id = vertex_id;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vid INTEGER)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

    -- init discovered_vertices
    INSERT INTO discovered_vertices VALUES (vertex_id);
    INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_edges
        SELECT DISTINCT t1.src_id,  t1.dst_id, 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, edges.dst_id FROM
        bfs_edges JOIN public.edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.src_id
        UNION SELECT bfs_edges.dst_id AS src_id, edges.src_id AS dst_id FROM
        bfs_edges JOIN public.edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.dst_id) AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.vid
        WHERE discovered_vertices.vid is NULL;

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

    EXECUTE
    'CREATE TABLE ' || result_table_name || '(vid INTEGER) DISTRIBUTED BY (vid)';

    EXECUTE
    'INSERT INTO ' || result_table_name || ' SELECT vid FROM discovered_vertices';

    SELECT COUNT(1) INTO result_count FROM discovered_vertices;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION weakly_connected_components(
    IN vertex_id INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    result_count INTEGER;
BEGIN

    -- create bfs edges table
    CREATE TEMPORARY TABLE bfs_edges(src_id INTEGER, dst_id INTEGER, flag INTEGER)
    ON COMMIT DROP DISTRIBUTED BY (dst_id);

    -- init bfs edges table
    EXECUTE
    'INSERT INTO bfs_edges
    SELECT ' || vertex_id || ', edges.src_id, 0 AS flag
    FROM public.edges WHERE edges.dst_id = ' || vertex_id || ' AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value
    || ' UNION SELECT ' || vertex_id || ', edges.dst_id, 0 AS flag
    FROM public.edges WHERE edges.src_id = ' || vertex_id || ' AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vid INTEGER)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

    -- init discovered_vertices
    INSERT INTO discovered_vertices VALUES (vertex_id);
    INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        EXECUTE
        'INSERT INTO bfs_edges SELECT DISTINCT t1.src_id,  t1.dst_id, 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, edges.dst_id FROM
        bfs_edges JOIN public.edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.src_id 
        AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value
        || ' UNION SELECT bfs_edges.dst_id AS src_id, edges.src_id AS dst_id FROM
        bfs_edges JOIN public.edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.dst_id 
        AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.vid
        WHERE discovered_vertices.vid is NULL';

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

    EXECUTE
    'CREATE TABLE ' || result_table_name || '(vid INTEGER) DISTRIBUTED BY (vid)';

    EXECUTE
    'INSERT INTO ' || result_table_name || ' SELECT vid FROM discovered_vertices';

    SELECT COUNT(1) INTO result_count FROM discovered_vertices;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.weakly_connected_components(
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num2update INTEGER;
    current_loop INTEGER := 1;
BEGIN

    -- create and init wcc table
    DROP TABLE IF EXISTS wcc_t;
    CREATE TEMP TABLE wcc_t AS
    SELECT vid, vid AS cid FROM public.vertices
    DISTRIBUTED BY (vid);

    -- first iteration
    CREATE TEMP TABLE wcc_msg AS
    SELECT t1.vid, MIN(t1.cid) AS cid FROM
    (SELECT src_id AS vid, cid FROM wcc_t JOIN public.edges_mirror
    ON wcc_t.vid = edges_mirror.dst_id UNION
    SELECT dst_id AS vid, cid FROM wcc_t JOIN public.edges 
    ON wcc_t.vid = edges.src_id) AS t1 
    GROUP BY vid DISTRIBUTED BY (vid);

    CREATE TEMP TABLE need2update AS
    SELECT wcc_t.vid, wcc_msg.cid FROM wcc_t JOIN wcc_msg 
    ON wcc_t.vid = wcc_msg.vid AND wcc_t.cid > wcc_msg.cid
    DISTRIBUTED BY (vid);

    SELECT COUNT(1) INTO num2update FROM need2update;

    WHILE num2update > 0 LOOP

        CREATE TEMP TABLE new_wcc_t AS
        SELECT wcc_t.vid, COALESCE(need2update.cid, wcc_t.cid) AS cid
        FROM wcc_t LEFT JOIN need2update USING (vid)
        DISTRIBUTED BY (vid);

        DROP TABLE wcc_t;
        ALTER TABLE new_wcc_t RENAME TO wcc_t;

        DROP TABLE IF EXISTS wcc_msg;
        CREATE TEMP TABLE wcc_msg AS
        SELECT t1.vid, MIN(t1.cid) AS cid FROM
        (SELECT src_id AS vid, cid FROM need2update JOIN public.edges_mirror
        ON need2update.vid = edges_mirror.dst_id UNION
        SELECT dst_id AS vid, cid FROM need2update JOIN public.edges 
        ON need2update.vid = edges.src_id) AS t1 GROUP BY vid
        DISTRIBUTED BY (vid);

        DROP TABLE IF EXISTS need2update;
        CREATE TEMP TABLE need2update AS
        SELECT wcc_t.vid, wcc_msg.cid FROM wcc_t JOIN wcc_msg 
        ON wcc_t.vid = wcc_msg.vid AND wcc_t.cid > wcc_msg.cid
        DISTRIBUTED BY (vid);

        SELECT COUNT(1) INTO num2update FROM need2update;

        current_loop := current_loop + 1;

    END LOOP;

    DROP TABLE IF EXISTS need2update, wcc_msg;

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS 
    SELECT vid, cid AS attr FROM wcc_t DISTRIBUTED BY (vid)';

    DROP TABLE wcc_t;

    RETURN current_loop;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.weakly_connected_components(
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num2update INTEGER;
    current_loop INTEGER := 1;
BEGIN

    -- create and init wcc table
    DROP TABLE IF EXISTS wcc_t;
    CREATE TEMP TABLE wcc_t AS
    SELECT vid, vid AS cid FROM public.vertices
    DISTRIBUTED BY (vid);

    -- first iteration
    EXECUTE
    'CREATE TEMP TABLE wcc_msg AS
    SELECT t1.vid, MIN(t1.cid) AS cid FROM
    (SELECT src_id AS vid, cid FROM wcc_t JOIN public.edges_mirror
    ON wcc_t.vid = edges_mirror.dst_id AND edges_mirror.' || quote_ident(edge_type_column) || ' = ' || edge_type_value 
    || ' UNION
    SELECT dst_id AS vid, cid FROM wcc_t JOIN public.edges 
    ON wcc_t.vid = edges.src_id AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value 
    || ') AS t1 GROUP BY vid DISTRIBUTED BY (vid)';

    CREATE TEMP TABLE need2update AS
    SELECT wcc_t.vid, MIN(wcc_msg.cid) AS cid FROM wcc_t JOIN wcc_msg 
    ON wcc_t.vid = wcc_msg.vid AND wcc_t.cid > wcc_msg.cid GROUP BY wcc_t.vid
    DISTRIBUTED BY (vid);

    DROP TABLE IF EXISTS wcc_msg;

    SELECT COUNT(1) INTO num2update FROM need2update;

    WHILE num2update > 0 LOOP
        CREATE TEMP TABLE new_wcc_t AS
        SELECT wcc_t.vid, COALESCE(need2update.cid, wcc_t.cid) AS cid
        FROM wcc_t LEFT JOIN need2update USING (vid)
        DISTRIBUTED BY (vid);

        DROP TABLE wcc_t;
        ALTER TABLE new_wcc_t RENAME TO wcc_t;

        EXECUTE
        'CREATE TEMP TABLE wcc_msg AS
        SELECT t1.vid, MIN(t1.cid) AS cid FROM
        (SELECT src_id AS vid, cid FROM need2update JOIN public.edges_mirror
        ON need2update.vid = edges_mirror.dst_id AND edges_mirror.' || quote_ident(edge_type_column) || ' = ' || edge_type_value 
        || ' UNION
        SELECT dst_id AS vid, cid FROM need2update JOIN public.edges 
        ON need2update.vid = edges.src_id AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value 
        || ') AS t1 GROUP BY vid DISTRIBUTED BY (vid)';

        DROP TABLE IF EXISTS need2update;
        CREATE TEMP TABLE need2update AS
        SELECT wcc_t.vid, MIN(wcc_msg.cid) AS cid FROM wcc_t JOIN wcc_msg 
        ON wcc_t.vid = wcc_msg.vid AND wcc_t.cid > wcc_msg.cid GROUP BY wcc_t.vid
        DISTRIBUTED BY (vid);

        DROP TABLE IF EXISTS wcc_msg;

        SELECT COUNT(1) INTO num2update FROM need2update;

        current_loop := current_loop + 1;

    END LOOP;

    DROP TABLE IF EXISTS need2update;

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS 
    SELECT vid, cid AS attr FROM wcc_t DISTRIBUTED BY (vid)';

    DROP TABLE wcc_t;

    RETURN current_loop;

END;
$$ LANGUAGE plpgsql;

