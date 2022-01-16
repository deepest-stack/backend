CREATE OR REPLACE FUNCTION scc_bfs_from(
    IN vertex_id VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN

    -- create bfs edges table
    CREATE TEMPORARY TABLE bfs_edges ON COMMIT DROP AS
    SELECT owner_vertex AS src_id, other_vertex AS dst_id, 0 AS flag
    FROM public.g_oe WHERE g_oe.owner_vertex = vertex_id
    DISTRIBUTED BY (dst_id);

    -- create discovered_vertices
    CREATE TEMPORARY TABLE discovered_vertices
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP AS
    SELECT vertex_id AS id
    DISTRIBUTED BY (id);
    INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_edges
        SELECT DISTINCT t1.src_id,  t1.dst_id, 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, g_oe.other_vertex AS dst_id FROM
        bfs_edges JOIN public.g_oe ON bfs_edges.flag = 1 AND bfs_edges.dst_id = g_oe.owner_vertex) AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.id
        WHERE discovered_vertices.id is NULL;

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION scc_bfs_from(
    IN vertex_id VARCHAR,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN

    -- create bfs edges table
    EXECUTE
    'CREATE TEMPORARY TABLE bfs_edges ON COMMIT DROP AS
    SELECT owner_vertex AS src_id, other_vertex AS dst_id, 0 AS flag
    FROM public.g_oe WHERE g_oe.owner_vertex = ' || quote_literal(vertex_id) ||
    ' AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
    ' DISTRIBUTED BY (dst_id)';

    -- create discovered_vertices
    CREATE TEMPORARY TABLE discovered_vertices
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP AS
    SELECT vertex_id AS id
    DISTRIBUTED BY (id);
    INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        EXECUTE
        'INSERT INTO bfs_edges SELECT DISTINCT t1.src_id,  t1.dst_id, 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, g_oe.other_vertex AS dst_id FROM
        bfs_edges JOIN public.g_oe ON bfs_edges.flag = 1 AND bfs_edges.dst_id = g_oe.owner_vertex 
        AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.id
        WHERE discovered_vertices.id is NULL';

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION scc_reversed_bfs_from(
    IN vertex_id VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN

    -- init bfs edges table
    INSERT INTO bfs_edges SELECT vertex_id, g_ie.other_vertex, 0
    FROM public.g_ie WHERE g_ie.owner_vertex = vertex_id;

    -- create discovered_vertices_reversed
    CREATE TEMPORARY TABLE discovered_vertices_reversed
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP AS
    SELECT vertex_id AS id
    DISTRIBUTED BY (id);
    INSERT INTO discovered_vertices_reversed SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_edges
        SELECT DISTINCT t1.dst_id,  t1.src_id, 0 FROM
        (SELECT bfs_edges.dst_id, g_ie.other_vertex AS src_id FROM
        bfs_edges JOIN public.g_ie ON bfs_edges.flag = 1 AND bfs_edges.dst_id = g_ie.owner_vertex) AS t1
        LEFT JOIN discovered_vertices_reversed ON t1.src_id = discovered_vertices_reversed.id
        WHERE discovered_vertices_reversed.id is NULL;

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices_reversed SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION scc_reversed_bfs_from(
    IN vertex_id VARCHAR,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN

    -- init bfs edges table
    EXECUTE
    'INSERT INTO bfs_edges SELECT g_ie.owner_vertex, g_ie.other_vertex, 0
    FROM public.g_ie WHERE g_ie.owner_vertex = ' || quote_literal(vertex_id) ||
    ' AND g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

    -- create discovered_vertices_reversed
    CREATE TEMPORARY TABLE discovered_vertices_reversed
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP AS
    SELECT vertex_id AS id
    DISTRIBUTED BY (id);
    INSERT INTO discovered_vertices_reversed SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        EXECUTE
        'INSERT INTO bfs_edges SELECT DISTINCT t1.dst_id, t1.src_id, 0 FROM
        (SELECT bfs_edges.dst_id, g_ie.other_vertex AS src_id FROM
        bfs_edges JOIN public.g_ie ON bfs_edges.flag = 1 AND bfs_edges.dst_id = g_ie.owner_vertex 
        AND g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        LEFT JOIN discovered_vertices_reversed ON t1.src_id = discovered_vertices_reversed.id
        WHERE discovered_vertices_reversed.id is NULL';

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices_reversed SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION strongly_connected_components(
    IN vertex_id VARCHAR,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    result_count INTEGER;
BEGIN

    PERFORM scc_bfs_from(vertex_id);
    PERFORM scc_reversed_bfs_from(vertex_id);

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT discovered_vertices.id
    FROM discovered_vertices JOIN discovered_vertices_reversed
    USING (id)
    DISTRIBUTED BY (id)';

    EXECUTE 'SELECT COUNT(1) FROM ' || result_table_name INTO result_count;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION strongly_connected_components(
    IN vertex_id VARCHAR,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    result_count INTEGER;
BEGIN

    PERFORM scc_bfs_from(vertex_id, edge_type_column, edge_type_value);
    PERFORM scc_reversed_bfs_from(vertex_id, edge_type_column, edge_type_value);

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT discovered_vertices.id
    FROM discovered_vertices JOIN discovered_vertices_reversed
    USING (id)
    DISTRIBUTED BY (id)';

    EXECUTE 'SELECT COUNT(1) FROM ' || result_table_name INTO result_count;

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
    SELECT id, id AS cid FROM public.g_v
    DISTRIBUTED BY (id);

    -- first iteration
    CREATE TEMP TABLE wcc_msg AS
    SELECT t1.id, MIN(t1.cid) AS cid FROM
    (SELECT other_vertex AS id, cid FROM wcc_t JOIN public.g_ie
    ON wcc_t.id = g_ie.owner_vertex UNION ALL
    SELECT other_vertex AS id, cid FROM wcc_t JOIN public.g_oe 
    ON wcc_t.id = g_oe.owner_vertex) AS t1 
    GROUP BY id DISTRIBUTED BY (id);

    CREATE TEMP TABLE need2update AS
    SELECT wcc_t.id, wcc_msg.cid FROM wcc_t JOIN wcc_msg 
    ON wcc_t.id = wcc_msg.id AND wcc_t.cid > wcc_msg.cid
    DISTRIBUTED BY (id);

    SELECT COUNT(1) INTO num2update FROM need2update;

    WHILE num2update > 0 LOOP

        CREATE TEMP TABLE new_wcc_t AS
        SELECT wcc_t.id, COALESCE(need2update.cid, wcc_t.cid) AS cid
        FROM wcc_t LEFT JOIN need2update USING (id)
        DISTRIBUTED BY (id);

        DROP TABLE wcc_t;
        ALTER TABLE new_wcc_t RENAME TO wcc_t;

        DROP TABLE IF EXISTS wcc_msg;
        CREATE TEMP TABLE wcc_msg AS
        SELECT t1.id, MIN(t1.cid) AS cid FROM
        (SELECT other_vertex AS id, cid FROM need2update JOIN public.g_ie
        ON need2update.id = g_ie.owner_vertex UNION ALL
        SELECT other_vertex AS id, cid FROM need2update JOIN public.g_oe 
        ON need2update.id = g_oe.owner_vertex) AS t1 GROUP BY id
        DISTRIBUTED BY (id);

        DROP TABLE IF EXISTS need2update;
        CREATE TEMP TABLE need2update AS
        SELECT wcc_t.id, wcc_msg.cid FROM wcc_t JOIN wcc_msg 
        ON wcc_t.id = wcc_msg.id AND wcc_t.cid > wcc_msg.cid
        DISTRIBUTED BY (id);

        SELECT COUNT(1) INTO num2update FROM need2update;

        current_loop := current_loop + 1;

    END LOOP;

    DROP TABLE IF EXISTS need2update, wcc_msg;

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS 
    SELECT id, cid AS attr FROM wcc_t DISTRIBUTED BY (id)';

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
    IF edge_type_column IS NULL OR edge_type_value IS NULL THEN
        SELECT public.weakly_connected_components(result_table_name) INTO current_loop;
        RETURN current_loop;
    END IF;
    -- create and init wcc table
    DROP TABLE IF EXISTS wcc_t;
    CREATE TEMP TABLE wcc_t AS
    SELECT id, id AS cid FROM public.g_v
    DISTRIBUTED BY (id);

    -- first iteration
    EXECUTE
    'CREATE TEMP TABLE wcc_msg AS
    SELECT t1.id, MIN(t1.cid) AS cid FROM
    (SELECT other_vertex AS id, cid FROM wcc_t JOIN public.g_ie
    ON wcc_t.id = g_ie.owner_vertex AND g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value 
    || ' UNION ALL
    SELECT other_vertex AS id, cid FROM wcc_t JOIN public.g_oe 
    ON wcc_t.id = g_oe.owner_vertex AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value 
    || ') AS t1 GROUP BY id DISTRIBUTED BY (id)';

    CREATE TEMP TABLE need2update AS
    SELECT wcc_t.id, MIN(wcc_msg.cid) AS cid FROM wcc_t JOIN wcc_msg 
    ON wcc_t.id = wcc_msg.id AND wcc_t.cid > wcc_msg.cid GROUP BY wcc_t.id
    DISTRIBUTED BY (id);

    DROP TABLE IF EXISTS wcc_msg;

    SELECT COUNT(1) INTO num2update FROM need2update;

    WHILE num2update > 0 LOOP
        CREATE TEMP TABLE new_wcc_t AS
        SELECT wcc_t.id, COALESCE(need2update.cid, wcc_t.cid) AS cid
        FROM wcc_t LEFT JOIN need2update USING (id)
        DISTRIBUTED BY (id);

        DROP TABLE wcc_t;
        ALTER TABLE new_wcc_t RENAME TO wcc_t;

        EXECUTE
        'CREATE TEMP TABLE wcc_msg AS
        SELECT t1.id, MIN(t1.cid) AS cid FROM
        (SELECT other_vertex AS id, cid FROM need2update JOIN public.g_ie
        ON need2update.id = g_ie.owner_vertex AND g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value 
        || ' UNION ALL
        SELECT other_vertex AS id, cid FROM need2update JOIN public.g_oe 
        ON need2update.id = g_oe.owner_vertex AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value 
        || ') AS t1 GROUP BY id DISTRIBUTED BY (id)';

        DROP TABLE IF EXISTS need2update;
        CREATE TEMP TABLE need2update AS
        SELECT wcc_t.id, MIN(wcc_msg.cid) AS cid FROM wcc_t JOIN wcc_msg 
        ON wcc_t.id = wcc_msg.id AND wcc_t.cid > wcc_msg.cid GROUP BY wcc_t.id
        DISTRIBUTED BY (id);

        DROP TABLE IF EXISTS wcc_msg;

        SELECT COUNT(1) INTO num2update FROM need2update;

        current_loop := current_loop + 1;

    END LOOP;

    DROP TABLE IF EXISTS need2update;

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS 
    SELECT id, cid AS attr FROM wcc_t DISTRIBUTED BY (id)';

    DROP TABLE wcc_t;

    RETURN current_loop;

END;
$$ LANGUAGE plpgsql;

