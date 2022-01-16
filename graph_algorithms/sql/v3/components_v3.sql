CREATE OR REPLACE FUNCTION public.scc_bfs_from(
    IN vertex_id VARCHAR(64)
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN

    -- init bfs edges table
    INSERT INTO bfs_edges
    SELECT vertex_id, edges.dst_id, 0 AS flag
    FROM public.edges WHERE edges.src_id = vertex_id;

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
        bfs_edges JOIN public.edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.src_id) AS t1
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


CREATE OR REPLACE FUNCTION public.scc_bfs_from(
    IN vertex_id VARCHAR(64),
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN

    -- init bfs edges table
    EXECUTE
    'INSERT INTO bfs_edges SELECT ''' || vertex_id || ''', edges.dst_id, 0 AS flag
    FROM public.edges WHERE edges.src_id = ''' || vertex_id ||
    ''' AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

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


CREATE OR REPLACE FUNCTION public.scc_reversed_bfs_from(
    IN vertex_id VARCHAR(64)
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN

    -- init bfs edges table
    INSERT INTO bfs_edges SELECT vertex_id, edges.src_id, 0 AS flag
    FROM public.edges WHERE edges.dst_id = vertex_id;

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
        bfs_edges JOIN public.edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.dst_id) AS t1
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


CREATE OR REPLACE FUNCTION public.scc_reversed_bfs_from(
    IN vertex_id VARCHAR(64),
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN

    -- init bfs edges table
    EXECUTE
    'INSERT INTO bfs_edges SELECT ''' || vertex_id || ''', edges.src_id, 0 AS flag
    FROM public.edges WHERE edges.dst_id = ''' || vertex_id ||
    ''' AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

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
        bfs_edges JOIN public.edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.dst_id 
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


CREATE OR REPLACE FUNCTION public.strongly_connected_components(
    IN vertex_id VARCHAR(64),
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    result_count INTEGER;
BEGIN
    -- create bfs edges table
    CREATE TEMPORARY TABLE bfs_edges(src_id VARCHAR(64), dst_id VARCHAR(64), flag INTEGER)
    ON COMMIT DROP DISTRIBUTED BY (dst_id);

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vid VARCHAR(64))
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

        -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices_reversed(vid VARCHAR(64))
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

    PERFORM scc_bfs_from(vertex_id);
    PERFORM scc_reversed_bfs_from(vertex_id);

    EXECUTE
    'CREATE TABLE ' || result_table_name || '(vid VARCHAR(64)) DISTRIBUTED BY (vid)';

    EXECUTE
    'INSERT INTO ' || result_table_name || ' SELECT discovered_vertices.vid
    FROM discovered_vertices JOIN discovered_vertices_reversed
    ON discovered_vertices.vid = discovered_vertices_reversed.vid';

    EXECUTE 'SELECT COUNT(1) FROM ' || result_table_name INTO result_count;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.strongly_connected_components(
    IN vertex_id VARCHAR(64),
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    result_count INTEGER;
BEGIN
    -- create bfs edges table
    CREATE TEMPORARY TABLE bfs_edges(src_id VARCHAR(64), dst_id VARCHAR(64), flag INTEGER)
    ON COMMIT DROP DISTRIBUTED BY (dst_id);

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vid VARCHAR(64))
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

        -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices_reversed(vid VARCHAR(64))
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

    PERFORM scc_bfs_from(vertex_id, edge_type_column, edge_type_value);
    PERFORM scc_reversed_bfs_from(vertex_id, edge_type_column, edge_type_value);

    EXECUTE
    'CREATE TABLE ' || result_table_name || '(vid VARCHAR(64)) DISTRIBUTED BY (vid)';

    EXECUTE
    'INSERT INTO ' || result_table_name || ' SELECT discovered_vertices.vid
    FROM discovered_vertices JOIN discovered_vertices_reversed
    ON discovered_vertices.vid = discovered_vertices_reversed.vid';

    EXECUTE 'SELECT COUNT(1) FROM ' || result_table_name INTO result_count;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.weakly_connected_components(
    IN vertex_id VARCHAR(64),
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    result_count INTEGER;
BEGIN

    -- create bfs edges table
    CREATE TEMPORARY TABLE bfs_edges(src_id VARCHAR(64), dst_id VARCHAR(64), flag INTEGER)
    ON COMMIT DROP DISTRIBUTED BY (dst_id);

    -- init bfs edges table
    INSERT INTO bfs_edges
    SELECT vertex_id, edges.src_id, 0 AS flag
    FROM public.edges WHERE edges.dst_id = vertex_id
    UNION SELECT vertex_id, edges.dst_id, 0 AS flag
    FROM public.edges WHERE edges.src_id = vertex_id;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vid VARCHAR(64))
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
    'CREATE TABLE ' || result_table_name || '(vid VARCHAR(64)) DISTRIBUTED BY (vid)';

    EXECUTE
    'INSERT INTO ' || result_table_name || ' SELECT vid FROM discovered_vertices';

    SELECT COUNT(1) INTO result_count FROM discovered_vertices;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.weakly_connected_components(
    IN vertex_id VARCHAR(64),
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    result_count INTEGER;
BEGIN

    -- create bfs edges table
    CREATE TEMPORARY TABLE bfs_edges(src_id VARCHAR(64), dst_id VARCHAR(64), flag INTEGER)
    ON COMMIT DROP DISTRIBUTED BY (dst_id);

    -- init bfs edges table
    EXECUTE
    'INSERT INTO bfs_edges
    SELECT ' || vertex_id || ', edges.src_id, 0 AS flag
    FROM public.edges WHERE edges.dst_id = ''' || vertex_id || ''' AND edges.' 
    || quote_ident(edge_type_column) || ' = ' || edge_type_value
    || ' UNION SELECT ' || vertex_id || ', edges.dst_id, 0 AS flag
    FROM public.edges WHERE edges.src_id = ''' || vertex_id || ''' AND edges.' 
    || quote_ident(edge_type_column) || ' = ' || edge_type_value;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vid VARCHAR(64))
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
    'CREATE TABLE ' || result_table_name || '(vid VARCHAR(64)) DISTRIBUTED BY (vid)';

    EXECUTE
    'INSERT INTO ' || result_table_name || ' SELECT vid FROM discovered_vertices';

    SELECT COUNT(1) INTO result_count FROM discovered_vertices;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;
