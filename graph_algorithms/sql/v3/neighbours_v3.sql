CREATE OR REPLACE FUNCTION public.neighbours(
    IN vertex_id VARCHAR(64),
    IN k_hop INTEGER,
    IN result_type INTEGER, -- 0 for vertices, others for edges
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    current_hop INTEGER := 1;
    result_count INTEGER;
BEGIN

    -- create bfs edges table
    CREATE TEMPORARY TABLE bfs_edges(src_id VARCHAR(64), dst_id VARCHAR(64), direction INTEGER, flag INTEGER)
    ON COMMIT DROP DISTRIBUTED BY (dst_id);

    -- init bfs edges table
    INSERT INTO bfs_edges
    SELECT vertex_id, edges.src_id, -1, 0 AS flag
    FROM public.edges WHERE edges.dst_id = vertex_id
    UNION SELECT vertex_id, edges.dst_id, 1, 0 AS flag
    FROM public.edges WHERE edges.src_id = vertex_id;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vid VARCHAR(64), hop INTEGER)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

    -- init discovered_vertices
    INSERT INTO discovered_vertices VALUES (vertex_id, 0);
    INSERT INTO discovered_vertices SELECT DISTINCT dst_id, current_hop FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 AND current_hop < k_hop LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_edges
        SELECT DISTINCT t1.src_id,  t1.dst_id, t1.direction, 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, edges.dst_id, 1 AS direction FROM
        bfs_edges JOIN public.edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.src_id
        UNION SELECT bfs_edges.dst_id AS src_id, edges.src_id AS dst_id, -1 AS direction FROM
        bfs_edges JOIN public.edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.dst_id) AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.vid
        WHERE discovered_vertices.vid is NULL;

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        current_hop := current_hop + 1;
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id, current_hop FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

    IF result_type = 0 THEN
        EXECUTE 'CREATE TABLE ' || result_table_name || '(vid VARCHAR(64), attr INTEGER) DISTRIBUTED BY (vid)';
        EXECUTE 'INSERT INTO ' || result_table_name || ' SELECT vid, hop FROM discovered_vertices';
        SELECT COUNT(1) INTO result_count FROM discovered_vertices;
    ELSE
        EXECUTE 'CREATE TABLE ' || result_table_name || '(src_id VARCHAR(64), dst_id VARCHAR(64)) DISTRIBUTED BY (dst_id)';
        EXECUTE 'INSERT INTO ' || result_table_name || ' SELECT t1.src_id, t1.dst_id FROM 
        (SELECT src_id, dst_id FROM bfs_edges WHERE direction = 1 UNION
        SELECT dst_id AS src_id, src_id AS dst_id FROM bfs_edges WHERE direction = -1) AS t1';
        EXECUTE 'SELECT COUNT(1) FROM ' || result_table_name INTO result_count;
    END IF;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.neighbours(
    IN vertex_id VARCHAR(64),
    IN k_hop INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_type INTEGER, -- 0 for vertices, others for edges
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    current_hop INTEGER := 1;
    result_count INTEGER;
BEGIN

    -- create bfs edges table
    CREATE TEMPORARY TABLE bfs_edges(src_id VARCHAR(64), dst_id VARCHAR(64),  direction INTEGER, flag INTEGER)
    ON COMMIT DROP DISTRIBUTED BY (dst_id);

    -- init bfs edges table
    EXECUTE
    'INSERT INTO bfs_edges
    SELECT ' || vertex_id || ', edges.src_id, -1, 0 AS flag
    FROM public.edges WHERE edges.dst_id = ''' || vertex_id || ''' AND edges.' 
    || quote_ident(edge_type_column) || ' = ' || edge_type_value
    || ' UNION SELECT ' || vertex_id || ', edges.dst_id, 1, 0 AS flag
    FROM public.edges WHERE edges.src_id = ''' || vertex_id || ''' AND edges.' 
    || quote_ident(edge_type_column) || ' = ' || edge_type_value;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vid VARCHAR(64), hop INTEGER)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

    -- init discovered_vertices
    INSERT INTO discovered_vertices VALUES (vertex_id, 0);
    INSERT INTO discovered_vertices SELECT DISTINCT dst_id, current_hop FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 AND current_hop < k_hop LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        EXECUTE
        'INSERT INTO bfs_edges SELECT DISTINCT t1.src_id,  t1.dst_id, t1.direction, 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, edges.dst_id ,1 AS direction FROM
        bfs_edges JOIN edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.src_id 
        AND public.edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value
        || ' UNION SELECT bfs_edges.dst_id AS src_id, edges.src_id AS dst_id, -1 AS direction FROM
        bfs_edges JOIN edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.dst_id 
        AND public.edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.vid
        WHERE discovered_vertices.vid is NULL';

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        current_hop := current_hop + 1;
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id, current_hop FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

    IF result_type = 0 THEN
        EXECUTE 'CREATE TABLE ' || result_table_name || '(vid VARCHAR(64), attr INTEGER) DISTRIBUTED BY (vid)';
        EXECUTE 'INSERT INTO ' || result_table_name || ' SELECT vid, hop FROM discovered_vertices';
        SELECT COUNT(1) INTO result_count FROM discovered_vertices;
    ELSE
        EXECUTE 'CREATE TABLE ' || result_table_name || '(src_id VARCHAR(64), dst_id VARCHAR(64)) DISTRIBUTED BY (dst_id)';
        EXECUTE 'INSERT INTO ' || result_table_name || ' SELECT t1.src_id, t1.dst_id FROM 
        (SELECT src_id, dst_id FROM bfs_edges WHERE direction = 1 UNION
        SELECT dst_id AS src_id, src_id AS dst_id FROM bfs_edges WHERE direction = -1) AS t1';
        EXECUTE 'SELECT COUNT(1) FROM ' || result_table_name INTO result_count;
    END IF;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;
