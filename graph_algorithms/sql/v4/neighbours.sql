CREATE OR REPLACE FUNCTION neighbours(
    IN vertex_id VARCHAR,
    IN k_hop INTEGER,
    IN result_type INTEGER, -- 0 for vertices, others for edges
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    current_hop INTEGER := 1;
    result_count INTEGER;
BEGIN

    -- create bfs edges table & insert in-edges
    CREATE TEMPORARY TABLE bfs_edges ON COMMIT DROP AS
    SELECT owner_vertex AS src_id, g_ie.other_vertex AS dst_id, -1 AS direction, 0 AS flag
    FROM public.g_ie WHERE g_ie.owner_vertex = vertex_id
    DISTRIBUTED BY (dst_id);

    -- insert out-edges
    INSERT INTO bfs_edges
    SELECT owner_vertex, g_oe.other_vertex, 1, 0
    FROM public.g_oe WHERE g_oe.owner_vertex = vertex_id;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP AS
    SELECT vertex_id AS id, 0 AS hop
    DISTRIBUTED BY (id);

    INSERT INTO discovered_vertices SELECT DISTINCT dst_id, current_hop FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 AND current_hop < k_hop LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_edges
        SELECT DISTINCT t1.src_id,  t1.dst_id, t1.direction, 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, g_oe.other_vertex AS dst_id, 1 AS direction FROM
        bfs_edges JOIN public.g_oe ON bfs_edges.flag = 1 AND bfs_edges.dst_id = g_oe.owner_vertex
        UNION ALL SELECT bfs_edges.dst_id AS src_id, g_ie.other_vertex AS dst_id, -1 AS direction FROM
        bfs_edges JOIN public.g_ie ON bfs_edges.flag = 1 AND bfs_edges.dst_id = g_ie.owner_vertex) AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.id
        WHERE discovered_vertices.id is NULL;

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        current_hop := current_hop + 1;
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id, current_hop FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

    IF result_type = 0 THEN
        EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS 
        SELECT id, hop AS attr FROM discovered_vertices DISTRIBUTED BY (id)';
        SELECT COUNT(1) INTO result_count FROM discovered_vertices;
    ELSE
        EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS 
        SELECT t1.src_id, t1.dst_id FROM 
        (SELECT src_id, dst_id FROM bfs_edges WHERE direction = 1 UNION ALL
        SELECT dst_id AS src_id, src_id AS dst_id FROM bfs_edges WHERE direction = -1) AS t1
        DISTRIBUTED BY (dst_id)';
        EXECUTE 'SELECT COUNT(1) FROM ' || result_table_name INTO result_count;
    END IF;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION neighbours(
    IN vertex_id VARCHAR,
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

    EXECUTE
    'CREATE TEMPORARY TABLE bfs_edges ON COMMIT DROP AS
    SELECT owner_vertex AS src_id, g_ie.other_vertex AS dst_id, -1 AS direction, 0 AS flag
    FROM public.g_ie WHERE g_ie.owner_vertex = ' || quote_literal(vertex_id) || 
    ' AND g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
    ' DISTRIBUTED BY (dst_id)';

    -- init bfs edges table
    EXECUTE
    'INSERT INTO bfs_edges
    SELECT owner_vertex, g_oe.other_vertex, 1, 0
    FROM public.g_oe WHERE g_oe.owner_vertex = ' || quote_literal(vertex_id) ||
    ' AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;


    CREATE TEMPORARY TABLE discovered_vertices
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP AS
    SELECT vertex_id AS id, 0 AS hop
    DISTRIBUTED BY (id);

    INSERT INTO discovered_vertices SELECT DISTINCT dst_id, current_hop FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 AND current_hop < k_hop LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        EXECUTE
        'INSERT INTO bfs_edges
        SELECT DISTINCT t1.src_id,  t1.dst_id, t1.direction, 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, g_oe.other_vertex AS dst_id, 1 AS direction FROM
        bfs_edges JOIN public.g_oe ON bfs_edges.flag = 1 
        AND bfs_edges.dst_id = g_oe.owner_vertex
        AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
        ' UNION ALL SELECT bfs_edges.dst_id AS src_id, g_ie.other_vertex AS dst_id, -1 AS direction FROM
        bfs_edges JOIN public.g_ie ON bfs_edges.flag = 1 
        AND bfs_edges.dst_id = g_ie.owner_vertex
        AND g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.id
        WHERE discovered_vertices.id is NULL';

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        current_hop := current_hop + 1;
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id, current_hop FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    END LOOP;

    IF result_type = 0 THEN
        EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS 
        SELECT id, hop AS attr FROM discovered_vertices DISTRIBUTED BY (id)';
        SELECT COUNT(1) INTO result_count FROM discovered_vertices;
    ELSE
        EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS 
        SELECT t1.src_id, t1.dst_id FROM 
        (SELECT src_id, dst_id FROM bfs_edges WHERE direction = 1 UNION ALL
        SELECT dst_id AS src_id, src_id AS dst_id FROM bfs_edges WHERE direction = -1) AS t1
        DISTRIBUTED BY (dst_id)';
        EXECUTE 'SELECT COUNT(1) FROM ' || result_table_name INTO result_count;
    END IF;

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;
