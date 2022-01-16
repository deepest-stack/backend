-- dependency: utils.sql

CREATE OR REPLACE FUNCTION public.init_shortest_paths(
    IN source VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    count INTEGER;
BEGIN
    CREATE TEMPORARY TABLE paths(
        pathid SERIAL,
        path VARCHAR[],
        vertex_idx INTEGER[],
        id VARCHAR,
        flag INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (id);

    CREATE TEMPORARY TABLE discovered_vertices(
        id VARCHAR
    ) WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (id);

    -- init paths
    INSERT INTO paths(path, vertex_idx, id, flag) VALUES (ARRAY[source], ARRAY[0], source, 0);

    INSERT INTO discovered_vertices VALUES(source);
    SELECT COUNT(1) INTO count FROM paths WHERE flag = 0;
    UPDATE paths SET flag = 1 WHERE flag = 0;

    RETURN count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.init_shortest_weighted_paths(
    IN source VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    count INTEGER;
BEGIN
    EXECUTE
    'CREATE TEMPORARY TABLE paths(
        pathid SERIAL,
        path VARCHAR[],
        vertex_idx INTEGER[],
        id VARCHAR,
        edges_attr NUMERIC[],
        weight NUMERIC,
        flag INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (id);

    CREATE TEMPORARY TABLE discovered_vertices(
        id VARCHAR
    ) WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (id)';

    -- init paths
    INSERT INTO paths(path, vertex_idx, id, edges_attr, weight, flag) VALUES (ARRAY[source], ARRAY[0], source, ARRAY[0.0], 0.0, 0);

    INSERT INTO discovered_vertices VALUES(source);

    SELECT COUNT(1) INTO count FROM paths WHERE flag = 0;
    UPDATE paths SET flag = 1 WHERE flag = 0;

    RETURN count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.shortest_path(
    IN source VARCHAR, 
    IN target VARCHAR,
    IN max_length INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    target_reached INTEGER := 0;
    current_loop INTEGER := 1;
    result_count INTEGER;
BEGIN

    SELECT public.init_shortest_paths(source) INTO num_growing_path;
    
    SELECT COUNT(1) INTO target_reached FROM paths WHERE id = target;

    WHILE num_growing_path > 0 AND target_reached = 0 AND current_loop <= max_length LOOP

        INSERT INTO paths(path, vertex_idx, id, flag)
        SELECT t1.path||t1.dst_id, t1.vertex_idx||current_loop, t1.dst_id, 0 FROM
        (SELECT paths.path, paths.vertex_idx, g_oe.other_vertex AS dst_id FROM
        paths JOIN public.g_oe ON paths.id = g_oe.owner_vertex AND paths.flag = 1) AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.id
        WHERE discovered_vertices.id is NULL;

        INSERT INTO discovered_vertices
        SELECT DISTINCT id FROM paths WHERE flag = 0;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- check whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
        -- check whether target has reached
        SELECT COUNT(1) INTO target_reached FROM paths WHERE flag = 0 AND id = target;
        UPDATE paths SET flag = 1 WHERE flag = 0;
        current_loop := current_loop + 1;

    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE id = target;
    PERFORM public.parse_path(
        result_table_name, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path) FROM paths WHERE id = ' || quote_literal(target)
        );
    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.shortest_path(
    IN source VARCHAR, 
    IN target VARCHAR, 
    IN max_length INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    target_reached INTEGER := 0;
    current_loop INTEGER := 1;
    result_count INTEGER;
BEGIN

    SELECT public.init_shortest_paths(source) INTO num_growing_path;
    
    SELECT COUNT(1) INTO target_reached FROM paths WHERE id = target;

    WHILE num_growing_path > 0 AND target_reached = 0 AND current_loop < max_length LOOP
        
        EXECUTE
        'INSERT INTO paths(path, vertex_idx, id, flag)
         SELECT t1.path||t1.dst_id, t1.vertex_idx||' || current_loop || ', t1.dst_id, 0 FROM
        (SELECT paths.path, paths.vertex_idx, g_oe.other_vertex AS dst_id FROM
        paths JOIN public.g_oe ON paths.id = g_oe.owner_vertex AND paths.flag = 1 AND g_oe.'
        || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.id
        WHERE discovered_vertices.id is NULL';

        INSERT INTO discovered_vertices
        SELECT DISTINCT id FROM paths WHERE flag = 0;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- check whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
        -- check whether target has reached
        SELECT COUNT(1) INTO target_reached FROM paths WHERE flag = 0 AND id = target;
        UPDATE paths SET flag = 1 WHERE flag = 0;
        current_loop := current_loop + 1;

    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE id = target;
    PERFORM public.parse_path(
        result_table_name, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path) FROM paths WHERE id = ' || quote_literal(target)
        );
    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.shortest_weighted_path(
    IN source VARCHAR, 
    IN target VARCHAR, 
    IN max_length INTEGER,
    IN weight_column VARCHAR,
    IN result_table_name VARCHAR
) RETURNS NUMERIC AS $$
DECLARE
    num_growing_path INTEGER;
    current_loop INTEGER := 1;
    min_weight NUMERIC;
BEGIN
    SELECT public.init_shortest_weighted_paths(source) INTO num_growing_path;
    WHILE num_growing_path > 0 AND current_loop <= max_length LOOP

        EXECUTE        
        'INSERT INTO paths(path, vertex_idx, id, edges_attr, weight, flag)
         SELECT 
            t1.path||t1.dst_id, 
            t1.vertex_idx||' || current_loop || ', 
            t1.dst_id, 
            t1.edges_attr||t1.weight, 
            t1.acc_weight+t1.weight, 
            0 
        FROM
        (SELECT 
            paths.path, 
            paths.vertex_idx, 
            paths.edges_attr, 
            paths.weight AS acc_weight, 
            g_oe.other_vertex AS dst_id, 
            (g_oe.properties::json->>' || quote_literal(weight_column) || ')::NUMERIC AS weight 
        FROM paths JOIN public.g_oe ON paths.id = g_oe.owner_vertex AND paths.flag = 1
        AND paths.id <> ' || quote_literal(target) || ') AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.id
        WHERE discovered_vertices.id is NULL';
        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- check whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
        UPDATE paths SET flag = 1 WHERE flag = 0;
        current_loop := current_loop + 1;

    END LOOP;

    SELECT weight INTO min_weight FROM paths WHERE id = target ORDER BY weight LIMIT 1;
    IF min_weight IS NULL THEN
        min_weight := 0.0;
        EXECUTE
        'CREATE TABLE ' || result_table_name || '(
        pathid INTEGER, 
        edge_idx INTEGER, 
        src_id VARCHAR, 
        dst_id VARCHAR, 
        attr NUMERIC) DISTRIBUTED BY (pathid)';
    ELSE
        PERFORM public.parse_path_with_attr(
            result_table_name, 
            'NUMERIC',
            'SELECT pathid, UNNEST(vertex_idx), UNNEST(path), UNNEST(edges_attr) FROM paths WHERE id = ' || quote_literal(target) || ' AND weight = ' || min_weight
        );
    END IF;
    
    RETURN min_weight;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.shortest_weighted_path(
    IN source VARCHAR, 
    IN target VARCHAR, 
    IN max_length INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN weight_column VARCHAR,
    IN result_table_name VARCHAR
) RETURNS NUMERIC AS $$
DECLARE
    num_growing_path INTEGER;
    current_loop INTEGER := 1;
    min_weight NUMERIC := 0.0;
BEGIN

    SELECT public.init_shortest_weighted_paths(source) INTO num_growing_path;

    WHILE num_growing_path > 0 AND current_loop <= max_length LOOP

        EXECUTE        
        'INSERT INTO paths(path, vertex_idx, id, edges_attr, weight, flag) 
        SELECT 
            t1.path||t1.dst_id, 
            t1.vertex_idx||' || current_loop || ', 
            t1.dst_id, 
            t1.edges_attr||t1.weight, 
            t1.acc_weight+t1.weight, 
            0 
        FROM
        (SELECT 
            paths.path, 
            paths.vertex_idx, 
            paths.edges_attr, 
            paths.weight AS acc_weight, 
            g_oe.other_vertex AS dst_id, 
            (g_oe.properties::json->>' || quote_literal(weight_column) || ')::NUMERIC AS weight 
        FROM paths JOIN public.g_oe ON paths.id = g_oe.owner_vertex AND paths.flag = 1
        AND paths.id <> ' || quote_literal(target) || 
        ' AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.id
        WHERE discovered_vertices.id is NULL';
        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- check whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
        UPDATE paths SET flag = 1 WHERE flag = 0;
        current_loop := current_loop + 1;

    END LOOP;

    SELECT weight INTO min_weight FROM paths WHERE id = target ORDER BY weight LIMIT 1;
    IF min_weight IS NULL THEN
        min_weight := 0.0;
        -- create an empty result table 
        EXECUTE
        'CREATE TABLE ' || result_table_name || '(
        pathid INTEGER, 
        edge_idx INTEGER, 
        src_id VARCHAR, 
        dst_id VARCHAR, 
        attr NUMERIC) DISTRIBUTED BY (pathid)';
    ELSE
        PERFORM public.parse_path_with_attr(
            result_table_name, 
            'NUMERIC',
            'SELECT pathid, UNNEST(vertex_idx), UNNEST(path), UNNEST(edges_attr) FROM paths WHERE id = ' || quote_literal(target) || ' AND weight = ' || min_weight
        );
    END IF;
    
    RETURN min_weight;

END;
$$ LANGUAGE plpgsql;
