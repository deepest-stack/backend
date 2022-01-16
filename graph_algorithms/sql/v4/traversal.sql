-- dependency: utils.sql

CREATE OR REPLACE FUNCTION public.init_paths(
    IN sources VARCHAR[]
) RETURNS INTEGER AS $$
DECLARE
    count INTEGER;
BEGIN
    CREATE TEMPORARY TABLE paths(
        pathid SERIAL,
        path VARCHAR[],
        vertex_idx INTEGER[],
        id VARCHAR,
        flag INTEGER,
        loop_index INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (id);

    CREATE TEMPORARY TABLE newly_discovered_edges(
        src_id VARCHAR,
        dst_id VARCHAR
    ) ON COMMIT DROP DISTRIBUTED BY (src_id);
    
    CREATE TEMPORARY TABLE current_vertices(
        id VARCHAR
    ) ON COMMIT DROP DISTRIBUTED BY (id);

    -- init paths
    INSERT INTO paths(path, vertex_idx, id, flag, loop_index)
    SELECT ARRAY["unnest"], ARRAY[0], "unnest", 0, 0 FROM UNNEST(sources);

    SELECT COUNT(1) INTO count FROM paths WHERE flag = 0;
    UPDATE paths SET flag = 1 WHERE flag = 0;

    RETURN count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.init_paths_with_attr(
    IN sources VARCHAR[],
    IN attr_type VARCHAR
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
        edges_attr ' || attr_type || '[],
        flag INTEGER,
        loop_index INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (id)';

    EXECUTE
    'CREATE TEMPORARY TABLE newly_discovered_edges(
        src_id VARCHAR,
        dst_id VARCHAR,
        attr ' || attr_type || '
    ) ON COMMIT DROP DISTRIBUTED BY (src_id)';
    
    CREATE TEMPORARY TABLE current_vertices(
        id VARCHAR
    ) ON COMMIT DROP DISTRIBUTED BY (id);

    -- init paths
    INSERT INTO paths(path, vertex_idx, id, flag, loop_index)
    SELECT ARRAY["unnest"], ARRAY[0], "unnest", 0, 0 FROM UNNEST(sources);
    EXECUTE
    'UPDATE paths SET edges_attr=ARRAY[NULL::'||attr_type||']';

    SELECT COUNT(1) INTO count FROM paths WHERE flag = 0;
    UPDATE paths SET flag = 1 WHERE flag = 0;

    RETURN count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.paths_from(
    IN sources VARCHAR[], 
    IN max_loops INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
    result_count INTEGER;
BEGIN
    
    SELECT public.init_paths(sources) INTO num_growing_path;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP
        
        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT id FROM paths 
        WHERE paths.flag = 1 AND paths.id IS NOT NULL;
        
        TRUNCATE TABLE newly_discovered_edges;
        INSERT INTO newly_discovered_edges
        SELECT owner_vertex, other_vertex FROM current_vertices JOIN public.g_oe ON 
        current_vertices.id = g_oe.owner_vertex;

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vertex_idx, id, flag, loop_index)
         SELECT t2.path||t2.id, t2.vertex_idx||current_loop, t2.id, 0, current_loop FROM
        (SELECT t1.path, t1.vertex_idx, newly_discovered_edges.dst_id AS id
        FROM (SELECT path, vertex_idx, id FROM paths WHERE flag = 1 AND id IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.id = newly_discovered_edges.src_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND id IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE id IS NULL OR loop_index = current_loop;

    PERFORM public.parse_path(
        result_table_name, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path) FROM paths WHERE id IS NULL OR loop_index = ' || current_loop
        );

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.paths_from(
    IN sources VARCHAR[], 
    IN max_loops INTEGER, 
    IN attr_column VARCHAR,
    IN attr_type VARCHAR,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
    result_count INTEGER;
BEGIN

    SELECT public.init_paths_with_attr(sources, attr_type) INTO num_growing_path;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP
        
        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT id FROM paths 
        WHERE paths.flag = 1 AND paths.id IS NOT NULL;
        
        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT owner_vertex, other_vertex, (properties::json->>' || quote_literal(attr_column) || ')::' || attr_type ||
        ' FROM current_vertices JOIN public.g_oe ON current_vertices.id = g_oe.owner_vertex';

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vertex_idx, id, edges_attr, flag, loop_index)
        SELECT t2.path||t2.id, t2.vertex_idx||current_loop, t2.id, t2.edges_attr||t2.attr, 0, current_loop FROM
        (SELECT t1.path, t1.vertex_idx, newly_discovered_edges.dst_id AS id, t1.edges_attr, newly_discovered_edges.attr
        FROM (SELECT path, vertex_idx, id, edges_attr FROM paths WHERE flag = 1 AND id IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.id = newly_discovered_edges.src_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND id IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE id IS NULL OR loop_index = current_loop;

    PERFORM public.parse_path_with_attr(
        result_table_name, 
        attr_type, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path), UNNEST(edges_attr) FROM paths WHERE id IS NULL OR loop_index = ' || current_loop
        );

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.paths_from(
    IN sources VARCHAR[], 
    IN max_loops INTEGER, 
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
    result_count INTEGER;
BEGIN

    SELECT public.init_paths(sources) INTO num_growing_path;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP

        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT id FROM paths 
        WHERE paths.flag = 1 AND paths.id IS NOT NULL;
        
        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT owner_vertex, other_vertex FROM current_vertices JOIN public.g_oe 
        ON current_vertices.id = g_oe.owner_vertex AND g_oe.'
        || quote_ident(edge_type_column) || ' = ' || edge_type_value;

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vertex_idx, id, flag, loop_index)
        SELECT t2.path||t2.id, t2.vertex_idx||current_loop, t2.id, 0, current_loop FROM
        (SELECT t1.path, t1.vertex_idx, newly_discovered_edges.dst_id AS id
        FROM (SELECT path, vertex_idx, id FROM paths WHERE flag = 1 AND id IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.id = newly_discovered_edges.src_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND id IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE id IS NULL OR loop_index = current_loop;

    PERFORM public.parse_path(
        result_table_name, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path) FROM paths WHERE id IS NULL OR loop_index = ' || current_loop
        );

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.paths_from(
    IN sources VARCHAR[], 
    IN max_loops INTEGER, 
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER, 
    IN attr_column VARCHAR,
    IN attr_type VARCHAR,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
    result_count INTEGER;
BEGIN

    SELECT public.init_paths_with_attr(sources, attr_type) INTO num_growing_path;
    
    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP

        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT id FROM paths 
        WHERE paths.flag = 1 AND paths.id IS NOT NULL;

        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT owner_vertex, other_vertex, (properties::json->>' || quote_literal(attr_column) || ')::' || attr_type ||
        ' FROM public.g_oe JOIN current_vertices ON current_vertices.id = g_oe.owner_vertex AND g_oe.'
        || quote_ident(edge_type_column) || ' = ' || edge_type_value;

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vertex_idx, id, edges_attr, flag, loop_index)
        SELECT t2.path||t2.id, t2.vertex_idx||current_loop , t2.id, t2.edges_attr||t2.attr, 0, current_loop FROM
        (SELECT t1.path, t1.vertex_idx, newly_discovered_edges.dst_id AS id, t1.edges_attr, newly_discovered_edges.attr
        FROM (SELECT path, vertex_idx, id, edges_attr FROM paths WHERE flag = 1 AND id IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.id = newly_discovered_edges.src_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND id IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE id IS NULL OR loop_index = current_loop;

    PERFORM public.parse_path_with_attr(
        result_table_name, 
        attr_type, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path), UNNEST(edges_attr) FROM paths WHERE id IS NULL OR loop_index = ' || current_loop
        );

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.backtrack(
    IN sources VARCHAR[], 
    IN max_loops INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
    result_count INTEGER;
BEGIN

    SELECT public.init_paths(sources) INTO num_growing_path;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP
        
        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT id FROM paths 
        WHERE paths.flag = 1 AND paths.id IS NOT NULL;
        
        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT other_vertex, owner_vertex FROM current_vertices JOIN public.g_ie 
        ON current_vertices.id = g_ie.owner_vertex';

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vertex_idx, id, flag, loop_index) 
        SELECT t2.path||t2.id, t2.vertex_idx||current_loop, t2.id, 0, current_loop FROM
        (SELECT t1.path, t1.vertex_idx, newly_discovered_edges.src_id AS id
        FROM (SELECT path, vertex_idx, id FROM paths WHERE flag = 1 AND id IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.id = newly_discovered_edges.dst_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND id IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE id IS NULL OR loop_index = current_loop;

    PERFORM public.parse_path(
        result_table_name, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path) FROM paths WHERE id IS NULL OR loop_index = ' || current_loop
        );

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.backtrack(
    IN sources VARCHAR[], 
    IN max_loops INTEGER, 
    IN attr_column VARCHAR,
    IN attr_type VARCHAR,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
    result_count INTEGER;
BEGIN

    SELECT public.init_paths_with_attr(sources, attr_type) INTO num_growing_path;
    
    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP
        
        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT id FROM paths 
        WHERE paths.flag = 1 AND paths.id IS NOT NULL;
        
        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT other_vertex, owner_vertex , (properties::json->>' || quote_literal(attr_column) || ')::' || attr_type ||
        ' FROM current_vertices JOIN public.g_ie 
        ON current_vertices.id = g_ie.owner_vertex';

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vertex_idx, id, edges_attr, flag, loop_index) 
        SELECT t2.path||t2.id, t2.vertex_idx||current_loop, t2.id, t2.edges_attr||t2.attr, 0, current_loop FROM
        (SELECT t1.path, t1.vertex_idx, newly_discovered_edges.src_id AS id, t1.edges_attr, newly_discovered_edges.attr
        FROM (SELECT path, vertex_idx, id, edges_attr FROM paths WHERE flag = 1 AND id IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.id = newly_discovered_edges.dst_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND id IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE id IS NULL OR loop_index = current_loop;

    PERFORM public.parse_path_with_attr(
        result_table_name, 
        attr_type, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path), UNNEST(edges_attr) FROM paths WHERE id IS NULL OR loop_index = ' || current_loop
        );

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.backtrack(
    IN sources VARCHAR[], 
    IN max_loops INTEGER, 
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
    result_count INTEGER;
BEGIN

    SELECT public.init_paths(sources) INTO num_growing_path;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP

        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT id FROM paths 
        WHERE paths.flag = 1 AND paths.id IS NOT NULL;
        
        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT other_vertex, owner_vertex FROM current_vertices JOIN public.g_ie 
        ON current_vertices.id = g_ie.owner_vertex AND g_ie.'
        || quote_ident(edge_type_column) || ' = ' || edge_type_value;

        current_loop := current_loop + 1 ;
        INSERT INTO paths(path, vertex_idx, id, flag, loop_index)
        SELECT t2.path||t2.id, t2.vertex_idx||current_loop, t2.id, 0, current_loop FROM
        (SELECT t1.path, t1.vertex_idx, newly_discovered_edges.src_id AS id
        FROM (SELECT path, vertex_idx, id FROM paths WHERE flag = 1 AND id IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.id = newly_discovered_edges.dst_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND id IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE id IS NULL OR loop_index = current_loop;

    PERFORM public.parse_path(
        result_table_name, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path) FROM paths WHERE id IS NULL OR loop_index = ' || current_loop
        );

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.backtrack(
    IN sources VARCHAR[], 
    IN max_loops INTEGER, 
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER, 
    IN attr_column VARCHAR,
    IN attr_type VARCHAR,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
    result_count INTEGER;
BEGIN

    SELECT public.init_paths_with_attr(sources, attr_type) INTO num_growing_path;
    
    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP

        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT id FROM paths 
        WHERE paths.flag = 1 AND paths.id IS NOT NULL;

        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT other_vertex, owner_vertex , (properties::json->>' || quote_literal(attr_column) || ')::' || attr_type ||
        ' FROM public.g_ie JOIN current_vertices ON g_ie.owner_vertex = current_vertices.id 
        AND g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vertex_idx, id, edges_attr, flag, loop_index)
        SELECT t2.path||t2.id, t2.vertex_idx||current_loop, t2.id, t2.edges_attr||t2.attr, 0, current_loop FROM
        (SELECT t1.path, t1.vertex_idx, newly_discovered_edges.src_id AS id, t1.edges_attr, newly_discovered_edges.attr
        FROM (SELECT path, vertex_idx, id, edges_attr FROM paths WHERE flag = 1 AND id IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.id = newly_discovered_edges.dst_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND id IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE id IS NULL OR loop_index = current_loop;

    PERFORM public.parse_path_with_attr(
        result_table_name, 
        attr_type, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path), UNNEST(edges_attr) FROM paths WHERE id IS NULL OR loop_index = ' || current_loop
        );

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.bfs_from(
    IN source VARCHAR, 
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    current_level INTEGER := 1;
    result_count INTEGER;
BEGIN
    -- create bfs edges table
    -- init bfs edges table
    CREATE TEMPORARY TABLE bfs_edges ON COMMIT DROP AS
    SELECT g_oe.owner_vertex AS src_id, g_oe.other_vertex AS dst_id, 0 AS level, 0 AS flag
    FROM public.g_oe WHERE g_oe.owner_vertex = source
    DISTRIBUTED BY (dst_id);

    -- create discovered vertices table
    -- init discovered_vertices
    CREATE TEMPORARY TABLE discovered_vertices
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP 
    AS SELECT source AS id DISTRIBUTED BY (id);
    INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_edges
        SELECT DISTINCT t1.src_id,  t1.dst_id, current_level, 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, g_oe.other_vertex AS dst_id FROM
        bfs_edges JOIN public.g_oe ON bfs_edges.flag = 1 AND bfs_edges.dst_id = g_oe.owner_vertex) AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.id
        WHERE discovered_vertices.id is NULL;

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        -- increase level
        current_level := current_level + 1;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;
    END LOOP;

    SELECT COUNT(1) INTO result_count FROM bfs_edges;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS 
    SELECT src_id, dst_id, level AS attr FROM bfs_edges DISTRIBUTED BY (src_id)';
    
    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.bfs_from(
    IN source VARCHAR,
    IN edge_type_column VARCHAR,
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    current_level INTEGER := 1;
    result_count INTEGER;
BEGIN
    -- create bfs edges table
    -- init bfs edges table
    EXECUTE
    'CREATE TEMPORARY TABLE bfs_edges ON COMMIT DROP AS 
    SELECT g_oe.owner_vertex AS src_id, g_oe.other_vertex AS dst_id, 0 AS level, 0 AS flag
    FROM public.g_oe WHERE g_oe.owner_vertex = ' || quote_literal(source) ||
    ' AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value
    || ' DISTRIBUTED BY (dst_id)';

    -- create discovered vertices table
    -- init discovered_vertices
    CREATE TEMPORARY TABLE discovered_vertices
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP 
    AS SELECT source AS id DISTRIBUTED BY (id);
    INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        EXECUTE
        'INSERT INTO bfs_edges
        SELECT DISTINCT t1.src_id,  t1.dst_id, ' || current_level || ', 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, g_oe.other_vertex AS dst_id FROM
        bfs_edges JOIN public.g_oe ON bfs_edges.flag = 1 
        AND bfs_edges.dst_id = g_oe.owner_vertex 
        AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.id
        WHERE discovered_vertices.id is NULL';

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        -- increase level
        current_level := current_level + 1;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;
    END LOOP;

    SELECT COUNT(1) INTO result_count FROM bfs_edges;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS 
    SELECT src_id, dst_id, level AS attr FROM bfs_edges DISTRIBUTED BY (src_id)';

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.meta_path_search(
    IN meta_path_def VARCHAR[],
    IN edge_orientation INTEGER[],
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    current_loop INTEGER := 1;
    result_count INTEGER;
BEGIN

    CREATE TEMPORARY TABLE paths(
        pathid SERIAL,
        path VARCHAR[],
        vertex_idx INTEGER[],
        id VARCHAR,
        flag INTEGER,
        loop_index INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (id);
    EXECUTE
    'INSERT INTO paths(path, vertex_idx, id, flag, loop_index)
    SELECT ARRAY[id], ARRAY[0], id, 0, ' || current_loop || ' FROM public.g_v
    WHERE ' || meta_path_def[1];

    FOR i IN 2..ARRAY_LENGTH(meta_path_def, 1) LOOP
        
        current_loop := i;

        -- check whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
        EXIT WHEN num_growing_path <= 0;

        UPDATE paths SET flag = 1 WHERE flag = 0;

        IF current_loop % 2 = 0 THEN
        -- constraint on edge
            IF edge_orientation[current_loop/2] = 1 THEN
                EXECUTE
                'INSERT INTO paths(path, vertex_idx, id, flag, loop_index)
                SELECT paths.path, paths.vertex_idx, g_oe.other_vertex, 0, ' || current_loop || ' FROM paths JOIN public.g_oe 
                ON paths.id = g_oe.owner_vertex AND paths.flag = 1 AND ' || meta_path_def[current_loop];
            ELSE
                EXECUTE
                'INSERT INTO paths(path, vertex_idx, id, flag, loop_index)
                SELECT paths.path, paths.vertex_idx, g_ie.other_vertex, 0, ' || current_loop || ' FROM paths JOIN public.g_ie 
                ON paths.id = g_ie.owner_vertex AND paths.flag = 1 AND ' || meta_path_def[current_loop];
            END IF;
        ELSE
        -- constraint on vertex
            EXECUTE
            'INSERT INTO paths(path, vertex_idx, id, flag, loop_index)
            SELECT paths.path||g_v.id, paths.vertex_idx||' || current_loop/2 || ' , g_v.id, 0, ' || current_loop || ' FROM paths JOIN public.g_v 
            ON paths.id = g_v.id AND paths.flag = 1 AND ' || meta_path_def[current_loop];
        END IF;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;


    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE loop_index = current_loop;

    PERFORM public.parse_path(
        result_table_name, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path) AS id FROM paths WHERE loop_index = ' || current_loop
        );

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;
