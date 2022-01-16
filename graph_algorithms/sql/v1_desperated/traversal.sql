CREATE OR REPLACE FUNCTION indexof(
    IN arr INTEGER[], 
    IN ele INTEGER
) RETURNS INTEGER AS $$
BEGIN
    FOR i IN 1..array_length(arr, 1) LOOP
        IF arr[i] = ele THEN
            RETURN i;
        END IF;
    END LOOP;
    RETURN 0;
END;
$$ LANGUAGE plpgsql immutable;


CREATE OR REPLACE FUNCTION init_paths(
    IN sources INTEGER[],
    OUT count INTEGER
) AS $$
BEGIN
    CREATE TEMPORARY TABLE paths(
        pathid SERIAL,
        path INTEGER[],
        vid int,
        flag INTEGER,
        loop_index INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (vid);

    CREATE TEMPORARY TABLE newly_discovered_edges(
        src_id INTEGER,
        dst_id INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (src_id);
    
    CREATE TEMPORARY TABLE current_vertices(
        vid INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (vid);

    -- init paths
    INSERT INTO paths(path, vid, flag, loop_index)
    SELECT ARRAY["unnest"], "unnest", 0, 0 FROM UNNEST(sources);

    SELECT COUNT(1) INTO count FROM paths WHERE flag = 0;
    UPDATE paths SET flag = 1 WHERE flag = 0;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION init_paths_with_attr(
    IN sources INTEGER[],
    OUT count INTEGER
) AS $$
BEGIN
    CREATE TEMPORARY TABLE paths(
        pathid SERIAL,
        path INTEGER[],
        vid INTEGER,
        edges_attr NUMERIC[],
        flag INTEGER,
        loop_index INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (vid);

    CREATE TEMPORARY TABLE newly_discovered_edges(
        src_id INTEGER,
        dst_id INTEGER,
        attr NUMERIC
    ) ON COMMIT DROP DISTRIBUTED BY (src_id);
    
    CREATE TEMPORARY TABLE current_vertices(
        vid INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (vid);

    -- init paths
    -- insert empty array into field `edges_attr`
    INSERT INTO paths(path, vid, edges_attr, flag, loop_index)
    SELECT ARRAY["unnest"], "unnest", ARRAY[NULL]::NUMERIC[], 0, 0 FROM UNNEST(sources);

    SELECT COUNT(1) INTO count FROM paths WHERE flag = 0;
    UPDATE paths SET flag = 1 WHERE flag = 0;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION paths_from(
    IN sources INTEGER[], 
    IN max_loops INTEGER,
    paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
BEGIN

    SELECT "count" INTO num_growing_path FROM init_paths(sources) LIMIT 1;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP
        
        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT vid FROM paths 
        WHERE paths.flag = 1 AND paths.vid IS NOT NULL;
        
        TRUNCATE TABLE newly_discovered_edges;
        INSERT INTO newly_discovered_edges
        SELECT src_id, dst_id FROM current_vertices JOIN public.edges ON 
        current_vertices.vid = edges.src_id;

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vid, flag, loop_index)
         SELECT t2.path||t2.vid, t2.vid, 0, current_loop FROM
        (SELECT t1.path, newly_discovered_edges.dst_id AS vid
        FROM (SELECT path, vid FROM paths WHERE flag = 1 AND vid IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.vid = newly_discovered_edges.src_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND vid IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    OPEN paths_return FOR SELECT path FROM paths WHERE vid IS NULL OR loop_index = current_loop;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION paths_from(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN attr_column VARCHAR,
    paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
    dummy NUMERIC[] := ARRAY[1];
BEGIN

    SELECT "count" INTO num_growing_path FROM init_paths_with_attr(sources) LIMIT 1;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP
        
        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT vid FROM paths 
        WHERE paths.flag = 1 AND paths.vid IS NOT NULL;
        
        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT src_id, dst_id, ' || quote_ident(attr_column) || 
        ' FROM current_vertices JOIN public.edges ON current_vertices.vid = edges.src_id';

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vid, edges_attr, flag, loop_index)
        SELECT t2.path||t2.vid, t2.vid, array_append(t2.edges_attr, t2.attr), 0, current_loop FROM
        (SELECT t1.path, newly_discovered_edges.dst_id AS vid, t1.edges_attr, newly_discovered_edges.attr
        FROM (SELECT path, vid, edges_attr FROM paths WHERE flag = 1 AND vid IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.vid = newly_discovered_edges.src_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND vid IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    OPEN paths_return FOR SELECT path, edges_attr FROM paths WHERE vid IS NULL OR loop_index = current_loop;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION paths_from(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
BEGIN

    SELECT "count" INTO num_growing_path FROM init_paths(sources) LIMIT 1;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP

        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT vid FROM paths 
        WHERE paths.flag = 1 AND paths.vid IS NOT NULL;
        
        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT src_id, dst_id FROM current_vertices JOIN public.edges 
        ON current_vertices.vid = edges.src_id AND edges.'
        || quote_ident(edge_type_column) || ' = ' || edge_type_value;

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vid, flag, loop_index)
        SELECT t2.path||t2.vid, t2.vid, 0, current_loop FROM
        (SELECT t1.path, newly_discovered_edges.dst_id AS vid
        FROM (SELECT path, vid FROM paths WHERE flag = 1 AND vid IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.vid = newly_discovered_edges.src_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND vid IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    OPEN paths_return FOR SELECT path FROM paths WHERE vid IS NULL OR loop_index = current_loop;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION paths_from(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER, 
    IN attr_column VARCHAR,
    paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
    dummy NUMERIC[] := ARRAY[1];
BEGIN

    SELECT "count" INTO num_growing_path FROM init_paths_with_attr(sources) LIMIT 1;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP

        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT vid FROM paths 
        WHERE paths.flag = 1 AND paths.vid IS NOT NULL;

        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT src_id, dst_id, ' || quote_ident(attr_column) || ' FROM edges JOIN 
        current_vertices ON current_vertices.vid = edges.src_id AND public.edges.'
        || quote_ident(edge_type_column) || ' = ' || edge_type_value;

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vid, edges_attr, flag, loop_index)
        SELECT t2.path||t2.vid, t2.vid, ARRAY_APPEND(t2.edges_attr, t2.attr), 0, current_loop FROM
        (SELECT t1.path, newly_discovered_edges.dst_id AS vid, t1.edges_attr, newly_discovered_edges.attr
        FROM (SELECT path, vid, edges_attr FROM paths WHERE flag = 1 AND vid IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.vid = newly_discovered_edges.src_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND vid IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    OPEN paths_return FOR SELECT path, edges_attr FROM paths WHERE vid IS NULL OR loop_index = current_loop;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION backtrack(
    IN sources INTEGER[], 
    IN max_loops INTEGER,
    paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
BEGIN

    SELECT "count" INTO num_growing_path FROM init_paths(sources) LIMIT 1;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP
        
        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT vid FROM paths 
        WHERE paths.flag = 1 AND paths.vid IS NOT NULL;
        
        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT src_id, dst_id FROM current_vertices JOIN public.edges 
        ON current_vertices.vid = edges.dst_id';

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vid, flag, loop_index) 
        SELECT t2.path||t2.vid, t2.vid, 0, current_loop FROM
        (SELECT t1.path, newly_discovered_edges.src_id AS vid
        FROM (SELECT path, vid FROM paths WHERE flag = 1 AND vid IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.vid = newly_discovered_edges.dst_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND vid IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    OPEN paths_return FOR SELECT path FROM paths WHERE vid IS NULL OR loop_index = current_loop;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION backtrack(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN attr_column VARCHAR,
    paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
    dummy NUMERIC[] := ARRAY[1];
BEGIN

    SELECT "count" INTO num_growing_path FROM init_paths_with_attr(sources) LIMIT 1;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP
        
        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT vid FROM paths 
        WHERE paths.flag = 1 AND paths.vid IS NOT NULL;
        
        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT src_id, dst_id , ' || quote_ident(attr_column) || ' FROM current_vertices JOIN public.edges 
        ON current_vertices.vid = edges.dst_id';

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vid, edges_attr, flag, loop_index) 
        SELECT t2.path||t2.vid, t2.vid, ARRAY_APPEND(t2.edges_attr, t2.attr), 0, current_loop FROM
        (SELECT t1.path, newly_discovered_edges.src_id AS vid, t1.edges_attr, newly_discovered_edges.attr
        FROM (SELECT path, vid, edges_attr FROM paths WHERE flag = 1 AND vid IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.vid = newly_discovered_edges.dst_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND vid IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    OPEN paths_return FOR SELECT path, edges_attr FROM paths WHERE vid IS NULL OR loop_index = current_loop;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION backtrack(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
BEGIN

    SELECT "count" INTO num_growing_path FROM init_paths(sources) LIMIT 1;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP

        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT vid FROM paths 
        WHERE paths.flag = 1 AND paths.vid IS NOT NULL;
        
        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT src_id, dst_id FROM current_vertices JOIN public.edges 
        ON current_vertices.vid = edges.dst_id AND edges.'
        || quote_ident(edge_type_column) || ' = ' || edge_type_value;

        current_loop := current_loop + 1 ;
        INSERT INTO paths(path, vid, flag, loop_index)
        SELECT t2.path||t2.vid, t2.vid, 0, current_loop FROM
        (SELECT t1.path, newly_discovered_edges.src_id AS vid
        FROM (SELECT path, vid FROM paths WHERE flag = 1 AND vid IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.vid = newly_discovered_edges.dst_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND vid IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    OPEN paths_return FOR SELECT path FROM paths WHERE vid IS NULL OR loop_index = current_loop;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION backtrack(
    IN sources INTEGER[], 
    IN max_loops INTEGER, 
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER, 
    IN attr_column VARCHAR,
    paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    num_discovered_vertex INTEGER := 0;
    MAX_DISCOVERED_VERTEX INTEGER := 10e5;
    current_loop INTEGER := 0;
    dummy NUMERIC[] := ARRAY[1];
BEGIN

    SELECT "count" INTO num_growing_path FROM init_paths_with_attr(sources) LIMIT 1;

    num_discovered_vertex := num_discovered_vertex + num_growing_path;

    WHILE num_growing_path > 0 AND current_loop < max_loops AND num_discovered_vertex < MAX_DISCOVERED_VERTEX LOOP

        TRUNCATE TABLE current_vertices;
        INSERT INTO current_vertices 
        SELECT DISTINCT vid FROM paths 
        WHERE paths.flag = 1 AND paths.vid IS NOT NULL;

        TRUNCATE TABLE newly_discovered_edges;
        EXECUTE
        'INSERT INTO newly_discovered_edges
        SELECT src_id, dst_id , ' || quote_ident(attr_column) || ' FROM public.edges JOIN 
        current_vertices ON edges.dst_id = current_vertices.vid 
        AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

        current_loop := current_loop + 1;
        INSERT INTO paths(path, vid, edges_attr, flag, loop_index)
        SELECT t2.path||t2.vid, t2.vid, ARRAY_APPEND(t2.edges_attr, t2.attr), 0, current_loop FROM
        (SELECT t1.path, newly_discovered_edges.src_id AS vid, t1.edges_attr, newly_discovered_edges.attr
        FROM (SELECT path, vid, edges_attr FROM paths WHERE flag = 1 AND vid IS NOT NULL) AS t1 
        LEFT JOIN newly_discovered_edges ON t1.vid = newly_discovered_edges.dst_id) AS t2;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0 AND vid IS NOT NULL;
        num_discovered_vertex := num_discovered_vertex + num_growing_path;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    OPEN paths_return FOR SELECT path, edges_attr FROM paths WHERE vid IS NULL OR loop_index = current_loop;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION bfs_from(
    IN source INTEGER, 
    IN bfs_result refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    current_level INTEGER := 1;
BEGIN
    -- create bfs edges table
    -- TODO, add index on flag, compare performance
    CREATE TEMPORARY TABLE bfs_edges(src_id INTEGER, dst_id INTEGER, level INTEGER, flag INTEGER)
    ON COMMIT DROP DISTRIBUTED BY (dst_id);

    -- init bfs edges table
    INSERT INTO bfs_edges
    SELECT source, edges.dst_id, 0 AS level, 0 AS flag
    FROM public.edges WHERE edges.src_id = source;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vid INTEGER)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

    -- init discovered_vertices
    INSERT INTO discovered_vertices VALUES (source);
    INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_edges
        SELECT DISTINCT t1.src_id,  t1.dst_id, current_level, 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, edges.dst_id FROM
        bfs_edges JOIN public.edges ON bfs_edges.flag = 1 AND bfs_edges.dst_id = edges.src_id) AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.vid
        WHERE discovered_vertices.vid is NULL;

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        -- increase level
        current_level := current_level + 1;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;
    END LOOP;

    OPEN bfs_result FOR SELECT src_id, dst_id, level FROM bfs_edges ORDER BY level;
    RETURN bfs_result;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION bfs_from(
    IN source INTEGER,
    IN edge_type_column VARCHAR,
    IN edge_type_value INTEGER,
    IN bfs_result refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    current_level INTEGER := 1;
BEGIN
    -- create bfs edges table
    -- TODO, add index on flag, compare performance
    CREATE TEMPORARY TABLE bfs_edges(src_id INTEGER, dst_id INTEGER, level INTEGER, flag INTEGER)
    ON COMMIT DROP DISTRIBUTED BY (dst_id);

    -- init bfs edges table
    EXECUTE
    'INSERT INTO bfs_edges
    SELECT ' || source || ', edges.dst_id, 0 AS level, 0 AS flag
    FROM public.edges WHERE edges.src_id = ' || source ||
    ' AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vid INTEGER)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

    -- init discovered_vertices
    INSERT INTO discovered_vertices VALUES (source);
    INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        EXECUTE
        'INSERT INTO bfs_edges
        SELECT DISTINCT t1.src_id,  t1.dst_id, ' || current_level || ', 0 FROM
        (SELECT bfs_edges.dst_id AS src_id, edges.dst_id FROM
        bfs_edges JOIN public.edges ON bfs_edges.flag = 1 
        AND bfs_edges.dst_id = edges.src_id 
        AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.vid
        WHERE discovered_vertices.vid is NULL';

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT dst_id FROM bfs_edges WHERE flag = 0;

        -- increase level
        current_level := current_level + 1;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;
    END LOOP;

    OPEN bfs_result FOR SELECT src_id, dst_id, level FROM bfs_edges ORDER BY level;
    RETURN bfs_result;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION meta_path_search(
    IN meta_path_def VARCHAR[],
    IN edge_orientation INTEGER[],
    IN paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path integer;
    current_loop integer := 1;
    connector varchar(5) := '''-->''';
BEGIN

    CREATE TEMPORARY TABLE paths(
        pathid SERIAL,
        path INTEGER[],
        vid INTEGER,
        flag INTEGER,
        loop_index INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (vid);
    EXECUTE
    'INSERT INTO paths(path, vid, flag, loop_index)
    SELECT ARRAY[vid], vid, 0, ' || current_loop || ' FROM public.vertices
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
                'INSERT INTO paths(path, vid, flag, loop_index)
                SELECT paths.path, edges.dst_id, 0, ' || current_loop || ' FROM paths JOIN public.edges 
                ON paths.vid = edges.src_id AND paths.flag = 1 AND ' || meta_path_def[current_loop];
            ELSE
                EXECUTE
                'INSERT INTO paths(path, vid, flag, loop_index)
                SELECT paths.path, edges.src_id, 0, ' || current_loop || ' FROM paths JOIN public.edges 
                ON paths.vid = edges.dst_id AND paths.flag = 1 AND ' || meta_path_def[current_loop];
            END IF;
        ELSE
        -- constraint on vertex
            EXECUTE
            'INSERT INTO paths(path, vid, flag, loop_index)
            SELECT paths.path||vertices.vid, vertices.vid, 0, ' || current_loop || ' FROM paths JOIN public.vertices 
            ON paths.vid = vertices.vid AND paths.flag = 1 AND ' || meta_path_def[current_loop];
        END IF;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;


    END LOOP;

    OPEN paths_return FOR SELECT path FROM paths WHERE loop_index = current_loop;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cycles_from(
    IN source INTEGER, 
    IN cycle_flag_column VARCHAR, 
    IN cycle_flag_value INTEGER,
    paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    current_loop INTEGER := 1;
BEGIN

    CREATE TEMPORARY TABLE paths(
        pathid SERIAL,
        path INTEGER[],
        vid INTEGER,
        flag INTEGER,
        loop_index INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (vid);

    -- init paths
    EXECUTE
    'INSERT INTO paths(path, vid, flag, loop_index)
    SELECT ARRAY[' || source || ',edges.dst_id], edges.dst_id, 0, ' || current_loop ||
    ' FROM public.edges WHERE edges.src_id = ' || source || 
    ' AND edges.' || quote_ident(cycle_flag_column) || ' = ' || cycle_flag_value;
        

    SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
    UPDATE paths SET flag = 1 WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
                
        current_loop := current_loop + 1;
                
        EXECUTE
        'INSERT INTO paths(path, vid, flag, loop_index)
        SELECT paths.path||edges.dst_id, edges.dst_id, 0, ' || current_loop ||
        ' FROM public.edges JOIN paths ON paths.flag = 1 AND paths.vid = edges.src_id
        AND paths.vid <> ' || source || ' AND indexof(paths.path, edges.dst_id) <= 1
        AND edges.' || quote_ident(cycle_flag_column) || ' = ' || cycle_flag_value;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether all cycles have been found
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    OPEN paths_return FOR SELECT path FROM paths WHERE vid = source;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;