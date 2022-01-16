CREATE OR REPLACE FUNCTION public.init_shortest_paths(
    IN source INTEGER,
    OUT count INTEGER
) AS $$
BEGIN
    CREATE TEMPORARY TABLE paths(
        pathid SERIAL,
        path INTEGER[],
        vid INTEGER,
        flag INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (vid);

    CREATE TEMPORARY TABLE discovered_vertices(
        vid INTEGER
    ) WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

    -- init paths
    INSERT INTO paths(path, vid, flag) VALUES (ARRAY[source], source, 0);

    INSERT INTO discovered_vertices VALUES(source);
    SELECT COUNT(1) INTO count FROM paths WHERE flag = 0;
    UPDATE paths SET flag = 1 WHERE flag = 0;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.init_shortest_weighted_paths(
    IN source INTEGER,
    OUT count INTEGER
) AS $$
BEGIN
    CREATE TEMPORARY TABLE paths(
        pathid SERIAL,
        path INTEGER[],
        vid INTEGER,
        weight REAL,
        flag INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (vid);

    CREATE TEMPORARY TABLE discovered_vertices(
        vid INTEGER
    ) WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vid);

    -- init paths
    INSERT INTO paths(path, vid, weight, flag) VALUES (ARRAY[source], source, 0.0, 0);

    INSERT INTO discovered_vertices VALUES(source);

    SELECT COUNT(1) INTO count FROM paths WHERE flag = 0;
    UPDATE paths SET flag = 1 WHERE flag = 0;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.shortest_path(
    IN source INTEGER, 
    IN target INTEGER,
    IN max_length INTEGER,
    IN paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    target_reached INTEGER := 0;
    current_loop INTEGER := 1;
BEGIN

    SELECT "count" INTO num_growing_path FROM init_shortest_paths(source);
    
    SELECT COUNT(1) INTO target_reached FROM paths WHERE vid = target;

    WHILE num_growing_path > 0 AND target_reached = 0 AND current_loop <= max_length LOOP

        INSERT INTO paths(path, vid, flag)
        SELECT t1.path||t1.dst_id, t1.dst_id, 0 FROM
        (SELECT paths.path, edges.dst_id FROM
        paths JOIN public.edges ON paths.vid = edges.src_id AND paths.flag = 1) AS t1
        WHERE t1.dst_id NOT IN (SELECT vid FROM discovered_vertices);

        INSERT INTO discovered_vertices
        SELECT DISTINCT vid FROM paths WHERE flag = 0;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- check whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
        -- check whether target has reached
        SELECT COUNT(1) INTO target_reached FROM paths WHERE flag = 0 AND vid = target;
        UPDATE paths SET flag = 1 WHERE flag = 0;
        current_loop := current_loop + 1;

    END LOOP;

    OPEN paths_return FOR SELECT path FROM paths WHERE vid = target;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.shortest_path(
    IN source INTEGER, 
    IN target INTEGER, 
    IN max_length INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    target_reached INTEGER := 0;
    current_loop INTEGER := 1;
BEGIN

    SELECT "count" INTO num_growing_path FROM init_shortest_paths(source);
    
    SELECT COUNT(1) INTO target_reached FROM paths WHERE vid = target;

    WHILE num_growing_path > 0 AND target_reached = 0 AND current_loop < max_length LOOP
        
        EXECUTE
        'INSERT INTO paths(path, vid, flag)
         SELECT t1.path||t1.dst_id, t1.dst_id, 0 FROM
        (SELECT paths.path, edges.dst_id FROM
        paths JOIN public.edges ON paths.vid = edges.src_id AND paths.flag = 1 AND edges.'
        || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        WHERE t1.dst_id NOT IN (SELECT vid FROM discovered_vertices)';

        INSERT INTO discovered_vertices
        SELECT DISTINCT vid FROM paths WHERE flag = 0;

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- check whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
        -- check whether target has reached
        SELECT COUNT(1) INTO target_reached FROM paths WHERE flag = 0 AND vid = target;
        UPDATE paths SET flag = 1 WHERE flag = 0;
        current_loop := current_loop + 1;

    END LOOP;

    OPEN paths_return FOR SELECT path FROM paths WHERE vid = target;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.shortest_weighted_path(
    IN source INTEGER, 
    IN target INTEGER, 
    IN max_length INTEGER,
    IN weight_column VARCHAR,
    IN paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    current_loop INTEGER := 1;
BEGIN
    SELECT "count" INTO num_growing_path FROM init_shortest_weighted_paths(source);
    WHILE num_growing_path > 0 AND current_loop <= max_length LOOP

        EXECUTE        
        'INSERT INTO paths(path, vid, weight, flag)
         SELECT t1.path||t1.dst_id, t1.dst_id, t1.acc_weight+t1.weight, 0 FROM
        (SELECT paths.path, paths.weight AS acc_weight, edges.dst_id, edges.' || quote_ident(weight_column) || ' AS weight 
        FROM paths JOIN public.edges ON paths.vid = edges.src_id AND paths.flag = 1
        AND paths.vid <> ' || target || ') AS t1
        WHERE t1.dst_id NOT IN (SELECT vid FROM discovered_vertices)';
        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- check whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
        UPDATE paths SET flag = 1 WHERE flag = 0;
        current_loop := current_loop + 1;

    END LOOP;

    OPEN paths_return FOR SELECT path, weight FROM paths WHERE vid = target ORDER BY weight LIMIT 1;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.shortest_weighted_path(
    IN source INTEGER, 
    IN target INTEGER, 
    IN max_length INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN weight_column VARCHAR,
    IN paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
    current_loop INTEGER := 1;
BEGIN

    SELECT "count" INTO num_growing_path FROM init_shortest_weighted_paths(source);

    WHILE num_growing_path > 0 AND current_loop <= max_length LOOP

        EXECUTE        
        'INSERT INTO paths(path, vid, weight, flag) 
        SELECT t1.path||t1.dst_id, t1.dst_id, t1.acc_weight+t1.weight, 0 FROM
        (SELECT paths.path, paths.weight AS acc_weight, edges.dst_id, edges.' || quote_ident(weight_column) || ' AS weight 
        FROM paths JOIN public.edges ON paths.vid = edges.src_id AND paths.flag = 1
        AND paths.vid <> ' || target || ' AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1
        WHERE t1.dst_id NOT IN (SELECT vid FROM discovered_vertices)';
        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- check whether leaf has reached
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
        UPDATE paths SET flag = 1 WHERE flag = 0;
        current_loop := current_loop + 1;

    END LOOP;

    OPEN paths_return FOR SELECT path, weight FROM paths WHERE vid = target ORDER BY weight LIMIT 1;
    RETURN paths_return;

END;
$$ LANGUAGE plpgsql;
