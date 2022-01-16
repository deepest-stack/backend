CREATE OR REPLACE FUNCTION longest_path(
    IN paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN
    CREATE TEMPORARY TABLE paths(path text, tail INTEGER, flag boolean) ON COMMIT DROP DISTRIBUTED BY (tail);
    INSERT INTO paths
    SELECT ''||zero_in_degree.src AS path,  edges.dst_id as tail , TRUE FROM
    (SELECT t1.src_id AS src FROM
    (SELECT DISTINCT src_id FROM edges) AS t1 LEFT JOIN
    (SELECT DISTINCT dst_id FROM edges) AS t2
    ON t1.src_id = t2.dst_id WHERE t2.dst_id IS NULL) AS zero_in_degree
    JOIN edges ON zero_in_degree.src = edges.src_id;

    SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag;

    WHILE num_growing_path > 0 LOOP
        -- if path keep growing, then the last iteration can not be longest path
        DELETE FROM paths WHERE NOT flag;
        UPDATE paths SET path=path||'-->'||tail;
        UPDATE paths SET flag=FALSE;
        INSERT INTO paths
        SELECT paths.path, edges.dst_id AS tail, TRUE FROM
        paths JOIN edges ON paths.tail = edges.src_id;
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag;
    END LOOP;

    OPEN paths_return FOR SELECT path FROM paths;

    RETURN paths_return;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION longest_path(
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN paths_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN
    CREATE TEMPORARY TABLE paths(path text, tail INTEGER, flag boolean) ON COMMIT DROP DISTRIBUTED BY (tail);
    EXECUTE
    'INSERT INTO paths
    SELECT ''''||zero_in_degree.src AS path,  edges.dst_id as tail, TRUE FROM
    (SELECT t1.src_id AS src FROM
    (SELECT DISTINCT src_id FROM edges WHERE ' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1 LEFT JOIN
    (SELECT DISTINCT dst_id FROM edges WHERE ' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t2
    ON t1.src_id = t2.dst_id WHERE t2.dst_id IS NULL) AS zero_in_degree
    JOIN edges ON zero_in_degree.src = edges.src_id AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

    SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag;

    WHILE num_growing_path > 0 LOOP
        -- if path keep growing, then the last iteration can not be longest path
        DELETE FROM paths WHERE NOT flag;
        UPDATE paths SET path=path||'-->'||tail;
        UPDATE paths SET flag=FALSE;
        EXECUTE
        'INSERT INTO paths
        SELECT paths.path, edges.dst_id AS tail, TRUE FROM
        paths JOIN edges ON paths.tail = edges.src_id AND edges.'
        || quote_ident(edge_type_column) || ' = ' || edge_type_value;
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag;
    END LOOP;

    OPEN paths_return FOR SELECT path FROM paths;

    RETURN paths_return;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION longest_weighted_path(
    IN weight_column VARCHAR,
    IN path_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN
    CREATE TEMPORARY TABLE paths(path TEXT, tail INTEGER, weight NUMERIC, flag INTEGER)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (tail);
    EXECUTE 
    'INSERT INTO paths 
    SELECT ''''||zero_in_degree.src||''-->''||edges.dst_id AS path, 
    edges.dst_id as tail, 
    edges.' || quote_ident(weight_column) || ', 0 FROM
    (SELECT t1.src_id AS src FROM
    (SELECT DISTINCT src_id FROM edges) AS t1 LEFT JOIN
    (SELECT DISTINCT dst_id FROM edges) AS t2
    ON t1.src_id = t2.dst_id WHERE t2.dst_id IS NULL) AS zero_in_degree
    JOIN edges ON zero_in_degree.src = edges.src_id';

    SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        UPDATE paths SET flag=1 WHERE flag = 0;
        -- DELETE FROM paths WHERE weight IS NULL;
        EXECUTE
        'INSERT INTO paths
        SELECT paths.path||''-->''||edges.dst_id AS path,
        edges.dst_id AS tail,
        paths.weight+edges.' || quote_ident(weight_column) || ' AS weight, 0 FROM
        paths JOIN edges ON paths.flag = 1 AND paths.tail = edges.src_id AND paths.weight IS NOT NULL';
        UPDATE paths SET flag=2 WHERE flag = 1;
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
    END LOOP;

    OPEN path_return FOR SELECT path, weight FROM paths WHERE weight = (SELECT max(weight) FROM paths);

    RETURN path_return;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION longest_weighted_path(
    IN weight_column VARCHAR,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN path_return refcursor
) RETURNS refcursor AS $$
DECLARE
    num_growing_path INTEGER;
BEGIN
    CREATE TEMPORARY TABLE paths(path TEXT, tail INTEGER, weight NUMERIC, flag INTEGER)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (tail);
    EXECUTE 
    'INSERT INTO paths 
    SELECT ''''||zero_in_degree.src||''-->''||edges.dst_id AS path, 
    edges.dst_id as tail, 
    edges.' || quote_ident(weight_column) || ', 0 FROM
    (SELECT t1.src_id AS src FROM
    (SELECT DISTINCT src_id FROM edges WHERE ' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t1 LEFT JOIN
    (SELECT DISTINCT dst_id FROM edges WHERE ' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ') AS t2
    ON t1.src_id = t2.dst_id WHERE t2.dst_id IS NULL) AS zero_in_degree
    JOIN edges ON zero_in_degree.src = edges.src_id AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

    SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
        UPDATE paths SET flag=1 WHERE flag = 0;
        -- DELETE FROM paths WHERE weight IS NULL;
        EXECUTE
        'INSERT INTO paths
        SELECT paths.path||''-->''||edges.dst_id AS path,
        edges.dst_id AS tail,
        paths.weight+edges.' || quote_ident(weight_column) || ' AS weight, 0 FROM
        paths JOIN edges ON paths.flag = 1 AND paths.tail = edges.src_id AND paths.weight IS NOT NULL AND edges.'
        || quote_ident(edge_type_column) || ' = ' || edge_type_value;
        UPDATE paths SET flag=2 WHERE flag = 1;
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
    END LOOP;

    OPEN path_return FOR SELECT path, weight FROM paths WHERE weight = (SELECT max(weight) FROM paths);

    RETURN path_return;
END;
$$ LANGUAGE plpgsql;


