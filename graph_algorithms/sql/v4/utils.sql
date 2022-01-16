CREATE OR REPLACE FUNCTION public.parse_path(
    IN result_table_name VARCHAR, 
    IN select_path_sql VARCHAR
) RETURNS VOID AS $$
BEGIN
    CREATE TEMPORARY TABLE tmp_paths(
        pathid INTEGER,
        vertex_idx INTEGER,
        vid VARCHAR
    ) ON COMMIT DROP DISTRIBUTED BY (pathid);

    EXECUTE
    'INSERT INTO tmp_paths(pathid, vertex_idx, vid) ' || select_path_sql;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT tmp_paths.pathid, t1.vertex_idx AS edge_idx, tmp_paths.vid AS src_id, t1.dst_id FROM
    (SELECT pathid, vid AS dst_id, vertex_idx-1 AS vertex_idx 
    FROM tmp_paths WHERE vertex_idx > 0 AND vid IS NOT NULL) AS t1
    JOIN tmp_paths ON t1.pathid = tmp_paths.pathid AND t1.vertex_idx = tmp_paths.vertex_idx
    DISTRIBUTED BY (pathid)';
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.parse_path_with_attr(
    IN result_table_name VARCHAR, 
    IN attr_type VARCHAR,
    IN select_path_sql VARCHAR
) RETURNS VOID AS $$
BEGIN
    EXECUTE
    'CREATE TEMPORARY TABLE tmp_paths(
    pathid INTEGER, 
    vertex_idx INTEGER, 
    vid VARCHAR, 
    attr ' || attr_type || ') ON COMMIT DROP DISTRIBUTED BY (pathid)';

    EXECUTE
    'INSERT INTO tmp_paths(pathid, vertex_idx, vid, attr) ' || select_path_sql;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT tmp_paths.pathid, t1.vertex_idx AS edge_idx, tmp_paths.vid AS src_id, t1.dst_id, t1.attr FROM
    (SELECT pathid, vid AS dst_id, vertex_idx-1 AS vertex_idx, attr 
    FROM tmp_paths WHERE vertex_idx > 0 AND vid IS NOT NULL) AS t1
    JOIN tmp_paths ON t1.pathid = tmp_paths.pathid AND t1.vertex_idx = tmp_paths.vertex_idx 
    DISTRIBUTED BY (pathid)';

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.indexof(
    IN arr anyarray, 
    IN ele anyelement
) RETURNS INTEGER AS $$
BEGIN
    FOR i IN 1..array_length(arr, 1) LOOP
        IF arr[i] = ele THEN
            RETURN i;
        END IF;
    END LOOP;
    RETURN 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
