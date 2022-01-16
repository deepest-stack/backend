CREATE OR REPLACE FUNCTION public.parse_path(
    IN result_table_name VARCHAR, 
    IN select_path_sql VARCHAR
) RETURNS VOID AS $$
BEGIN
    CREATE TEMPORARY TABLE tmp_paths(
        pathid INTEGER,
        vertex_idx INTEGER,
        vid VARCHAR(64)
    ) ON COMMIT DROP DISTRIBUTED BY (pathid);

    EXECUTE
    'INSERT INTO tmp_paths(pathid, vertex_idx, vid) ' || select_path_sql;

    EXECUTE
    'CREATE TABLE ' || result_table_name || '(pathid INTEGER, edge_idx INTEGER, src_id VARCHAR(64), dst_id VARCHAR(64)) DISTRIBUTED BY (pathid)';

    EXECUTE
    'INSERT INTO ' || result_table_name || ' SELECT tmp_paths.pathid, t1.vertex_idx AS edge_idx, tmp_paths.vid AS src_id, t1.dst_id FROM
    (SELECT pathid, vid AS dst_id, vertex_idx-1 AS vertex_idx FROM tmp_paths WHERE vertex_idx > 0 AND vid IS NOT NULL) AS t1
    JOIN tmp_paths ON t1.pathid = tmp_paths.pathid AND t1.vertex_idx = tmp_paths.vertex_idx';
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
    vid VARCHAR(64), 
    edge_attr ' || attr_type || ') ON COMMIT DROP DISTRIBUTED BY (pathid)';

    EXECUTE
    'INSERT INTO tmp_paths(pathid, vertex_idx, vid, edge_attr) ' || select_path_sql;

    EXECUTE
    'CREATE TABLE ' || result_table_name || '(
    pathid INTEGER, 
    edge_idx INTEGER, 
    src_id VARCHAR(64), 
    dst_id VARCHAR(64), 
    edge_attr ' || attr_type || ') DISTRIBUTED BY (pathid)';

    EXECUTE
    'INSERT INTO ' || result_table_name || ' SELECT tmp_paths.pathid, t1.vertex_idx AS edge_idx, tmp_paths.vid AS src_id, t1.dst_id, t1.edge_attr FROM
    (SELECT pathid, vid AS dst_id, vertex_idx-1 AS vertex_idx, edge_attr FROM tmp_paths WHERE vertex_idx > 0 AND vid IS NOT NULL) AS t1
    JOIN tmp_paths ON t1.pathid = tmp_paths.pathid AND t1.vertex_idx = tmp_paths.vertex_idx';
END;
$$ LANGUAGE plpgsql;
