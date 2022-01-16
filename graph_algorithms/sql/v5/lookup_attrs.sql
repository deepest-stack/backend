CREATE OR REPLACE FUNCTION ${graph_name}.lookup_vertices(
    IN vids INTEGER[], 
    IN vtype VARCHAR,
    IN attr_columns VARCHAR[],
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    result_count INTEGER := 0;
    idx INTEGER;
BEGIN

    CREATE TEMP TABLE tmp_vertices ON COMMIT DROP AS
    SELECT t1.vid FROM UNNEST(vids) AS t1(vid)
    DISTRIBUTED BY (vid);

    FOR idx IN 1..array_length(attr_columns, 1) LOOP
        attr_columns[idx] := '''' || attr_columns[idx] || ''', ' || attr_columns[idx];
    END LOOP;
    
    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT vid, json_build_object(' || array_to_string(attr_columns, ', ') ||
    ') AS attrs FROM ${graph_name}.vertex_' || vtype || ' JOIN tmp_vertices USING(vid)
    DISTRIBUTED BY (vid)';

    EXECUTE 'SELECT COUNT(1) FROM ' || result_table_name INTO result_count;
    RETURN result_count;

END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION ${graph_name}.lookup_edges(
    IN src_ids INTEGER[], 
    IN dst_ids INTEGER[],
    IN etype VARCHAR,
    IN attr_columns VARCHAR[],
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    result_count INTEGER := 0;
    idx INTEGER;
BEGIN
    
    CREATE TEMP TABLE tmp_edges ON COMMIT DROP AS
    SELECT t1.src_id, t1.dst_id FROM UNNEST(src_ids, dst_ids) AS t1(src_id, dst_id)
    DISTRIBUTED BY (src_id);
    
    FOR idx IN 1..array_length(attr_columns, 1) LOOP
        attr_columns[idx] := '''' || attr_columns[idx] || ''', ' || attr_columns[idx];
    END LOOP;
    
    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT tmp_edges.src_id, tmp_edges.dst_id, json_build_object(' || array_to_string(attr_columns, ', ') ||
    ') AS attrs FROM ${graph_name}.edge_' || etype || ' JOIN tmp_edges USING(src_id, dst_id)
    DISTRIBUTED BY (src_id)';

    EXECUTE 'SELECT COUNT(1) FROM ' || result_table_name INTO result_count;
    RETURN result_count;

END;
$$ LANGUAGE plpgsql;