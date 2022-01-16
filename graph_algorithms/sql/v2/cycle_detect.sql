-- dependency: utils

CREATE OR REPLACE FUNCTION public.cycle_detect_in(
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_zero_in_v INTEGER;
BEGIN
    DROP TABLE IF EXISTS zero_in_v;
    CREATE TEMP TABLE zero_in_v AS
    SELECT vid FROM public.vertices WHERE in_degree = 0
    DISTRIBUTED BY (vid);

    DROP TABLE IF EXISTS vertex_in_deg;
    CREATE TEMP TABLE vertex_in_deg AS
    SELECT vid, in_degree FROM public.vertices WHERE in_degree <> 0
    DISTRIBUTED BY (vid);

    SELECT COUNT(1) INTO num_zero_in_v FROM zero_in_v;

    WHILE num_zero_in_v > 0 LOOP
        UPDATE vertex_in_deg SET in_degree = vertex_in_deg.in_degree - t1.in_degree FROM
        (SELECT dst_id, COUNT(1) AS in_degree FROM
        zero_in_v JOIN public.edges ON zero_in_v.vid = edges.src_id GROUP BY dst_id) AS t1
        WHERE vertex_in_deg.vid = t1.dst_id;

        DROP TABLE zero_in_v;

        CREATE TEMP TABLE zero_in_v AS
        SELECT vid FROM vertex_in_deg WHERE in_degree = 0
        DISTRIBUTED BY (vid);

        SELECT COUNT(1) INTO num_zero_in_v FROM zero_in_v;

        DELETE FROM vertex_in_deg WHERE in_degree = 0;
    END LOOP;

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS SELECT vid FROM vertex_in_deg DISTRIBUTED BY (vid)';
    DROP TABLE IF EXISTS zero_in_v;
    DROP TABLE IF EXISTS vertex_in_deg;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.cycle_detect_out(
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_zero_out_v INTEGER;
BEGIN
    DROP TABLE IF EXISTS zero_out_v;
    CREATE TEMP TABLE zero_out_v AS
    SELECT vid FROM public.vertices WHERE out_degree = 0
    DISTRIBUTED BY (vid);

    DROP TABLE IF EXISTS vertex_out_deg;
    CREATE TEMP TABLE vertex_out_deg AS
    SELECT vid, out_degree FROM public.vertices WHERE out_degree <> 0
    DISTRIBUTED BY (vid);

    SELECT COUNT(1) INTO num_zero_out_v FROM zero_out_v;

    WHILE num_zero_out_v > 0 LOOP
        UPDATE vertex_out_deg SET out_degree = vertex_out_deg.out_degree - t1.out_degree FROM
        (SELECT src_id, COUNT(1) AS out_degree FROM
        zero_out_v JOIN public.edges_mirror ON zero_out_v.vid = edges_mirror.dst_id GROUP BY src_id) AS t1
        WHERE vertex_out_deg.vid = t1.src_id;

        DROP TABLE zero_out_v;

        CREATE TEMP TABLE zero_out_v AS
        SELECT vid FROM vertex_out_deg WHERE out_degree = 0
        DISTRIBUTED BY (vid);

        SELECT COUNT(1) INTO num_zero_out_v FROM zero_out_v;

        DELETE FROM vertex_out_deg WHERE out_degree = 0;
    END LOOP;

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS SELECT vid FROM vertex_out_deg DISTRIBUTED BY (vid)';
    DROP TABLE IF EXISTS zero_out_v;
    DROP TABLE IF EXISTS vertex_out_deg;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.cycle_detect(
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
BEGIN
    DROP TABLE IF EXISTS public.cycle_detect_tmp_t1;
    PERFORM public.cycle_detect_in('public.cycle_detect_tmp_t1');

    DROP TABLE IF EXISTS public.cycle_detect_tmp_t2;
    PERFORM public.cycle_detect_out('public.cycle_detect_tmp_t2');

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || 
    ' AS SELECT vid FROM public.cycle_detect_tmp_t1 JOIN public.cycle_detect_tmp_t2
    USING (vid) DISTRIBUTED BY (vid)';
    DROP TABLE IF EXISTS public.cycle_detect_tmp_t1;
    DROP TABLE IF EXISTS public.cycle_detect_tmp_t2;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.cycle_detect_in(
    IN edge_type_column VARCHAR,
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_zero_in_v INTEGER;
BEGIN
    DROP TABLE IF EXISTS zero_in_v;
    EXECUTE
    'CREATE TEMP TABLE zero_in_v AS
    SELECT DISTINCT src_id AS vid FROM public.edges WHERE edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
    ' EXCEPT (SELECT dst_id FROM public.edges_mirror 
    WHERE edges_mirror.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ')
    DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS vertex_in_deg;
    EXECUTE
    'CREATE TEMP TABLE vertex_in_deg AS
    SELECT dst_id AS vid, COUNT(1) AS in_degree FROM public.edges_mirror 
    WHERE edges_mirror.' || quote_ident(edge_type_column) || ' = ' || edge_type_value
    || ' GROUP BY dst_id DISTRIBUTED BY (vid)';

    SELECT COUNT(1) INTO num_zero_in_v FROM zero_in_v;

    WHILE num_zero_in_v > 0 LOOP
        EXECUTE
        'UPDATE vertex_in_deg SET in_degree = vertex_in_deg.in_degree - t1.in_degree FROM
        (SELECT dst_id, COUNT(1) AS in_degree FROM
        zero_in_v JOIN public.edges ON zero_in_v.vid = edges.src_id 
        AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY dst_id) AS t1
        WHERE vertex_in_deg.vid = t1.dst_id';

        DROP TABLE zero_in_v;

        CREATE TEMP TABLE zero_in_v AS
        SELECT vid FROM vertex_in_deg WHERE in_degree = 0
        DISTRIBUTED BY (vid);

        SELECT COUNT(1) INTO num_zero_in_v FROM zero_in_v;

        DELETE FROM vertex_in_deg WHERE in_degree = 0;
    END LOOP;

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS SELECT vid FROM vertex_in_deg DISTRIBUTED BY (vid)';
    DROP TABLE IF EXISTS zero_in_v;
    DROP TABLE IF EXISTS vertex_in_deg;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.cycle_detect_out(
    IN edge_type_column VARCHAR,
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_zero_out_v INTEGER;
BEGIN
    DROP TABLE IF EXISTS zero_out_v;
    EXECUTE
    'CREATE TEMP TABLE zero_out_v AS
    SELECT DISTINCT dst_id AS vid FROM public.edges_mirror WHERE edges_mirror.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
    ' EXCEPT (SELECT src_id FROM public.edges
    WHERE edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ')
    DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS vertex_out_deg;
    EXECUTE
    'CREATE TEMP TABLE vertex_out_deg AS
    SELECT src_id AS vid, COUNT(1) AS out_degree FROM public.edges 
    WHERE edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value
    || ' GROUP BY src_id DISTRIBUTED BY (vid)';

    SELECT COUNT(1) INTO num_zero_out_v FROM zero_out_v;

    WHILE num_zero_out_v > 0 LOOP
        EXECUTE
        'UPDATE vertex_out_deg SET out_degree = vertex_out_deg.out_degree - t1.out_degree FROM
        (SELECT src_id, COUNT(1) AS out_degree FROM
        zero_out_v JOIN public.edges_mirror ON zero_out_v.vid = edges_mirror.dst_id 
        AND edges_mirror.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY src_id) AS t1
        WHERE vertex_out_deg.vid = t1.src_id';

        DROP TABLE zero_out_v;

        CREATE TEMP TABLE zero_out_v AS
        SELECT vid FROM vertex_out_deg WHERE out_degree = 0
        DISTRIBUTED BY (vid);

        SELECT COUNT(1) INTO num_zero_out_v FROM zero_out_v;

        DELETE FROM vertex_out_deg WHERE out_degree = 0;
    END LOOP;

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS SELECT vid FROM vertex_out_deg DISTRIBUTED BY (vid)';
    DROP TABLE IF EXISTS zero_out_v;
    DROP TABLE IF EXISTS vertex_out_deg;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.cycle_detect(
    IN edge_type_column VARCHAR,
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
BEGIN
    DROP TABLE IF EXISTS public.cycle_detect_tmp_t1;
    PERFORM public.cycle_detect_in(edge_type_column, edge_type_value, 'public.cycle_detect_tmp_t1');

    DROP TABLE IF EXISTS public.cycle_detect_tmp_t2;
    PERFORM public.cycle_detect_out(edge_type_column, edge_type_value, 'public.cycle_detect_tmp_t2');

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || 
    ' AS SELECT vid FROM public.cycle_detect_tmp_t1 JOIN public.cycle_detect_tmp_t2
    USING (vid) DISTRIBUTED BY (vid)';
    DROP TABLE IF EXISTS public.cycle_detect_tmp_t1;
    DROP TABLE IF EXISTS public.cycle_detect_tmp_t2;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.cycles_from(
    IN source INTEGER, 
    IN cycle_vertex_table VARCHAR, 
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num_growing_path INTEGER;
    current_loop INTEGER := 1;
    result_count INTEGER;
BEGIN

    CREATE TEMPORARY TABLE paths(
        pathid SERIAL,
        path INTEGER[],
        vertex_idx INTEGER[],
        vid INTEGER,
        flag INTEGER,
        loop_index INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (vid);

    -- init paths
    EXECUTE
    'INSERT INTO paths(path, vertex_idx, vid, flag, loop_index)
    SELECT ARRAY[' || source || ',edges.dst_id], ARRAY[0, 1], edges.dst_id, 0, ' || current_loop ||
    ' FROM public.edges JOIN ' || cycle_vertex_table || ' AS t1 ON edges.src_id = ' || source || 
    ' AND edges.dst_id = t1.vid';
        

    SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
    UPDATE paths SET flag = 1 WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
                
        current_loop := current_loop + 1;
                
        EXECUTE
        'INSERT INTO paths(path, vertex_idx, vid, flag, loop_index)
        SELECT paths.path||edges.dst_id, paths.vertex_idx||' || current_loop || ' ,edges.dst_id, 0, ' || current_loop ||
        ' FROM public.edges JOIN paths ON paths.flag = 1 AND paths.vid = edges.src_id
        AND paths.vid <> ' || source || ' AND indexof(paths.path, edges.dst_id) <= 1
        JOIN ' || cycle_vertex_table || ' AS t1 ON edges.dst_id = t1.vid';

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether all cycles have been found
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE vid = source;

    PERFORM public.parse_path(
        result_table_name, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path) FROM paths WHERE vid = ' || source
        );

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;
