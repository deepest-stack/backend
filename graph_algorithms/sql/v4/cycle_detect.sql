-- dependency: utils

CREATE OR REPLACE FUNCTION public.cycle_detect_in(
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_zero_in_v INTEGER;
BEGIN
    DROP TABLE IF EXISTS zero_in_v;
    CREATE TEMP TABLE zero_in_v AS
    SELECT id FROM public.g_v WHERE properties::json->>'1' = '0'
    DISTRIBUTED BY (id);

    DROP TABLE IF EXISTS vertex_in_deg;
    CREATE TEMP TABLE vertex_in_deg AS
    SELECT id, (properties::json->>'1')::INTEGER AS in_degree FROM public.g_v 
    WHERE properties::json->>'1' <> '0'
    DISTRIBUTED BY (id);

    SELECT COUNT(1) INTO num_zero_in_v FROM zero_in_v;

    WHILE num_zero_in_v > 0 LOOP
        UPDATE vertex_in_deg SET in_degree = vertex_in_deg.in_degree - t1.in_degree FROM
        (SELECT other_vertex AS dst_id, COUNT(1) AS in_degree FROM
        zero_in_v JOIN public.g_oe ON zero_in_v.id = g_oe.owner_vertex GROUP BY other_vertex) AS t1
        WHERE vertex_in_deg.id = t1.dst_id;

        DROP TABLE zero_in_v;

        CREATE TEMP TABLE zero_in_v AS
        SELECT id FROM vertex_in_deg WHERE in_degree = 0
        DISTRIBUTED BY (id);

        SELECT COUNT(1) INTO num_zero_in_v FROM zero_in_v;

        DELETE FROM vertex_in_deg WHERE in_degree = 0;
    END LOOP;

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS SELECT id FROM vertex_in_deg DISTRIBUTED BY (id)';
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
    SELECT id FROM public.g_v WHERE properties::json->>'2' = '0'
    DISTRIBUTED BY (id);

    DROP TABLE IF EXISTS vertex_out_deg;
    CREATE TEMP TABLE vertex_out_deg AS
    SELECT id, (properties::json->>'2')::INTEGER AS out_degree FROM public.g_v 
    WHERE properties::json->>'2' <> '0'
    DISTRIBUTED BY (id);

    SELECT COUNT(1) INTO num_zero_out_v FROM zero_out_v;

    WHILE num_zero_out_v > 0 LOOP
        UPDATE vertex_out_deg SET out_degree = vertex_out_deg.out_degree - t1.out_degree FROM
        (SELECT other_vertex AS src_id, COUNT(1) AS out_degree FROM
        zero_out_v JOIN public.g_ie ON zero_out_v.id = g_ie.owner_vertex GROUP BY other_vertex) AS t1
        WHERE vertex_out_deg.id = t1.src_id;

        DROP TABLE zero_out_v;

        CREATE TEMP TABLE zero_out_v AS
        SELECT id FROM vertex_out_deg WHERE out_degree = 0
        DISTRIBUTED BY (id);

        SELECT COUNT(1) INTO num_zero_out_v FROM zero_out_v;

        DELETE FROM vertex_out_deg WHERE out_degree = 0;
    END LOOP;

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS SELECT id FROM vertex_out_deg DISTRIBUTED BY (id)';
    DROP TABLE IF EXISTS zero_out_v;
    DROP TABLE IF EXISTS vertex_out_deg;

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
    SELECT DISTINCT owner_vertex AS id FROM public.g_oe 
    WHERE g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
    ' EXCEPT (SELECT owner_vertex FROM public.g_ie 
    WHERE g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ')
    DISTRIBUTED BY (id)';

    DROP TABLE IF EXISTS vertex_in_deg;
    EXECUTE
    'CREATE TEMP TABLE vertex_in_deg AS
    SELECT owner_vertex AS id, COUNT(1) AS in_degree FROM public.g_ie 
    WHERE g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value
    || ' GROUP BY owner_vertex DISTRIBUTED BY (id)';

    SELECT COUNT(1) INTO num_zero_in_v FROM zero_in_v;

    WHILE num_zero_in_v > 0 LOOP
        EXECUTE
        'UPDATE vertex_in_deg SET in_degree = vertex_in_deg.in_degree - t1.in_degree FROM
        (SELECT other_vertex AS dst_id, COUNT(1) AS in_degree FROM
        zero_in_v JOIN public.g_oe ON zero_in_v.id = g_oe.owner_vertex 
        AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY other_vertex) AS t1
        WHERE vertex_in_deg.id = t1.dst_id';

        DROP TABLE zero_in_v;

        CREATE TEMP TABLE zero_in_v AS
        SELECT id FROM vertex_in_deg WHERE in_degree = 0
        DISTRIBUTED BY (id);

        SELECT COUNT(1) INTO num_zero_in_v FROM zero_in_v;

        DELETE FROM vertex_in_deg WHERE in_degree = 0;
    END LOOP;

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS SELECT id FROM vertex_in_deg DISTRIBUTED BY (id)';
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
    SELECT DISTINCT owner_vertex AS id FROM public.g_ie 
    WHERE g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
    ' EXCEPT (SELECT owner_vertex FROM public.g_oe
    WHERE g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ')
    DISTRIBUTED BY (id)';

    DROP TABLE IF EXISTS vertex_out_deg;
    EXECUTE
    'CREATE TEMP TABLE vertex_out_deg AS
    SELECT owner_vertex AS id, COUNT(1) AS out_degree FROM public.g_oe 
    WHERE g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value
    || ' GROUP BY owner_vertex DISTRIBUTED BY (id)';

    SELECT COUNT(1) INTO num_zero_out_v FROM zero_out_v;

    WHILE num_zero_out_v > 0 LOOP
        EXECUTE
        'UPDATE vertex_out_deg SET out_degree = vertex_out_deg.out_degree - t1.out_degree FROM
        (SELECT other_vertex AS src_id, COUNT(1) AS out_degree FROM
        zero_out_v JOIN public.g_ie ON zero_out_v.id = g_ie.owner_vertex 
        AND g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY other_vertex) AS t1
        WHERE vertex_out_deg.id = t1.src_id';

        DROP TABLE zero_out_v;

        CREATE TEMP TABLE zero_out_v AS
        SELECT id FROM vertex_out_deg WHERE out_degree = 0
        DISTRIBUTED BY (id);

        SELECT COUNT(1) INTO num_zero_out_v FROM zero_out_v;

        DELETE FROM vertex_out_deg WHERE out_degree = 0;
    END LOOP;

    EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || ' AS SELECT id FROM vertex_out_deg DISTRIBUTED BY (id)';
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
    ' AS SELECT id FROM public.cycle_detect_tmp_t1 JOIN public.cycle_detect_tmp_t2
    USING (id) DISTRIBUTED BY (id)';
    DROP TABLE IF EXISTS public.cycle_detect_tmp_t1;
    DROP TABLE IF EXISTS public.cycle_detect_tmp_t2;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.cycle_detect(
    IN edge_type_column VARCHAR,
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
BEGIN
    IF edge_type_column IS NULL OR edge_type_value IS NULL THEN
        PERFORM public.cycle_detect(result_table_name);
    ELSE
        DROP TABLE IF EXISTS public.cycle_detect_tmp_t1;
        PERFORM public.cycle_detect_in(edge_type_column, edge_type_value, 'public.cycle_detect_tmp_t1');

        DROP TABLE IF EXISTS public.cycle_detect_tmp_t2;
        PERFORM public.cycle_detect_out(edge_type_column, edge_type_value, 'public.cycle_detect_tmp_t2');

        EXECUTE 'CREATE UNLOGGED TABLE ' || result_table_name || 
        ' AS SELECT id FROM public.cycle_detect_tmp_t1 JOIN public.cycle_detect_tmp_t2
        USING (id) DISTRIBUTED BY (id)';
        DROP TABLE IF EXISTS public.cycle_detect_tmp_t1;
        DROP TABLE IF EXISTS public.cycle_detect_tmp_t2;
    END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.cycles_from(
    IN source VARCHAR, 
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
        path VARCHAR[],
        vertex_idx INTEGER[],
        id VARCHAR,
        flag INTEGER,
        loop_index INTEGER
    ) ON COMMIT DROP DISTRIBUTED BY (id);

    -- init paths
    EXECUTE
    'INSERT INTO paths(path, vertex_idx, id, flag, loop_index)
    SELECT ARRAY[owner_vertex, other_vertex], ARRAY[0, 1], other_vertex, 0, ' || current_loop ||
    ' FROM public.g_oe JOIN ' || cycle_vertex_table || ' AS t1 ON g_oe.owner_vertex = ' || quote_literal(source) || 
    ' AND g_oe.other_vertex = t1.id';

    SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
    UPDATE paths SET flag = 1 WHERE flag = 0;

    WHILE num_growing_path > 0 LOOP
                
        current_loop := current_loop + 1;
                
        EXECUTE
        'INSERT INTO paths(path, vertex_idx, id, flag, loop_index)
        SELECT paths.path||other_vertex, paths.vertex_idx||' || current_loop || ', other_vertex, 0, ' || current_loop ||
        ' FROM public.g_oe JOIN paths ON paths.flag = 1 AND paths.id = g_oe.owner_vertex
        AND paths.id <> ' || quote_literal(source) || ' AND indexof(paths.path, g_oe.other_vertex) <= 1
        JOIN ' || cycle_vertex_table || ' AS t1 ON g_oe.other_vertex = t1.id';

        -- vertex whose neighbours have been discovered
        UPDATE paths SET flag = 2 WHERE flag = 1;
        -- whether all cycles have been found
        SELECT COUNT(1) INTO num_growing_path FROM paths WHERE flag = 0;
        UPDATE paths SET flag = 1 WHERE flag = 0;

    END LOOP;

    SELECT COUNT(1) INTO result_count FROM paths WHERE id = source;

    PERFORM public.parse_path(
        result_table_name, 
        'SELECT pathid, UNNEST(vertex_idx), UNNEST(path) FROM paths WHERE id = ' || quote_literal(source)
        );

    RETURN result_count;

END;
$$ LANGUAGE plpgsql;
