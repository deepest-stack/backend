CREATE OR REPLACE FUNCTION public.topo_sorting(
    IN max_loop INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num2update INTEGER;
    current_loop INTEGER := 1; 
BEGIN
    CREATE TEMPORARY TABLE topo_sort_t with (appendoptimized=TRUE,orientation=ROW) ON COMMIT DROP AS
    SELECT id, 1 AS rk FROM public.g_v
    DISTRIBUTED BY (id);

    CREATE TEMPORARY TABLE new_sort with (appendoptimized=TRUE,orientation=ROW) ON COMMIT DROP AS 
    SELECT other_vertex AS id, max(rk)+1 AS rk FROM topo_sort_t JOIN public.g_oe 
    ON topo_sort_t.id = g_oe.owner_vertex GROUP BY other_vertex
    DISTRIBUTED BY (id);
    
    SELECT COUNT(1) INTO num2update FROM new_sort;

    WHILE num2update > 0 AND current_loop < max_loop LOOP

        INSERT INTO topo_sort_t SELECT id, rk FROM new_sort;

        ALTER TABLE new_sort RENAME TO old_sort;

        CREATE TEMP TABLE new_sort with (appendoptimized=TRUE,orientation=ROW) ON COMMIT DROP AS 
        SELECT other_vertex AS id, max(rk)+1 AS rk FROM old_sort JOIN public.g_oe 
        ON old_sort.id = g_oe.owner_vertex GROUP BY other_vertex
        DISTRIBUTED BY (id);

        DROP TABLE old_sort;

        SELECT COUNT(1) INTO num2update FROM new_sort;

        current_loop := current_loop + 1;

    END LOOP;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT id, max(rk) AS attr FROM topo_sort_t GROUP BY id
    DISTRIBUTED BY (id)';

    RETURN current_loop;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.topo_sorting(
    IN max_loop INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num2update INTEGER;
    current_loop INTEGER := 1; 
BEGIN
    IF edge_type_column IS NULL OR edge_type_value IS NULL THEN
        SELECT public.topo_sorting(max_loop, result_table_name) INTO current_loop;
        RETURN current_loop;
    END IF;

    CREATE TEMPORARY TABLE topo_sort_t with (appendoptimized=TRUE,orientation=ROW) ON COMMIT DROP AS
    SELECT id, 1 AS rk FROM public.g_v
    DISTRIBUTED BY (id);

    EXECUTE
    'CREATE TEMPORARY TABLE new_sort with (appendoptimized=TRUE,orientation=ROW) ON COMMIT DROP AS 
    SELECT other_vertex AS id, max(rk)+1 AS rk FROM topo_sort_t JOIN public.g_oe  
    ON topo_sort_t.id = g_oe.owner_vertex AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value
    || ' GROUP BY other_vertex DISTRIBUTED BY (id)';
    
    SELECT COUNT(1) INTO num2update FROM new_sort;

    WHILE num2update > 0 AND current_loop < max_loop LOOP

        INSERT INTO topo_sort_t SELECT id, rk FROM new_sort;

        ALTER TABLE new_sort RENAME TO old_sort;

        EXECUTE
        'CREATE TEMPORARY TABLE new_sort with (appendoptimized=TRUE,orientation=ROW) ON COMMIT DROP AS 
        SELECT other_vertex AS id, max(rk)+1 AS rk FROM topo_sort_t JOIN public.g_oe  
        ON topo_sort_t.id = g_oe.owner_vertex AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value
        || ' GROUP BY other_vertex DISTRIBUTED BY (id)';

        DROP TABLE old_sort;

        SELECT COUNT(1) INTO num2update FROM new_sort;

        current_loop := current_loop + 1;

    END LOOP;

    EXECUTE
    'CREATE TABLE ' || result_table_name || ' AS
    SELECT id, max(rk) AS attr FROM topo_sort_t GROUP BY id
    DISTRIBUTED BY (id)';

    RETURN current_loop;

END;
$$ LANGUAGE plpgsql;
