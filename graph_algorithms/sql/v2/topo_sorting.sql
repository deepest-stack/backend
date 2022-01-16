CREATE OR REPLACE FUNCTION public.topo_sorting(
    IN max_loop INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num2update INTEGER;
    current_loop INTEGER := 1; 
BEGIN
    CREATE TEMP TABLE topo_sort_t with (appendoptimized=TRUE,orientation=ROW) AS
    SELECT vid, 1 AS rk FROM public.vertices
    DISTRIBUTED BY (vid);

    CREATE TEMP TABLE new_sort AS 
    SELECT dst_id AS vid, max(rk)+1 AS rk FROM topo_sort_t JOIN public.edges 
    ON topo_sort_t.vid = edges.src_id GROUP BY dst_id
    DISTRIBUTED BY (vid);
    
    SELECT COUNT(1) INTO num2update FROM new_sort;

    WHILE num2update > 0 AND current_loop < max_loop LOOP

        INSERT INTO topo_sort_t SELECT vid, rk FROM new_sort;

        ALTER TABLE new_sort RENAME TO old_sort;

        CREATE TEMP TABLE new_sort AS 
        SELECT dst_id AS vid, max(rk)+1 AS rk FROM old_sort JOIN public.edges 
        ON old_sort.vid = edges.src_id GROUP BY dst_id
        DISTRIBUTED BY (vid);

        DROP TABLE old_sort;

        SELECT COUNT(1) INTO num2update FROM new_sort;

        current_loop := current_loop + 1;

    END LOOP;

    DROP TABLE IF EXISTS new_sort;

    EXECUTE
    'CREATE TABLE ' || result_table_name || ' AS
    SELECT vid, max(rk) AS attr FROM topo_sort_t GROUP BY vid
    DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS topo_sort_t;

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
    CREATE TEMP TABLE topo_sort_t with (appendoptimized=TRUE,orientation=ROW) AS
    SELECT vid, 1 AS rk FROM public.vertices
    DISTRIBUTED BY (vid);

    EXECUTE
    'CREATE TEMP TABLE new_sort AS 
    SELECT dst_id AS vid, max(rk)+1 AS rk FROM topo_sort_t JOIN public.edges 
    ON topo_sort_t.vid = edges.src_id AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value
    || ' GROUP BY dst_id DISTRIBUTED BY (vid)';
    
    SELECT COUNT(1) INTO num2update FROM new_sort;

    WHILE num2update > 0 AND current_loop < max_loop LOOP

        INSERT INTO topo_sort_t SELECT vid, rk FROM new_sort;

        ALTER TABLE new_sort RENAME TO old_sort;

        EXECUTE
        'CREATE TEMP TABLE new_sort AS 
        SELECT dst_id AS vid, max(rk)+1 AS rk FROM old_sort JOIN public.edges 
        ON old_sort.vid = edges.src_id AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value
        || ' GROUP BY dst_id DISTRIBUTED BY (vid)';

        DROP TABLE old_sort;

        SELECT COUNT(1) INTO num2update FROM new_sort;

        current_loop := current_loop + 1;

    END LOOP;

    DROP TABLE IF EXISTS new_sort;

    EXECUTE
    'CREATE TABLE ' || result_table_name || ' AS
    SELECT vid, max(rk) AS attr FROM topo_sort_t GROUP BY vid
    DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS topo_sort_t;

    RETURN current_loop;

END;
$$ LANGUAGE plpgsql;
