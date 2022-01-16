CREATE OR REPLACE FUNCTION public.out_neighbour_count(
    IN k_hop INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num2update INTEGER;
    current_hop INTEGER := 1; 
BEGIN
    CREATE TEMP TABLE out_neighbour_count_t with (appendoptimized=TRUE,orientation=ROW) AS
    SELECT src_id AS vid, COUNT(1) AS num_neighbour FROM public.edges GROUP BY src_id
    DISTRIBUTED BY (vid);

    CREATE TEMP TABLE new_out_neighbour_count AS 
    SELECT src_id AS vid, SUM(num_neighbour) AS num_neighbour
    FROM out_neighbour_count_t JOIN public.edges_mirror 
    ON out_neighbour_count_t.vid = edges_mirror.dst_id GROUP BY src_id
    DISTRIBUTED BY (vid);
    
    SELECT COUNT(1) INTO num2update FROM new_out_neighbour_count;

    WHILE num2update > 0 AND current_hop < k_hop LOOP

        INSERT INTO out_neighbour_count_t SELECT vid, num_neighbour FROM new_out_neighbour_count;

        ALTER TABLE new_out_neighbour_count RENAME TO old_out_neighbour_count;

        CREATE TEMP TABLE new_out_neighbour_count AS 
        SELECT src_id AS vid, SUM(num_neighbour) AS num_neighbour
        FROM old_out_neighbour_count JOIN public.edges_mirror 
        ON old_out_neighbour_count.vid = edges_mirror.dst_id GROUP BY src_id
        DISTRIBUTED BY (vid);

        DROP TABLE old_out_neighbour_count;

        SELECT COUNT(1) INTO num2update FROM new_out_neighbour_count;

        current_hop := current_hop + 1;

    END LOOP;

    DROP TABLE IF EXISTS new_out_neighbour_count;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT vid, max(num_neighbour) AS attr FROM out_neighbour_count_t GROUP BY vid
    DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS out_neighbour_count_t;

    RETURN current_hop;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.in_neighbour_count(
    IN k_hop INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num2update INTEGER;
    current_hop INTEGER := 1; 
BEGIN
    CREATE TEMP TABLE in_neighbour_count_t with (appendoptimized=TRUE,orientation=ROW) AS
    SELECT dst_id AS vid, COUNT(1) AS num_neighbour FROM public.edges_mirror GROUP BY dst_id
    DISTRIBUTED BY (vid);

    CREATE TEMP TABLE new_in_neighbour_count AS 
    SELECT dst_id AS vid, SUM(num_neighbour) AS num_neighbour
    FROM in_neighbour_count_t JOIN public.edges
    ON in_neighbour_count_t.vid = edges.src_id GROUP BY dst_id
    DISTRIBUTED BY (vid);
    
    SELECT COUNT(1) INTO num2update FROM new_in_neighbour_count;

    WHILE num2update > 0 AND current_hop < k_hop LOOP

        INSERT INTO in_neighbour_count_t SELECT vid, num_neighbour FROM new_in_neighbour_count;

        ALTER TABLE new_in_neighbour_count RENAME TO old_in_neighbour_count;

        CREATE TEMP TABLE new_in_neighbour_count AS 
        SELECT dst_id AS vid, SUM(num_neighbour) AS num_neighbour
        FROM old_in_neighbour_count JOIN public.edges
        ON old_in_neighbour_count.vid = edges.src_id GROUP BY dst_id
        DISTRIBUTED BY (vid);

        DROP TABLE old_in_neighbour_count;

        SELECT COUNT(1) INTO num2update FROM new_in_neighbour_count;

        current_hop := current_hop + 1;

    END LOOP;

    DROP TABLE IF EXISTS new_in_neighbour_count;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT vid, max(num_neighbour) AS attr FROM in_neighbour_count_t GROUP BY vid
    DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS in_neighbour_count_t;

    RETURN current_hop;

END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION public.neighbour_count(
    IN k_hop INTEGER,
    IN result_table_name VARCHAR
) RETURNS SETOF INTEGER AS $$
DECLARE
    num2update INTEGER;
    in_hop INTEGER;
    out_hop INTEGER;
BEGIN
    SELECT "in_neighbour_count" INTO in_hop FROM public.in_neighbour_count(k_hop, 'public.neighbour_count_t1');

    SELECT "out_neighbour_count" INTO out_hop FROM public.out_neighbour_count(k_hop, 'public.neighbour_count_t2');

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT neighbour_count_t1.vid, COALESCE(neighbour_count_t1.attr, 0)+COALESCE(neighbour_count_t2.attr, 0) AS attr
    FROM public.neighbour_count_t1 FULL JOIN public.neighbour_count_t2 USING (vid)
    DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS public.neighbour_count_t1, public.neighbour_count_t2;

    RETURN NEXT in_hop;
    RETURN NEXT out_hop;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.out_neighbour_count(
    IN k_hop INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num2update INTEGER;
    current_hop INTEGER := 1; 
BEGIN
    EXECUTE
    'CREATE TEMP TABLE out_neighbour_count_t with (appendoptimized=TRUE,orientation=ROW) AS
    SELECT src_id AS vid, COUNT(1) AS num_neighbour FROM public.edges 
    WHERE edges.'|| quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY src_id
    DISTRIBUTED BY (vid)';

    EXECUTE
    'CREATE TEMP TABLE new_out_neighbour_count AS 
    SELECT src_id AS vid, SUM(num_neighbour) AS num_neighbour
    FROM out_neighbour_count_t JOIN public.edges_mirror 
    ON out_neighbour_count_t.vid = edges_mirror.dst_id 
    WHERE edges_mirror.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY src_id
    DISTRIBUTED BY (vid)';
    
    SELECT COUNT(1) INTO num2update FROM new_out_neighbour_count;

    WHILE num2update > 0 AND current_hop < k_hop LOOP

        INSERT INTO out_neighbour_count_t SELECT vid, num_neighbour FROM new_out_neighbour_count;

        ALTER TABLE new_out_neighbour_count RENAME TO old_out_neighbour_count;

        EXECUTE
        'CREATE TEMP TABLE new_out_neighbour_count AS 
        SELECT src_id AS vid, SUM(num_neighbour) AS num_neighbour
        FROM old_out_neighbour_count JOIN public.edges_mirror 
        ON old_out_neighbour_count.vid = edges_mirror.dst_id 
        AND edges_mirror.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY src_id
        DISTRIBUTED BY (vid)';

        DROP TABLE old_out_neighbour_count;

        SELECT COUNT(1) INTO num2update FROM new_out_neighbour_count;

        current_hop := current_hop + 1;

    END LOOP;

    DROP TABLE IF EXISTS new_out_neighbour_count;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT vid, max(num_neighbour) AS attr FROM out_neighbour_count_t GROUP BY vid
    DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS out_neighbour_count_t;

    RETURN current_hop;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.in_neighbour_count(
    IN k_hop INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num2update INTEGER;
    current_hop INTEGER := 1; 
BEGIN
    EXECUTE
    'CREATE TEMP TABLE in_neighbour_count_t with (appendoptimized=TRUE,orientation=ROW) AS
    SELECT dst_id AS vid, COUNT(1) AS num_neighbour FROM public.edges_mirror 
    WHERE edges_mirror.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY dst_id
    DISTRIBUTED BY (vid)';

    EXECUTE
    'CREATE TEMP TABLE new_in_neighbour_count AS 
    SELECT dst_id AS vid, SUM(num_neighbour) AS num_neighbour
    FROM in_neighbour_count_t JOIN public.edges
    ON in_neighbour_count_t.vid = edges.src_id 
    AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY dst_id
    DISTRIBUTED BY (vid)';
    
    SELECT COUNT(1) INTO num2update FROM new_in_neighbour_count;

    WHILE num2update > 0 AND current_hop < k_hop LOOP

        INSERT INTO in_neighbour_count_t SELECT vid, num_neighbour FROM new_in_neighbour_count;

        ALTER TABLE new_in_neighbour_count RENAME TO old_in_neighbour_count;

        EXECUTE
        'CREATE TEMP TABLE new_in_neighbour_count AS 
        SELECT dst_id AS vid, SUM(num_neighbour) AS num_neighbour
        FROM old_in_neighbour_count JOIN public.edges
        ON old_in_neighbour_count.vid = edges.src_id 
        AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY dst_id
        DISTRIBUTED BY (vid)';

        DROP TABLE old_in_neighbour_count;

        SELECT COUNT(1) INTO num2update FROM new_in_neighbour_count;

        current_hop := current_hop + 1;

    END LOOP;

    DROP TABLE IF EXISTS new_in_neighbour_count;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT vid, max(num_neighbour) AS attr FROM in_neighbour_count_t GROUP BY vid
    DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS in_neighbour_count_t;

    RETURN current_hop;

END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION public.neighbour_count(
    IN k_hop INTEGER,
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS SETOF INTEGER AS $$
DECLARE
    num2update INTEGER;
    in_hop INTEGER;
    out_hop INTEGER;
BEGIN
    SELECT "in_neighbour_count" INTO in_hop FROM 
    public.in_neighbour_count(k_hop, edge_type_column, edge_type_value, 'public.neighbour_count_t1');

    SELECT "out_neighbour_count" INTO out_hop FROM 
    public.out_neighbour_count(k_hop, edge_type_column, edge_type_value, 'public.neighbour_count_t2');

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT neighbour_count_t1.vid, COALESCE(neighbour_count_t1.attr, 0)+COALESCE(neighbour_count_t2.attr, 0) AS attr
    FROM public.neighbour_count_t1 FULL JOIN public.neighbour_count_t2 USING (vid)
    DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS public.neighbour_count_t1, public.neighbour_count_t2;

    RETURN NEXT in_hop;
    RETURN NEXT out_hop;

END;
$$ LANGUAGE plpgsql;