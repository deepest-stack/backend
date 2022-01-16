CREATE OR REPLACE FUNCTION public.out_neighbour_count(
    IN k_hop INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    num2update INTEGER;
    current_hop INTEGER := 1; 
BEGIN
    CREATE TEMP TABLE out_neighbour_count_t with (appendoptimized=TRUE,orientation=ROW) AS
    SELECT owner_vertex AS id, COUNT(1) AS num_neighbour FROM public.g_oe GROUP BY owner_vertex
    DISTRIBUTED BY (id);

    CREATE TEMP TABLE new_out_neighbour_count AS 
    SELECT other_vertex AS id, SUM(num_neighbour) AS num_neighbour
    FROM out_neighbour_count_t JOIN public.g_ie 
    ON out_neighbour_count_t.id = g_ie.owner_vertex GROUP BY other_vertex
    DISTRIBUTED BY (id);
    
    SELECT COUNT(1) INTO num2update FROM new_out_neighbour_count;

    WHILE num2update > 0 AND current_hop < k_hop LOOP

        INSERT INTO out_neighbour_count_t SELECT id, num_neighbour FROM new_out_neighbour_count;

        ALTER TABLE new_out_neighbour_count RENAME TO old_out_neighbour_count;

        CREATE TEMP TABLE new_out_neighbour_count AS 
        SELECT other_vertex AS id, SUM(num_neighbour) AS num_neighbour
        FROM old_out_neighbour_count JOIN public.g_ie 
        ON old_out_neighbour_count.id = g_ie.owner_vertex GROUP BY other_vertex
        DISTRIBUTED BY (id);

        DROP TABLE old_out_neighbour_count;

        SELECT COUNT(1) INTO num2update FROM new_out_neighbour_count;

        current_hop := current_hop + 1;

    END LOOP;

    DROP TABLE IF EXISTS new_out_neighbour_count;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT id, max(num_neighbour) AS attr FROM out_neighbour_count_t GROUP BY id
    DISTRIBUTED BY (id)';

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
    SELECT owner_vertex AS id, COUNT(1) AS num_neighbour FROM public.g_ie GROUP BY owner_vertex
    DISTRIBUTED BY (id);

    CREATE TEMP TABLE new_in_neighbour_count AS 
    SELECT other_vertex AS id, SUM(num_neighbour) AS num_neighbour
    FROM in_neighbour_count_t JOIN public.g_oe
    ON in_neighbour_count_t.id = g_oe.owner_vertex GROUP BY other_vertex
    DISTRIBUTED BY (id);
    
    SELECT COUNT(1) INTO num2update FROM new_in_neighbour_count;

    WHILE num2update > 0 AND current_hop < k_hop LOOP

        INSERT INTO in_neighbour_count_t SELECT id, num_neighbour FROM new_in_neighbour_count;

        ALTER TABLE new_in_neighbour_count RENAME TO old_in_neighbour_count;

        CREATE TEMP TABLE new_in_neighbour_count AS 
        SELECT other_vertex AS id, SUM(num_neighbour) AS num_neighbour
        FROM old_in_neighbour_count JOIN public.g_oe
        ON old_in_neighbour_count.id = g_oe.owner_vertex GROUP BY other_vertex
        DISTRIBUTED BY (id);

        DROP TABLE old_in_neighbour_count;

        SELECT COUNT(1) INTO num2update FROM new_in_neighbour_count;

        current_hop := current_hop + 1;

    END LOOP;

    DROP TABLE IF EXISTS new_in_neighbour_count;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT id, max(num_neighbour) AS attr FROM in_neighbour_count_t GROUP BY id
    DISTRIBUTED BY (id)';

    DROP TABLE IF EXISTS in_neighbour_count_t;

    RETURN current_hop;

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
    SELECT owner_vertex AS id, COUNT(1) AS num_neighbour FROM public.g_oe 
    WHERE g_oe.'|| quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY owner_vertex
    DISTRIBUTED BY (id)';

    EXECUTE
    'CREATE TEMP TABLE new_out_neighbour_count AS 
    SELECT other_vertex AS id, SUM(num_neighbour) AS num_neighbour
    FROM out_neighbour_count_t JOIN public.g_ie 
    ON out_neighbour_count_t.id = g_ie.owner_vertex 
    WHERE g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY other_vertex
    DISTRIBUTED BY (id)';
    
    SELECT COUNT(1) INTO num2update FROM new_out_neighbour_count;

    WHILE num2update > 0 AND current_hop < k_hop LOOP

        INSERT INTO out_neighbour_count_t SELECT id, num_neighbour FROM new_out_neighbour_count;

        ALTER TABLE new_out_neighbour_count RENAME TO old_out_neighbour_count;

        EXECUTE
        'CREATE TEMP TABLE new_out_neighbour_count AS 
        SELECT other_vertex AS id, SUM(num_neighbour) AS num_neighbour
        FROM old_out_neighbour_count JOIN public.g_ie 
        ON old_out_neighbour_count.id = g_ie.owner_vertex 
        AND g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY other_vertex
        DISTRIBUTED BY (id)';

        DROP TABLE old_out_neighbour_count;

        SELECT COUNT(1) INTO num2update FROM new_out_neighbour_count;

        current_hop := current_hop + 1;

    END LOOP;

    DROP TABLE IF EXISTS new_out_neighbour_count;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT id, max(num_neighbour) AS attr FROM out_neighbour_count_t GROUP BY id
    DISTRIBUTED BY (id)';

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
    SELECT owner_vertex AS id, COUNT(1) AS num_neighbour FROM public.g_ie 
    WHERE g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY owner_vertex
    DISTRIBUTED BY (id)';

    EXECUTE
    'CREATE TEMP TABLE new_in_neighbour_count AS 
    SELECT other_vertex AS id, SUM(num_neighbour) AS num_neighbour
    FROM in_neighbour_count_t JOIN public.g_oe
    ON in_neighbour_count_t.id = g_oe.owner_vertex 
    AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY other_vertex
    DISTRIBUTED BY (id)';
    
    SELECT COUNT(1) INTO num2update FROM new_in_neighbour_count;

    WHILE num2update > 0 AND current_hop < k_hop LOOP

        INSERT INTO in_neighbour_count_t SELECT id, num_neighbour FROM new_in_neighbour_count;

        ALTER TABLE new_in_neighbour_count RENAME TO old_in_neighbour_count;

        EXECUTE
        'CREATE TEMP TABLE new_in_neighbour_count AS 
        SELECT other_vertex AS id, SUM(num_neighbour) AS num_neighbour
        FROM old_in_neighbour_count JOIN public.g_oe
        ON old_in_neighbour_count.id = g_oe.owner_vertex 
        AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value || ' GROUP BY other_vertex
        DISTRIBUTED BY (id)';

        DROP TABLE old_in_neighbour_count;

        SELECT COUNT(1) INTO num2update FROM new_in_neighbour_count;

        current_hop := current_hop + 1;

    END LOOP;

    DROP TABLE IF EXISTS new_in_neighbour_count;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT id, max(num_neighbour) AS attr FROM in_neighbour_count_t GROUP BY id
    DISTRIBUTED BY (id)';

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
    -- SELECT "in_neighbour_count" INTO in_hop FROM public.in_neighbour_count(k_hop, 'public.neighbour_count_t1');
    SELECT public.in_neighbour_count(k_hop, 'public.neighbour_count_t1') INTO in_hop;
    -- SELECT "out_neighbour_count" INTO out_hop FROM public.out_neighbour_count(k_hop, 'public.neighbour_count_t2');
    SELECT public.out_neighbour_count(k_hop, 'public.neighbour_count_t2') INTO out_hop;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT 
        neighbour_count_t1.id, 
        COALESCE(neighbour_count_t1.attr, 0)+COALESCE(neighbour_count_t2.attr, 0) AS attr
    FROM public.neighbour_count_t1 FULL JOIN public.neighbour_count_t2 USING (id)
    DISTRIBUTED BY (id)';

    DROP TABLE IF EXISTS public.neighbour_count_t1, public.neighbour_count_t2;

    RETURN NEXT in_hop;
    RETURN NEXT out_hop;

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
    SELECT public.in_neighbour_count(k_hop, edge_type_column, edge_type_value, 'public.neighbour_count_t1') INTO in_hop;

    SELECT public.out_neighbour_count(k_hop, edge_type_column, edge_type_value, 'public.neighbour_count_t2') INTO out_hop;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' AS
    SELECT 
        neighbour_count_t1.id, 
        COALESCE(neighbour_count_t1.attr, 0)+COALESCE(neighbour_count_t2.attr, 0) AS attr
    FROM public.neighbour_count_t1 FULL JOIN public.neighbour_count_t2 USING (id)
    DISTRIBUTED BY (id)';

    DROP TABLE IF EXISTS public.neighbour_count_t1, public.neighbour_count_t2;

    RETURN NEXT in_hop;
    RETURN NEXT out_hop;

END;
$$ LANGUAGE plpgsql;