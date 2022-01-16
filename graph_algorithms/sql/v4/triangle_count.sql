CREATE OR REPLACE FUNCTION public.triangle_count(
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
BEGIN
    CREATE TEMPORARY TABLE nbrs_t1 WITH (appendoptimized=TRUE,orientation=ROW) ON COMMIT DROP AS
    SELECT g_ie.other_vertex AS src_id, g_ie.owner_vertex AS dst_id, g_oe.other_vertex AS nbr_of_dst
    FROM public.g_ie JOIN public.g_oe 
    ON g_ie.owner_vertex = g_oe.owner_vertex AND g_ie.other_vertex <> g_oe.other_vertex
    DISTRIBUTED BY (src_id);

    INSERT INTO nbrs_t1
    SELECT t1.other_vertex, t1.owner_vertex, t2.other_vertex
    FROM public.g_ie AS t1 JOIN public.g_ie AS t2
    ON t1.owner_vertex = t2.owner_vertex and t1.other_vertex <> t2.other_vertex;

    CREATE TEMPORARY TABLE nbrs_t2 WITH (appendoptimized=TRUE,orientation=ROW) ON COMMIT DROP AS
    SELECT nbrs_t1.src_id, nbrs_t1.dst_id
    FROM public.g_ie JOIN nbrs_t1
    ON g_ie.owner_vertex = nbrs_t1.src_id AND g_ie.other_vertex = nbrs_t1.nbr_of_dst
    DISTRIBUTED BY (src_id);

    INSERT INTO nbrs_t2
    SELECT nbrs_t1.src_id, nbrs_t1.dst_id
    FROM public.g_oe JOIN nbrs_t1
    ON g_oe.owner_vertex = nbrs_t1.src_id AND g_oe.other_vertex = nbrs_t1.nbr_of_dst;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' WITH (appendoptimized=TRUE,orientation=ROW) AS
    SELECT id, (COALESCE(t1.num_triangle, 0)+COALESCE(t2.num_triangle, 0))/2 AS attr FROM
    (SELECT src_id AS id, COUNT(1) AS num_triangle FROM nbrs_t2 GROUP BY src_id) AS t1
    FULL JOIN
    (SELECT dst_id AS id, COUNT(1) AS num_triangle FROM nbrs_t2 GROUP BY dst_id) AS t2
    USING (id)
    DISTRIBUTED BY (id)';

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.triangle_count(
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
BEGIN
    IF edge_type_column IS NULL OR edge_type_value IS NULL THEN
        PERFORM public.triangle_count(result_table_name);
    ELSE
        EXECUTE
        'CREATE TEMPORARY TABLE nbrs_t1 WITH (appendoptimized=TRUE,orientation=ROW) ON COMMIT DROP AS
        SELECT g_ie.other_vertex AS src_id, g_ie.owner_vertex AS dst_id, g_oe.other_vertex AS nbr_of_dst
        FROM public.g_ie JOIN public.g_oe 
        ON g_ie.owner_vertex = g_oe.owner_vertex AND g_ie.other_vertex <> g_oe.other_vertex
        AND g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
        ' AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
        ' DISTRIBUTED BY (src_id)';

        EXECUTE
        'INSERT INTO nbrs_t1
        SELECT t1.other_vertex, t1.owner_vertex, t2.other_vertex
        FROM public.g_ie AS t1 JOIN public.g_ie AS t2
        ON t1.owner_vertex = t2.owner_vertex and t1.other_vertex <> t2.other_vertex
        AND t1.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
        ' AND t2.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

        EXECUTE
        'CREATE TEMP TABLE nbrs_t2 WITH (appendoptimized=TRUE,orientation=ROW) ON COMMIT DROP AS
        SELECT nbrs_t1.src_id, nbrs_t1.dst_id
        FROM public.g_ie JOIN nbrs_t1
        ON g_ie.owner_vertex = nbrs_t1.src_id AND g_ie.other_vertex = nbrs_t1.nbr_of_dst
        AND g_ie.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
        ' DISTRIBUTED BY (src_id)';

        EXECUTE
        'INSERT INTO nbrs_t2
        SELECT nbrs_t1.src_id, nbrs_t1.dst_id
        FROM public.g_oe JOIN nbrs_t1
        ON g_oe.owner_vertex = nbrs_t1.src_id AND g_oe.other_vertex = nbrs_t1.nbr_of_dst
        AND g_oe.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

        EXECUTE
        'CREATE UNLOGGED TABLE ' || result_table_name || ' WITH (appendoptimized=TRUE,orientation=ROW) AS
        SELECT id, (COALESCE(t1.num_triangle, 0)+COALESCE(t2.num_triangle, 0))/2 AS attr FROM
        (SELECT src_id AS id, COUNT(1) AS num_triangle FROM nbrs_t2 GROUP BY src_id) AS t1
        FULL JOIN
        (SELECT dst_id AS id, COUNT(1) AS num_triangle FROM nbrs_t2 GROUP BY dst_id) AS t2
        USING (id)
        DISTRIBUTED BY (id)';
    END IF;
END;
$$ LANGUAGE plpgsql;

