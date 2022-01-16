CREATE OR REPLACE FUNCTION public.triangle_count(
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
BEGIN
    CREATE TEMP TABLE nbrs_t1 WITH (appendoptimized=TRUE,orientation=ROW) AS
    SELECT edges_mirror.src_id, edges_mirror.dst_id, edges.dst_id AS nbr_of_dst
    FROM public.edges_mirror JOIN public.edges 
    ON edges_mirror.dst_id = edges.src_id AND edges.dst_id <> edges_mirror.src_id
    -- UNION
    -- SELECT t1.src_id, t1.dst_id, t2.src_id
    -- FROM public.edges_mirror AS t1 JOIN public.edges_mirror AS t2
    -- ON t1.dst_id = t2.dst_id and t1.src_id <> t2.src_id
    DISTRIBUTED BY (src_id);

    INSERT INTO nbrs_t1
    SELECT t1.src_id, t1.dst_id, t2.src_id
    FROM public.edges_mirror AS t1 JOIN public.edges_mirror AS t2
    ON t1.dst_id = t2.dst_id and t1.src_id <> t2.src_id;

    CREATE TEMP TABLE nbrs_t2 WITH (appendoptimized=TRUE,orientation=ROW) AS
    SELECT nbrs_t1.src_id, nbrs_t1.dst_id
    FROM public.edges_mirror JOIN nbrs_t1
    ON edges_mirror.dst_id = nbrs_t1.src_id AND edges_mirror.src_id = nbrs_t1.nbr_of_dst
    -- UNION 
    -- SELECT nbrs_t1.src_id, nbrs_t1.dst_id
    -- FROM public.edges JOIN nbrs_t1
    -- ON edges.src_id = nbrs_t1.src_id AND edges.dst_id = nbrs_t1.nbr_of_dst
    DISTRIBUTED BY (src_id);

    INSERT INTO nbrs_t2
    SELECT nbrs_t1.src_id, nbrs_t1.dst_id
    FROM public.edges JOIN nbrs_t1
    ON edges.src_id = nbrs_t1.src_id AND edges.dst_id = nbrs_t1.nbr_of_dst;

    DROP TABLE IF EXISTS nbrs_t1;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' WITH (appendoptimized=TRUE,orientation=ROW) AS
    SELECT vid, (COALESCE(t1.num_triangle, 0)+COALESCE(t2.num_triangle, 0))/2 AS attr FROM
    (SELECT src_id AS vid, COUNT(1) AS num_triangle FROM nbrs_t2 GROUP BY src_id) AS t1
    FULL JOIN
    (SELECT dst_id AS vid, COUNT(1) AS num_triangle FROM nbrs_t2 GROUP BY dst_id) AS t2
    USING (vid)
    DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS nbrs_t2;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.triangle_count(
    IN edge_type_column VARCHAR, 
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
BEGIN
    EXECUTE
    'CREATE TEMP TABLE nbrs_t1 WITH (appendoptimized=TRUE,orientation=ROW) AS
    SELECT edges_mirror.src_id, edges_mirror.dst_id, edges.dst_id AS nbr_of_dst
    FROM public.edges_mirror JOIN public.edges 
    ON edges_mirror.dst_id = edges.src_id AND edges.dst_id <> edges_mirror.src_id
    AND edges_mirror.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
    ' AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
    ' DISTRIBUTED BY (src_id)';

    EXECUTE
    'INSERT INTO nbrs_t1
    SELECT t1.src_id, t1.dst_id, t2.src_id
    FROM public.edges_mirror AS t1 JOIN public.edges_mirror AS t2
    ON t1.dst_id = t2.dst_id and t1.src_id <> t2.src_id
    AND t1.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
    ' AND t2.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

    EXECUTE
    'CREATE TEMP TABLE nbrs_t2 WITH (appendoptimized=TRUE,orientation=ROW) AS
    SELECT nbrs_t1.src_id, nbrs_t1.dst_id
    FROM public.edges_mirror JOIN nbrs_t1
    ON edges_mirror.dst_id = nbrs_t1.src_id AND edges_mirror.src_id = nbrs_t1.nbr_of_dst
    AND edges_mirror.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
    ' DISTRIBUTED BY (src_id)';

    EXECUTE
    'INSERT INTO nbrs_t2
    SELECT nbrs_t1.src_id, nbrs_t1.dst_id
    FROM public.edges JOIN nbrs_t1
    ON edges.src_id = nbrs_t1.src_id AND edges.dst_id = nbrs_t1.nbr_of_dst
    AND edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value;

    DROP TABLE IF EXISTS nbrs_t1;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' WITH (appendoptimized=TRUE,orientation=ROW) AS
    SELECT vid, (COALESCE(t1.num_triangle, 0)+COALESCE(t2.num_triangle, 0))/2 AS attr FROM
    (SELECT src_id AS vid, COUNT(1) AS num_triangle FROM nbrs_t2 GROUP BY src_id) AS t1
    FULL JOIN
    (SELECT dst_id AS vid, COUNT(1) AS num_triangle FROM nbrs_t2 GROUP BY dst_id) AS t2
    USING (vid)
    DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS nbrs_t2;

END;
$$ LANGUAGE plpgsql;


-- *** another implementation *** --

CREATE OR REPLACE FUNCTION public.intersection_count(
    IN arr1 INTEGER[],
    IN arr2 INTEGER[]
) RETURNS INTEGER AS $$
DECLARE
    arr2loop INTEGER[];
    arr2rm INTEGER[];
    ele INTEGER;
    original_length INTEGER;
BEGIN
    IF array_length(arr1, 1) < array_length(arr2, 1) THEN
        arr2loop := arr1;
        arr2rm := arr2;
    ELSE
        arr2loop := arr2;
        arr2rm := arr1;
    END IF;
    SELECT array_length(arr2rm, 1) INTO original_length;
    FOREACH ele IN ARRAY arr2loop LOOP
        arr2rm := array_remove(arr2rm, ele);
    END LOOP;
    RETURN original_length - array_length(arr2rm, 1);
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION public.triangle_count_v2(
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
BEGIN
    CREATE TEMP TABLE nbrs_t1 WITH (appendoptimized=TRUE,orientation=ROW) AS
    SELECT vid, t1.nbrs || t2.nbrs AS nbrs FROM
    (SELECT src_id AS vid, ARRAY_AGG(dst_id) AS nbrs FROM public.edges GROUP BY src_id) AS t1
    FULL JOIN
    (SELECT dst_id AS vid, ARRAY_AGG(src_id) AS nbrs FROM public.edges_mirror GROUP BY dst_id) AS t2
    USING (vid) DISTRIBUTED BY (vid);

    CREATE TEMP TABLE nbrs_t2 WITH (appendoptimized=TRUE,orientation=ROW) AS
    SELECT edges.src_id, edges.dst_id, public.intersection_count(nbrs_t1.nbrs, t1.nbrs) AS num_triangle FROM
    nbrs_t1 JOIN public.edges ON nbrs_t1.vid = edges.src_id
    JOIN nbrs_t1 AS t1 ON t1.vid = edges.dst_id
    DISTRIBUTED BY (src_id);

    DROP TABLE IF EXISTS nbrs_t1;

    EXECUTE
    'CREATE UNLOGGED TABLE ' || result_table_name || ' WITH (appendoptimized=TRUE,orientation=ROW) AS
    SELECT vid, (COALESCE(t1.num_triangle, 0)+COALESCE(t2.num_triangle, 0))/2 AS attr FROM
    (SELECT src_id AS vid, SUM(num_triangle) AS num_triangle FROM nbrs_t2 GROUP BY src_id) AS t1
    FULL JOIN
    (SELECT dst_id AS vid, SUM(num_triangle) AS num_triangle FROM nbrs_t2 GROUP BY dst_id) AS t2
    USING (vid) DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS nbrs_t2;

END;
$$ LANGUAGE plpgsql;
