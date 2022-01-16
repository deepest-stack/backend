CREATE OR REPLACE FUNCTION public.load_edges(
    IN edge_file VARCHAR
) RETURNS VOID AS $$
BEGIN
    CREATE TABLE IF NOT EXISTS public.edges(src_id INT, dst_id INT) DISTRIBUTED BY (src_id);
    EXECUTE 'COPY public.edges(src_id, dst_id) FROM ''' || edge_file || ''' WITH csv DELIMITER AS '' ''';

    CREATE TABLE IF NOT EXISTS public.edges_mirror(src_id INT, dst_id INT) DISTRIBUTED BY (dst_id);
    EXECUTE 'COPY public.edges_mirror(src_id, dst_id) FROM ''' || edge_file || ''' WITH csv DELIMITER AS '' ''';
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.init_vertices(
) RETURNS VOID AS $$
BEGIN
    CREATE TABLE IF NOT EXISTS public.vertices(vid INT, in_degree INT, out_degree INT) DISTRIBUTED BY (vid);
    INSERT INTO public.vertices(vid, in_degree, out_degree)
    SELECT COALESCE(t1.dst_id, t2.src_id), COALESCE(t1."count", 0), COALESCE(t2."count", 0) FROM
    (SELECT dst_id, COUNT(1) FROM public.edges_mirror GROUP BY dst_id) AS t1
    FULL JOIN
    (SELECT src_id, COUNT(1) FROM public.edges GROUP BY src_id) AS t2
    ON t1.dst_id = t2.src_id;
END;
$$ LANGUAGE plpgsql;