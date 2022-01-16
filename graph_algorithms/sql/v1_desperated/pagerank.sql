CREATE OR REPLACE FUNCTION public.pagerank(
    IN beta REAL, 
    IN delta_min REAL,
    IN max_iter INTEGER,
    IN result_table_name VARCHAR
) RETURNS INTEGER AS $$
DECLARE
    initial_rank REAL;
    jp_prob REAL;
    num_unconv INTEGER := 1;
    current_iter INTEGER := 0; 
BEGIN
    
    SELECT 1. / COUNT(1) INTO initial_rank FROM public.vertices;
    jp_prob := beta * initial_rank;

    DROP TABLE IF EXISTS public.pagerank_t;

    CREATE TABLE public.pagerank_t AS 
    SELECT vid, initial_rank AS pr FROM vertices
    DISTRIBUTED BY (vid);

    CREATE TEMPORARY TABLE v_zero_in ON COMMIT DROP AS 
    SELECT vid, initial_rank AS pr FROM vertices WHERE in_degree = 0
    DISTRIBUTED BY (vid);

    WHILE num_unconv > 0 AND current_iter < max_iter LOOP
        CREATE TABLE public.msg AS 
        SELECT edges.dst_id AS vid, (1-beta)*SUM(pagerank_t.pr/vertices.out_degree)+jp_prob AS pr
        FROM public.edges JOIN public.pagerank_t ON edges.src_id=pagerank_t.vid 
        JOIN public.vertices ON edges.src_id = vertices.vid 
        GROUP BY dst_id DISTRIBUTED by (vid);

        SELECT COUNT(1) INTO num_unconv FROM public.msg JOIN public.pagerank_t 
        ON msg.vid=pagerank_t.vid AND ABS(msg.pr-pagerank_t.pr) > delta_min;

        DROP TABLE public.pagerank_t;
        ALTER TABLE public.msg RENAME TO pagerank_t;
        INSERT INTO public.pagerank_t SELECT * FROM v_zero_in;

        current_iter := current_iter + 1;

    END LOOP;

    ALTER TABLE public.pagerank_t RENAME COLUMN pr TO attr;

    EXECUTE 'ALTER TABLE public.pagerank_t RENAME TO ' || result_table_name;

    RETURN current_iter;

END;
$$ LANGUAGE plpgsql;