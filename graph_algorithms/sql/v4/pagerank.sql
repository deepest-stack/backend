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
    
    SELECT 1. / COUNT(1) INTO initial_rank FROM public.g_v;
    jp_prob := beta * initial_rank;

    DROP TABLE IF EXISTS public.pagerank_t;

    CREATE UNLOGGED TABLE public.pagerank_t AS 
    SELECT id, initial_rank AS pr FROM public.g_v
    DISTRIBUTED BY (id);

    CREATE TEMPORARY TABLE v_zero_in ON COMMIT DROP AS 
    SELECT t1.owner_vertex as id, initial_rank AS pr FROM
    (SELECT owner_vertex FROM public.g_oe EXCEPT (SELECT owner_vertex FROM public.g_ie)) AS t1
    DISTRIBUTED BY (id);

    CREATE TEMPORARY TABLE v_out_degree ON COMMIT DROP AS 
    SELECT g_oe.owner_vertex AS id, COUNT(1) AS degree
    FROM public.g_oe GROUP BY owner_vertex
    DISTRIBUTED BY (id);

    WHILE num_unconv > 0 AND current_iter < max_iter LOOP
        CREATE UNLOGGED TABLE public.msg AS 
        SELECT g_oe.other_vertex AS id, (1-beta)*SUM(pagerank_t.pr/v_out_degree.degree)+jp_prob AS pr
        FROM public.g_oe JOIN public.pagerank_t ON g_oe.owner_vertex=pagerank_t.id 
        JOIN v_out_degree ON g_oe.owner_vertex = v_out_degree.id 
        GROUP BY other_vertex DISTRIBUTED by (id);

        SELECT COUNT(1) INTO num_unconv FROM public.msg JOIN public.pagerank_t 
        ON msg.id=pagerank_t.id AND ABS(msg.pr-pagerank_t.pr) > delta_min;

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
