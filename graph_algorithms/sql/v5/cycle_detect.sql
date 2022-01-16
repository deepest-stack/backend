-- dependency: utils.sql

CREATE OR REPLACE FUNCTION ${graph_name}.cycle_detect_in(
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_zero_in_v INTEGER;
BEGIN
    DROP TABLE IF EXISTS zero_in_v;
    CREATE TEMP TABLE zero_in_v AS
    SELECT vid FROM ${graph_name}.vertex_all WHERE in_degree = 0
    DISTRIBUTED BY (vid);

    DROP TABLE IF EXISTS vertex_in_deg;
    CREATE TEMP TABLE vertex_in_deg AS
    SELECT vid, in_degree FROM ${graph_name}.vertex_all 
    WHERE in_degree > 0
    DISTRIBUTED BY (vid);

    SELECT count(1) INTO num_zero_in_v FROM zero_in_v;

    WHILE num_zero_in_v > 0 LOOP
        UPDATE vertex_in_deg SET in_degree=vertex_in_deg.in_degree-t1.in_degree FROM
        (SELECT dst_id, count(1) AS in_degree FROM
        zero_in_v JOIN ${graph_name}.edge_all 
        ON zero_in_v.vid = edge_all.src_id GROUP BY dst_id) AS t1
        WHERE vertex_in_deg.vid = t1.dst_id;

        DROP TABLE zero_in_v;

        CREATE TEMP TABLE zero_in_v AS
        SELECT vid FROM vertex_in_deg WHERE in_degree = 0
        DISTRIBUTED BY (vid);

        SELECT count(1) INTO num_zero_in_v FROM zero_in_v;

        DELETE FROM vertex_in_deg WHERE in_degree = 0;
    END LOOP;

    EXECUTE 'CREATE TEMP TABLE ' || result_table_name || ' AS SELECT vid FROM vertex_in_deg DISTRIBUTED BY (vid)';
    DROP TABLE IF EXISTS zero_in_v;
    DROP TABLE IF EXISTS vertex_in_deg;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${graph_name}.cycle_detect_in(
    IN edge_type VARCHAR,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_zero_in_v INTEGER;
BEGIN

    DROP TABLE IF EXISTS vertex_in_deg;
    EXECUTE
    'CREATE TEMP TABLE vertex_in_deg AS
    SELECT dst_id AS vid, count(1) AS in_degree FROM ${graph_name}.edge_'||edge_type||
    ' GROUP BY dst_id DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS zero_in_v;
    CREATE TEMP TABLE zero_in_v AS
    SELECT vid FROM ${graph_name}.vertex_all EXCEPT SELECT vid FROM vertex_in_deg
    DISTRIBUTED BY (vid);

    SELECT count(1) INTO num_zero_in_v FROM zero_in_v;

    WHILE num_zero_in_v > 0 LOOP
        EXECUTE
        'UPDATE vertex_in_deg SET in_degree=vertex_in_deg.in_degree-t1.in_degree FROM
        (SELECT dst_id, count(1) AS in_degree FROM
        zero_in_v JOIN ${graph_name}.edge_'||edge_type||' ON zero_in_v.vid = edge_'||edge_type||'.src_id GROUP BY dst_id) AS t1
        WHERE vertex_in_deg.vid = t1.dst_id';

        DROP TABLE zero_in_v;

        CREATE TEMP TABLE zero_in_v AS
        SELECT vid FROM vertex_in_deg WHERE in_degree = 0
        DISTRIBUTED BY (vid);

        SELECT count(1) INTO num_zero_in_v FROM zero_in_v;

        DELETE FROM vertex_in_deg WHERE in_degree = 0;
    END LOOP;

    EXECUTE 'CREATE TEMP TABLE ' || result_table_name || ' AS SELECT vid FROM vertex_in_deg DISTRIBUTED BY (vid)';
    DROP TABLE IF EXISTS zero_in_v;
    DROP TABLE IF EXISTS vertex_in_deg;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${graph_name}.cycle_detect_out(
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_zero_out_v INTEGER;
BEGIN
    DROP TABLE IF EXISTS zero_out_v;
    CREATE TEMP TABLE zero_out_v AS
    SELECT vid FROM ${graph_name}.vertex_all WHERE out_degree = 0
    DISTRIBUTED BY (vid);

    DROP TABLE IF EXISTS vertex_out_deg;
    CREATE TEMP TABLE vertex_out_deg AS
    SELECT vid, out_degree FROM ${graph_name}.vertex_all 
    WHERE out_degree > 0
    DISTRIBUTED BY (vid);

    SELECT count(1) INTO num_zero_out_v FROM zero_out_v;

    WHILE num_zero_out_v > 0 LOOP
        UPDATE vertex_out_deg SET out_degree=vertex_out_deg.out_degree-t1.out_degree FROM
        (SELECT src_id, count(1) AS out_degree FROM
        zero_out_v JOIN ${graph_name}.edge_all ON zero_out_v.vid = edge_all.dst_id GROUP BY src_id) AS t1
        WHERE vertex_out_deg.vid = t1.src_id;

        DROP TABLE zero_out_v;

        CREATE TEMP TABLE zero_out_v AS
        SELECT vid FROM vertex_out_deg WHERE out_degree = 0
        DISTRIBUTED BY (vid);

        SELECT count(1) INTO num_zero_out_v FROM zero_out_v;

        DELETE FROM vertex_out_deg WHERE out_degree = 0;
    END LOOP;

    EXECUTE 'CREATE TEMP TABLE ' || result_table_name || ' AS SELECT vid FROM vertex_out_deg DISTRIBUTED BY (vid)';
    DROP TABLE IF EXISTS zero_out_v;
    DROP TABLE IF EXISTS vertex_out_deg;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${graph_name}.cycle_detect_out(
    IN edge_type VARCHAR,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_zero_out_v INTEGER;
BEGIN

    DROP TABLE IF EXISTS vertex_out_deg;
    EXECUTE
    'CREATE TEMP TABLE vertex_out_deg AS
    SELECT src_id AS vid, count(1) AS out_degree FROM ${graph_name}.edge_'||edge_type||
    ' GROUP BY src_id DISTRIBUTED BY (vid)';

    DROP TABLE IF EXISTS zero_out_v;
    CREATE TEMP TABLE zero_out_v AS
    SELECT vid FROM ${graph_name}.vertex_all EXCEPT SELECT vid FROM vertex_out_deg
    DISTRIBUTED BY (vid);

    SELECT count(1) INTO num_zero_out_v FROM zero_out_v;

    WHILE num_zero_out_v > 0 LOOP
        EXECUTE
        'UPDATE vertex_out_deg SET out_degree=vertex_out_deg.out_degree-t1.out_degree FROM
        (SELECT src_id, count(1) AS out_degree FROM
        zero_out_v JOIN ${graph_name}.edge_'||edge_type||' ON zero_out_v.vid = edge_'||edge_type||'.dst_id GROUP BY src_id) AS t1
        WHERE vertex_out_deg.vid = t1.src_id';

        DROP TABLE zero_out_v;

        CREATE TEMP TABLE zero_out_v AS
        SELECT vid FROM vertex_out_deg WHERE out_degree = 0
        DISTRIBUTED BY (vid);

        SELECT count(1) INTO num_zero_out_v FROM zero_out_v;

        DELETE FROM vertex_out_deg WHERE out_degree = 0;
    END LOOP;

    EXECUTE 'CREATE TEMP TABLE ' || result_table_name || ' AS SELECT vid FROM vertex_out_deg DISTRIBUTED BY (vid)';
    DROP TABLE IF EXISTS zero_out_v;
    DROP TABLE IF EXISTS vertex_out_deg;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${graph_name}.cycle_detect(
    IN max_length INTEGER,
    IN num_limit INTEGER,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER := 1;
    num_v_left INTEGER;
    num_cycles_detected INTEGER := 0;
    current_vid BIGINT;
    current_loop INTEGER;
BEGIN
    DROP TABLE IF EXISTS cycle_detect_tmp_t1;
    PERFORM ${graph_name}.cycle_detect_in('cycle_detect_tmp_t1');

    DROP TABLE IF EXISTS cycle_detect_tmp_t2;
    PERFORM ${graph_name}.cycle_detect_out('cycle_detect_tmp_t2');

    DROP TABLE IF EXISTS v_in_cycle;
    CREATE TEMP TABLE v_in_cycle
    AS SELECT cycle_detect_tmp_t1.vid FROM cycle_detect_tmp_t1 JOIN cycle_detect_tmp_t2
    USING (vid) DISTRIBUTED BY (vid);

    DROP TABLE IF EXISTS cycle_detect_tmp_t1;
    DROP TABLE IF EXISTS cycle_detect_tmp_t2;

    DROP TABLE IF EXISTS e_in_cycle;
    CREATE TEMP TABLE e_in_cycle
    AS SELECT t1.src_id, t1.dst_id, t1.etype FROM
    (SELECT edge_all.src_id, edge_all.dst_id, edge_all.etype 
    FROM ${graph_name}.edge_all JOIN v_in_cycle ON edge_all.src_id = v_in_cycle.vid) AS t1
    JOIN v_in_cycle ON t1.dst_id = v_in_cycle.vid
    DISTRIBUTED BY (src_id);

    DROP TABLE IF EXISTS cycles_detected;
    CREATE TEMP TABLE cycles_detected(
        path BIGINT[],
        edges_type VARCHAR[]
    ) WITH (appendoptimized=TRUE,orientation=row) 
    DISTRIBUTED RANDOMLY;

    SELECT count(1) INTO num_v_left FROM v_in_cycle;
    WHILE num_v_left > 0 LOOP
        DROP TABLE IF EXISTS paths;
        CREATE TEMP TABLE paths(
            path BIGINT[],
            edges_type VARCHAR[],
            tail_vid BIGINT
        ) DISTRIBUTED BY (tail_vid);
        SELECT v_in_cycle.vid INTO current_vid FROM v_in_cycle LIMIT 1;
        INSERT INTO paths VALUES(ARRAY[current_vid], ARRAY[]::VARCHAR[], current_vid);
        DELETE FROM v_in_cycle WHERE vid = current_vid;
        current_loop := 1;

        WHILE num_growing_path > 0 AND (num_limit < 0 OR num_limit > num_cycles_detected)
            AND (max_length < 0 OR max_length >= current_loop) LOOP
            CREATE TEMP TABLE paths_tmp AS
            SELECT paths.path||e_in_cycle.dst_id AS path,
                paths.edges_type||e_in_cycle.etype AS edges_type,
                e_in_cycle.dst_id AS tail_vid,
                ${graph_name}.indexof(paths.path, e_in_cycle.dst_id) AS idx_of_tail_vid
            FROM paths JOIN e_in_cycle ON paths.tail_vid = e_in_cycle.src_id
            DISTRIBUTED BY (tail_vid);
            current_loop := current_loop + 1;
            DELETE FROM v_in_cycle USING (SELECT DISTINCT(tail_vid) AS tail_vid FROM paths_tmp) AS t1 WHERE vid = t1.tail_vid;
            INSERT INTO cycles_detected SELECT path[idx_of_tail_vid:current_loop], edges_type[idx_of_tail_vid:current_loop] 
            FROM paths_tmp WHERE idx_of_tail_vid > 0;
            DROP TABLE paths;
            CREATE TEMP TABLE paths AS
            SELECT path, edges_type, tail_vid 
            FROM paths_tmp WHERE idx_of_tail_vid = 0
            DISTRIBUTED BY (tail_vid);
            DROP TABLE paths_tmp;
            SELECT count(1) INTO num_growing_path FROM paths;
            SELECT count(1) INTO num_cycles_detected FROM cycles_detected;
        END LOOP;

        IF num_limit > 0 AND num_cycles_detected >= num_limit THEN
            EXIT;
        END IF;

        SELECT count(1) INTO num_v_left FROM v_in_cycle;
    END LOOP;

    DROP TABLE IF EXISTS v_in_cycle;
    DROP TABLE IF EXISTS e_in_cycle;
    DROP TABLE IF EXISTS paths;

    EXECUTE 'ALTER TABLE cycles_detected RENAME TO '||result_table_name;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ${graph_name}.cycle_detect(
    IN max_length INTEGER,
    IN num_limit INTEGER,
    IN edge_type VARCHAR,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    num_growing_path INTEGER := 1;
    num_v_left INTEGER;
    num_cycles_detected INTEGER := 0;
    current_vid BIGINT;
    current_loop INTEGER;
BEGIN
    DROP TABLE IF EXISTS cycle_detect_tmp_t1;
    PERFORM ${graph_name}.cycle_detect_in(edge_type, 'cycle_detect_tmp_t1');

    DROP TABLE IF EXISTS cycle_detect_tmp_t2;
    PERFORM ${graph_name}.cycle_detect_out(edge_type, 'cycle_detect_tmp_t2');

    DROP TABLE IF EXISTS v_in_cycle;
    CREATE TEMP TABLE v_in_cycle
    AS SELECT cycle_detect_tmp_t1.vid FROM cycle_detect_tmp_t1 JOIN cycle_detect_tmp_t2
    USING (vid) DISTRIBUTED BY (vid);

    DROP TABLE IF EXISTS cycle_detect_tmp_t1;
    DROP TABLE IF EXISTS cycle_detect_tmp_t2;

    DROP TABLE IF EXISTS e_in_cycle;
    EXECUTE
    'CREATE TEMP TABLE e_in_cycle
    AS SELECT t1.src_id, t1.dst_id FROM
    (SELECT edge_'||edge_type||'.src_id, edge_'||edge_type||'.dst_id
    FROM ${graph_name}.edge_'||edge_type||' JOIN v_in_cycle ON edge_'||edge_type||'.src_id = v_in_cycle.vid) AS t1
    JOIN v_in_cycle ON t1.dst_id = v_in_cycle.vid
    DISTRIBUTED BY (src_id)';

    DROP TABLE IF EXISTS cycles_detected;
    CREATE TEMP TABLE cycles_detected(
        path BIGINT[]
    ) WITH (appendoptimized=TRUE,orientation=row) 
    DISTRIBUTED RANDOMLY;

    SELECT count(1) INTO num_v_left FROM v_in_cycle;
    WHILE num_v_left > 0 LOOP
        DROP TABLE IF EXISTS paths;
        CREATE TEMP TABLE paths(
            path BIGINT[],
            tail_vid BIGINT
        ) DISTRIBUTED BY (tail_vid);
        SELECT v_in_cycle.vid INTO current_vid FROM v_in_cycle LIMIT 1;
        INSERT INTO paths VALUES(ARRAY[current_vid], current_vid);
        DELETE FROM v_in_cycle WHERE vid = current_vid;
        current_loop := 1;

        WHILE num_growing_path > 0 AND (num_limit < 0 OR num_limit > num_cycles_detected)
            AND (max_length < 0 OR max_length >= current_loop) LOOP
            CREATE TEMP TABLE paths_tmp AS
            SELECT paths.path||e_in_cycle.dst_id AS path,
                e_in_cycle.dst_id AS tail_vid,
                ${graph_name}.indexof(paths.path, e_in_cycle.dst_id) AS idx_of_tail_vid
            FROM paths JOIN e_in_cycle ON paths.tail_vid = e_in_cycle.src_id
            DISTRIBUTED BY (tail_vid);
            current_loop := current_loop + 1;
            DELETE FROM v_in_cycle USING (SELECT DISTINCT(tail_vid) AS tail_vid FROM paths_tmp) AS t1 WHERE vid = t1.tail_vid;
            INSERT INTO cycles_detected SELECT path[idx_of_tail_vid:current_loop]
            FROM paths_tmp WHERE idx_of_tail_vid > 0;
            DROP TABLE paths;
            CREATE TEMP TABLE paths AS
            SELECT path, tail_vid 
            FROM paths_tmp WHERE idx_of_tail_vid = 0
            DISTRIBUTED BY (tail_vid);
            DROP TABLE paths_tmp;
            SELECT count(1) INTO num_growing_path FROM paths;
            SELECT count(1) INTO num_cycles_detected FROM cycles_detected;
        END LOOP;

        IF num_limit > 0 AND num_cycles_detected >= num_limit THEN
            EXIT;
        END IF;

        SELECT count(1) INTO num_v_left FROM v_in_cycle;
    END LOOP;

    DROP TABLE IF EXISTS v_in_cycle;
    DROP TABLE IF EXISTS e_in_cycle;
    DROP TABLE IF EXISTS paths;

    EXECUTE 'ALTER TABLE cycles_detected RENAME TO '||result_table_name;

END;
$$ LANGUAGE plpgsql;

