CREATE OR REPLACE FUNCTION public.gen_rand_num(
    IN seed BIGINT
) RETURNS INTEGER AS $$
BEGIN
    RETURN (48271 * seed) %  255;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION public.modularity(
    IN norm REAL
) RETURNS REAL AS $$
DECLARE
    q REAL;
BEGIN
    SELECT SUM(num_edges - tot_degree/norm/2*tot_degree)/2/norm INTO q FROM community_info;
    RETURN q;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.convert2canonical(
    IN table2convert VARCHAR
) RETURNS VOID AS $$
BEGIN
    EXECUTE
    'CREATE TEMP TABLE tmp_t1 AS
    SELECT t1.src_id, t1.dst_id, SUM(weight) AS weight FROM
    (SELECT src_id, dst_id, weight FROM ' || table2convert || ' WHERE src_id <= dst_id
    UNION
    SELECT dst_id AS src_id, src_id AS dst_id, weight FROM ' || table2convert || ' WHERE src_id > dst_id) AS t1
    GROUP BY src_id, dst_id
    DISTRIBUTED BY (src_id)';

    EXECUTE 'DROP TABLE ' || table2convert;

    EXECUTE 'ALTER TABLE tmp_t1 RENAME TO ' || table2convert;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.send_msg(
    IN current_loop INTEGER
) RETURNS VOID AS $$
BEGIN
    -- v_send_msg contains vertex that can send msg and its community
    -- v_receive_msg contains vertex that can receive msg, i.e. vertex whose community has not update yet
    -- odd --> even
    IF current_loop % 2 = 0 THEN
        CREATE TEMP TABLE v_send_msg AS
        SELECT vid, cid FROM vertex_state WHERE rand_num % 2 = 1
        DISTRIBUTED BY (vid);
        CREATE TEMP TABLE v_receive_msg AS
        SELECT vid FROM vertex_state WHERE rand_num % 2 = 0 AND community_updated = 0
        DISTRIBUTED BY (vid);
    -- even --> odd
    ELSE
        CREATE TEMP TABLE v_send_msg AS
        SELECT vid, cid FROM vertex_state WHERE rand_num % 2 = 0
        DISTRIBUTED BY (vid);
        CREATE TEMP TABLE v_receive_msg AS
        SELECT vid FROM vertex_state WHERE rand_num % 2 = 1 AND community_updated = 0
        DISTRIBUTED BY (vid);
    END IF;

    -- message_tmp contains vertex that can receive msg and the new communities to choose from
    CREATE TEMP TABLE message_tmp AS
    SELECT DISTINCT t1.vid, t1.cid FROM
    (SELECT louvain_edges.src_id AS vid, v_send_msg.cid FROM
    v_receive_msg JOIN louvain_edges ON louvain_edges.src_id = v_receive_msg.vid
    JOIN v_send_msg ON v_send_msg.vid = louvain_edges.dst_id
    UNION
    SELECT louvain_edges_mirror.dst_id AS vid, v_send_msg.cid FROM
    v_receive_msg JOIN louvain_edges_mirror ON louvain_edges_mirror.dst_id = v_receive_msg.vid
    JOIN v_send_msg ON v_send_msg.vid = louvain_edges_mirror.src_id) AS t1
    DISTRIBUTED BY (vid);

    DROP TABLE v_send_msg, v_receive_msg;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.num_edges_v2c(
) RETURNS VOID AS $$
BEGIN
    CREATE TEMP TABLE message_with_num_edges_tmp AS
    SELECT t1.vid, t1.cid, SUM(t1.weight) AS num_edges FROM
    (SELECT message_tmp.vid, message_tmp.cid, weight FROM
    message_tmp JOIN louvain_edges ON message_tmp.vid = louvain_edges.src_id
    JOIN vertex_state ON louvain_edges.dst_id = vertex_state.vid AND message_tmp.cid = vertex_state.cid
    UNION
    SELECT message_tmp.vid, message_tmp.cid, weight FROM
    message_tmp JOIN louvain_edges_mirror ON message_tmp.vid = louvain_edges_mirror.dst_id
    JOIN vertex_state ON louvain_edges_mirror.src_id = vertex_state.vid AND message_tmp.cid = vertex_state.cid) AS t1
    GROUP BY t1.vid, t1.cid
    DISTRIBUTED BY (vid);

    DROP TABLE message_tmp;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.select_opt_community(
    IN tot_weight REAL
) RETURNS VOID AS $$
BEGIN
    CREATE TEMP TABLE delta_q_tmp AS
    SELECT vid, 
    message_with_num_edges_tmp.cid,
    -- delta Q 
    (message_with_num_edges_tmp.num_edges-tot_degree/tot_weight*degree)/2/tot_weight AS q, 
    -- num_edges to community, used to update num_edges within community
    message_with_num_edges_tmp.num_edges, 
    -- degree of vertex, used to update tot_degree of community
    degree FROM
    message_with_num_edges_tmp JOIN vertex_state USING (vid)
    JOIN community_info ON message_with_num_edges_tmp.cid = community_info.cid
    DISTRIBUTED BY (vid);

    DROP TABLE message_with_num_edges_tmp;

    -- select optimal community based on delta Q
    CREATE TEMP TABLE opt_community_tmp AS
    SELECT DISTINCT ON(t1.vid) t1.vid, cid, num_edges, degree FROM
    (SELECT vid, MAX(q) AS max_q FROM delta_q_tmp GROUP BY vid HAVING MAX(q) > 0) AS t1
    JOIN delta_q_tmp ON t1.vid = delta_q_tmp.vid AND ABS(max_q-q)<1E-6
    DISTRIBUTED BY (vid);

    DROP TABLE delta_q_tmp;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.update_community_info(
) RETURNS VOID AS $$
BEGIN
    ALTER TABLE community_info RENAME TO old_community_info;

    -- delete community(vertex) that has been merged
    CREATE TEMP TABLE community_info AS
    SELECT old_community_info.* FROM 
    old_community_info LEFT JOIN opt_community_tmp 
    ON old_community_info.cid = opt_community_tmp.vid 
    WHERE opt_community_tmp.vid IS NULL
    DISTRIBUTED BY (cid);

    -- update num_edges & tot_degree of community
    UPDATE community_info SET num_edges=community_info.num_edges+t3.num_edges,
    tot_degree=community_info.tot_degree+t3.degree FROM
    (SELECT t1.cid, t1.num_edges+COALESCE(t2.num_edges, 0) AS num_edges, t1.degree FROM
        (SELECT cid, SUM(num_edges) AS num_edges, SUM(degree) AS degree
        FROM opt_community_tmp GROUP BY cid) AS t1
    LEFT JOIN 
        -- vertices assigned to same community will additionally increase number of edges within community
        (SELECT opt_community_tmp.cid, SUM(weight) AS num_edges
        FROM opt_community_tmp JOIN louvain_edges ON opt_community_tmp.vid = louvain_edges.src_id
        JOIN opt_community_tmp AS t1 ON louvain_edges.dst_id = t1.vid AND opt_community_tmp.cid = t1.cid
        GROUP BY opt_community_tmp.cid) AS t2
    USING (cid)) AS t3 WHERE community_info.cid = t3.cid;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.update_vertex_state(
) RETURNS VOID AS $$
BEGIN
    -- update community of vertex
    UPDATE vertex_state SET cid = opt_community_tmp.cid 
    FROM opt_community_tmp WHERE vertex_state.vid = opt_community_tmp.vid;

    -- update state of vertex whose community has updated
    UPDATE vertex_state SET community_updated = 1 
    WHERE cid IN (SELECT DISTINCT cid FROM opt_community_tmp);

    -- TODO performance compare with create new table
    -- DONE no significant difference
    UPDATE vertex_state SET rand_num = public.gen_rand_num(rand_num::BIGINT);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.louvain_stage1(
    IN max_iter_stage1 INTEGER,
    IN max_iter_no_change_stage1 INTEGER,
    IN tot_weight REAL
) RETURNS VOID AS $$
DECLARE
    current_iter_stage1 INTEGER := 1;
    iters_no_change INTEGER := 0;
    num_vertices_changed INTEGER;
    current_modularity REAL;
    new_modularity REAL;
BEGIN
    SELECT public.modularity(tot_weight) INTO current_modularity;
    WHILE current_iter_stage1 <= max_iter_stage1 LOOP
        -- send message
        PERFORM public.send_msg(current_iter_stage1);
        -- attach num_edges to message
        PERFORM public.num_edges_v2c();
        -- select community
        PERFORM public.select_opt_community(tot_weight);
        -- check vertex need to update
        SELECT COUNT(1) INTO num_vertices_changed FROM opt_community_tmp;
        IF num_vertices_changed < 1 THEN
            iters_no_change := iters_no_change + 1;
            -- no vertex update in last max_iter_no_change_stage1 iterations
            IF iters_no_change >= max_iter_no_change_stage1 THEN
                RAISE INFO 'no vertex change community in the last % iterations, break inner loop', max_iter_no_change_stage1;
                DROP TABLE opt_community_tmp;
                EXIT;
            END IF;
        ELSE
            iters_no_change := 0;
        END IF;
        RAISE INFO '% vertices will be assigned to new community', num_vertices_changed;
        -- update community info
        PERFORM public.update_community_info();
        -- check modularity change
        SELECT public.modularity(tot_weight) INTO new_modularity;
        RAISE INFO 'current inner iteration %, modularity %', current_iter_stage1, new_modularity;
        IF new_modularity < current_modularity THEN
            RAISE INFO 'modularity descend, break inner loop';
            -- rollback community info
            DROP TABLE community_info;
            ALTER TABLE old_community_info RENAME TO community_info;
            DROP TABLE opt_community_tmp;
            EXIT;
        ELSE
            current_modularity := new_modularity;
            DROP TABLE old_community_info;
        END IF;
        -- update vertex state
        PERFORM public.update_vertex_state();
        DROP TABLE opt_community_tmp;
        current_iter_stage1 := current_iter_stage1 + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.update_edges(
) RETURNS VOID AS $$
BEGIN
    CREATE TEMP TABLE new_louvain_edges AS
    SELECT vertex_state.cid AS src_id, t1.cid AS dst_id, SUM(weight) AS weight FROM
    vertex_state JOIN louvain_edges ON vertex_state.vid = louvain_edges.src_id
    JOIN vertex_state AS t1 ON t1.vid = louvain_edges.dst_id
    GROUP BY vertex_state.cid, t1.cid
    DISTRIBUTED BY (src_id);

    DROP TABLE louvain_edges, louvain_edges_mirror, vertex_state;

    ALTER TABLE new_louvain_edges RENAME TO louvain_edges;

    PERFORM public.convert2canonical('louvain_edges');

    CREATE TEMP TABLE louvain_edges_mirror AS
    SELECT * FROM louvain_edges DISTRIBUTED BY (dst_id);

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.louvain_stage2(
) RETURNS VOID AS $$
BEGIN
    PERFORM public.update_edges();
    CREATE TEMP TABLE vertex_state AS
    SELECT cid AS vid, tot_degree AS degree, cid AS rand_num, cid, 0 AS community_updated
    FROM community_info DISTRIBUTED BY (vid);  
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.init_louvain(
) RETURNS VOID AS $$
BEGIN
    -- init vertex state
    CREATE TEMP TABLE vertex_state AS
    SELECT vid, in_degree+out_degree AS degree, vid AS rand_num, vid AS cid, 0 AS community_updated
    FROM public.vertices DISTRIBUTED BY (vid);

    -- init community info
    CREATE TEMP TABLE community_info AS
    SELECT vid AS cid, 0 AS num_edges, in_degree+out_degree AS tot_degree
    FROM public.vertices DISTRIBUTED BY (cid);

    -- init edges
    CREATE TEMP TABLE louvain_edges AS
    SELECT src_id, dst_id, 1 AS weight
    FROM public.edges DISTRIBUTED BY (src_id);

    PERFORM public.convert2canonical('louvain_edges');

    CREATE TEMP TABLE louvain_edges_mirror AS
    SELECT * FROM louvain_edges DISTRIBUTED BY (dst_id);

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.init_louvain(
    IN weight_column VARCHAR
) RETURNS VOID AS $$
BEGIN
    -- init edges
    EXECUTE
    'CREATE TEMP TABLE louvain_edges AS
    SELECT src_id, dst_id, ' || quote_ident(weight_column) || ' AS weight
    FROM public.edges DISTRIBUTED BY (src_id)';

    PERFORM public.convert2canonical('louvain_edges');

    CREATE TEMP TABLE louvain_edges_mirror AS
    SELECT * FROM louvain_edges DISTRIBUTED BY (dst_id);

    -- init vertex state
    CREATE TEMP TABLE vertex_state AS
    SELECT COALESCE(t1.src_id, t2.dst_id) AS vid, 
    COALESCE(t1.deg, 0) + COALESCE(t2.deg, 0) AS degree,
    COALESCE(t1.src_id, t2.dst_id) AS rand_num, 
    COALESCE(t1.src_id, t2.dst_id) AS cid, 
    0 AS community_updated FROM
    (SELECT src_id, SUM(weight) AS deg FROM louvain_edges GROUP BY src_id) AS t1
    FULL JOIN
    (SELECT dst_id, SUM(weight) AS deg FROM louvain_edges_mirror GROUP BY dst_id) AS t2
    ON t1.src_id = t2.dst_id
    DISTRIBUTED BY (vid);

    -- init community info
    CREATE TEMP TABLE community_info AS
    SELECT vid AS cid, 0 AS num_edges, degree AS tot_degree
    FROM vertex_state DISTRIBUTED BY (cid);

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.louvain(
    IN max_iter INTEGER,
    IN max_iter_stage1 INTEGER,
    IN max_iter_no_change_stage1 INTEGER,
    IN weight_column VARCHAR,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    current_iter INTEGER := 1;
    tot_weight REAL;
    num_communities INTEGER;
BEGIN
    IF weight_column IS NULL THEN
        PERFORM public.init_louvain();
    ELSE
        PERFORM public.init_louvain(weight_column);
    END IF;
    SELECT SUM(weight) INTO tot_weight FROM louvain_edges;

    WHILE current_iter <= max_iter LOOP
        SELECT COUNT(1) INTO num_communities FROM vertex_state;
        RAISE INFO 'current outer iteration %, community count %', current_iter, num_communities;
        PERFORM public.louvain_stage1(max_iter_stage1, max_iter_no_change_stage1, tot_weight);
        IF current_iter = 1 THEN
            CREATE UNLOGGED TABLE vertex_cid AS
            SELECT vid, cid AS attr FROM vertex_state
            DISTRIBUTED BY (attr);
        ELSE
            CREATE UNLOGGED TABLE new_vertex_cid AS
            SELECT vertex_cid.vid, vertex_state.cid AS attr FROM
            vertex_cid JOIN vertex_state ON vertex_cid.attr = vertex_state.vid
            DISTRIBUTED BY (attr);
            DROP TABLE vertex_cid;
            ALTER TABLE new_vertex_cid RENAME TO vertex_cid;
        END IF;

        PERFORM public.louvain_stage2();

        current_iter := current_iter + 1;

    END LOOP;

    DROP TABLE IF EXISTS community_info, louvain_edges, louvain_edges_mirror, vertex_state;

    EXECUTE 'ALTER TABLE vertex_cid RENAME TO ' || result_table_name;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.init_louvain(
    IN edge_type_column VARCHAR,
    IN edge_type_value INTEGER
) RETURNS VOID AS $$
BEGIN
    -- init edges
    EXECUTE
    'CREATE TEMP TABLE louvain_edges AS
    SELECT src_id, dst_id, 1 AS weight
    FROM public.edges WHERE edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
    ' DISTRIBUTED BY (src_id)';

    PERFORM public.convert2canonical('louvain_edges');

    CREATE TEMP TABLE louvain_edges_mirror AS
    SELECT * FROM louvain_edges DISTRIBUTED BY (dst_id);

    -- init vertex state
    CREATE TEMP TABLE vertex_state AS
    SELECT COALESCE(t1.src_id, t2.dst_id) AS vid, 
    COALESCE(t1.deg, 0) + COALESCE(t2.deg, 0) AS degree,
    COALESCE(t1.src_id, t2.dst_id) AS rand_num, 
    COALESCE(t1.src_id, t2.dst_id) AS cid, 
    0 AS community_updated FROM
    (SELECT src_id, SUM(weight) AS deg FROM louvain_edges GROUP BY src_id) AS t1
    FULL JOIN
    (SELECT dst_id, SUM(weight) AS deg FROM louvain_edges_mirror GROUP BY dst_id) AS t2
    ON t1.src_id = t2.dst_id
    DISTRIBUTED BY (vid);

    -- init community info
    CREATE TEMP TABLE community_info AS
    SELECT vid AS cid, 0 AS num_edges, degree AS tot_degree
    FROM vertex_state DISTRIBUTED BY (cid);


END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.init_louvain(
    IN weight_column VARCHAR,
    IN edge_type_column VARCHAR,
    IN edge_type_value INTEGER
) RETURNS VOID AS $$
BEGIN
    -- init edges
    EXECUTE
    'CREATE TEMP TABLE louvain_edges AS
    SELECT src_id, dst_id, ' || quote_ident(weight_column) || ' AS weight
    FROM public.edges WHERE edges.' || quote_ident(edge_type_column) || ' = ' || edge_type_value ||
    ' DISTRIBUTED BY (src_id)';

    PERFORM public.convert2canonical('louvain_edges');

    CREATE TEMP TABLE louvain_edges_mirror AS
    SELECT * FROM louvain_edges DISTRIBUTED BY (dst_id);

    -- init vertex state
    CREATE TEMP TABLE vertex_state AS
    SELECT COALESCE(t1.src_id, t2.dst_id) AS vid, 
    COALESCE(t1.deg, 0) + COALESCE(t2.deg, 0) AS degree,
    COALESCE(t1.src_id, t2.dst_id) AS rand_num, 
    COALESCE(t1.src_id, t2.dst_id) AS cid, 
    0 AS community_updated FROM
    (SELECT src_id, SUM(weight) AS deg FROM louvain_edges GROUP BY src_id) AS t1
    FULL JOIN
    (SELECT dst_id, SUM(weight) AS deg FROM louvain_edges_mirror GROUP BY dst_id) AS t2
    ON t1.src_id = t2.dst_id
    DISTRIBUTED BY (vid);

    -- init community info
    CREATE TEMP TABLE community_info AS
    SELECT vid AS cid, 0 AS num_edges, degree AS tot_degree
    FROM vertex_state DISTRIBUTED BY (cid);

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.louvain(
    IN max_iter INTEGER,
    IN max_iter_stage1 INTEGER,
    IN max_iter_no_change_stage1 INTEGER,
    IN weight_column VARCHAR,
    IN edge_type_column VARCHAR,
    IN edge_type_value INTEGER,
    IN result_table_name VARCHAR
) RETURNS VOID AS $$
DECLARE
    current_iter INTEGER := 1;
    tot_weight REAL;
    num_communities INTEGER;
BEGIN
    IF weight_column IS NULL THEN
        PERFORM public.init_louvain(edge_type_column, edge_type_value);
    ELSE
        PERFORM public.init_louvain(weight_column, edge_type_column, edge_type_value);
    END IF;
    SELECT SUM(weight) INTO tot_weight FROM louvain_edges;

    WHILE current_iter <= max_iter LOOP
        SELECT COUNT(1) INTO num_communities FROM vertex_state;
        RAISE INFO 'current outer iteration %, community count %', current_iter, num_communities;
        PERFORM public.louvain_stage1(max_iter_stage1, max_iter_no_change_stage1, tot_weight);
        IF current_iter = 1 THEN
            CREATE UNLOGGED TABLE vertex_cid AS
            SELECT vid, cid AS attr FROM vertex_state
            DISTRIBUTED BY (attr);
        ELSE
            CREATE UNLOGGED TABLE new_vertex_cid AS
            SELECT vertex_cid.vid, vertex_state.cid AS attr FROM
            vertex_cid JOIN vertex_state ON vertex_cid.attr = vertex_state.vid
            DISTRIBUTED BY (attr);
            DROP TABLE vertex_cid;
            ALTER TABLE new_vertex_cid RENAME TO vertex_cid;
        END IF;

        PERFORM public.louvain_stage2();

        current_iter := current_iter + 1;

    END LOOP;

    DROP TABLE IF EXISTS community_info, louvain_edges, louvain_edges_mirror, vertex_state;

    EXECUTE 'ALTER TABLE vertex_cid RENAME TO ' || result_table_name;

END;
$$ LANGUAGE plpgsql;
