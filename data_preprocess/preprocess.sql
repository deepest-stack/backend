CREATE OR REPLACE FUNCTION public.most_common(
    IN table_name VARCHAR, 
    IN column_name VARCHAR,
    IN dummy anyelement
) RETURNS anyelement AS $$
DECLARE
    result ALIAS FOR $0;
BEGIN
    EXECUTE 
    'CREATE TEMP TABLE t1 AS
    SELECT ' || quote_ident(column_name) || ' AS target_column, COUNT(1) AS freq FROM ' 
    || table_name || ' GROUP BY ' || quote_ident(column_name) || ' ORDER BY freq DESC LIMIT 1
    DISTRIBUTED BY (target_column)';
    SELECT target_column INTO result FROM t1 LIMIT 1;
    DROP TABLE t1;
    RETURN result;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.top_n(
    IN table_name VARCHAR, 
    IN column_name VARCHAR,
    IN dummy anyelement,
    IN n INTEGER
) RETURNS anyarray AS $$
DECLARE
    result ALIAS FOR $0;
    cur refcursor;
BEGIN
    EXECUTE 
    'CREATE TEMP TABLE t1 AS
    SELECT ' || quote_ident(column_name) || ' AS target_column, COUNT(1) AS freq FROM ' 
    || table_name || ' GROUP BY ' || quote_ident(column_name) || ' ORDER BY freq DESC LIMIT ' || n
    || ' DISTRIBUTED BY (target_column)';

    SELECT ARRAY(SELECT target_column FROM t1) INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.top_percent(
    IN table_name VARCHAR, 
    IN column_name VARCHAR,
    IN dummy anyelement,
    IN percent REAL
) RETURNS anyarray AS $$
DECLARE
    result ALIAS FOR $0;
    total_row INTEGER;
    freq INTEGER;
    acc_freq INTEGER := 0;
    top_n INTEGER := 0;
BEGIN
    EXECUTE
    'CREATE TEMP TABLE t1 AS SELECT COUNT(1) AS total_row FROM ' 
    || quote_ident(table_name) || ' DISTRIBUTED RANDOMLY';
    SELECT t1.total_row INTO total_row FROM t1 LIMIT 1;
    DROP TABLE t1;

    EXECUTE 
    'CREATE TEMP TABLE t1 AS
    SELECT ' || quote_ident(column_name) || ' AS target_column, COUNT(1) AS freq FROM ' 
    || table_name || ' GROUP BY ' || quote_ident(column_name)
    || ' DISTRIBUTED BY (target_column)';

    result := ARRAY[dummy];

    FOR freq IN SELECT t1.freq FROM t1 ORDER BY t1.freq DESC LOOP
        top_n := top_n + 1;
        acc_freq := acc_freq + freq;
        IF acc_freq > total_row * percent THEN
            EXIT;
        END IF;
    END LOOP;

    EXECUTE
    'CREATE TEMP TABLE t2 AS
    SELECT target_column FROM t1 ORDER BY freq DESC LIMIT ' 
    || top_n || ' DISTRIBUTED RANDOMLY';

    DROP TABLE t1;

    SELECT ARRAY(SELECT target_column FROM t2) INTO result;
    DROP TABLE t2;

    RETURN result;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.binaryze(
    IN x NUMERIC, 
    IN threshold NUMERIC,
    IN p INTEGER,
    IN n INTEGER
) RETURNS INTEGER AS $$
DECLARE
    result INTEGER;
BEGIN
    IF x >= threshold THEN
        result := p;
    ELSE
        result := n;
    END IF;
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION public.onehot_encode(
    IN x anyelement,
    IN enum anyarray,
    IN with_others INTEGER
) RETURNS INTEGER[] AS $$
DECLARE
    result INTEGER[];
    len INTEGER;
    idx INTEGER;
BEGIN
    len := array_length(enum, 1);
    IF with_others = 1 THEN
        len := len + 1;
    END IF;
    result := ARRAY[0::INTEGER];
    FOR i IN 2..len LOOP
        result := result || 0::INTEGER;
    END LOOP;
    idx := indexof(enum, x);
    IF idx <> 0 THEN
        result[idx] = 1::INTEGER;
    ELSIF with_others = 1 THEN
        result[len] = 1::INTEGER;
    END IF;
    return result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION public.onehot_encode(
    IN x anyelement,
    IN n INTEGER,
    IN with_others INTEGER
) RETURNS INTEGER[] AS $$
DECLARE
    result INTEGER[];
    len INTEGER;
    idx INTEGER;
BEGIN
    len := array_length(enum, 1);
    IF with_others = 1 THEN
        len := len + 1;
    END IF;
    result := ARRAY[0::INTEGER];
    FOR i IN 2..len LOOP
        result := result || 0::INTEGER;
    END LOOP;
    idx := indexof(enum, x);
    IF idx <> 0 THEN
        result[idx] = 1::INTEGER;
    ELSIF with_others = 1 THEN
        result[len] = 1::INTEGER;
    END IF;
    return result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;


