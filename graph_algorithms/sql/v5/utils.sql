CREATE OR REPLACE FUNCTION {graph_name}.indexof(
    IN arr anyarray, 
    IN ele anyelement
) RETURNS INTEGER AS $$
BEGIN
    FOR i IN 1..array_length(arr, 1) LOOP
        IF arr[i] = ele THEN
            RETURN i;
        END IF;
    END LOOP;
    RETURN 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
