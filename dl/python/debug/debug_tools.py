#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

import psycopg2
import psycopg2.extras
import re


class PlPythonCursor(object):

    def __init__(self, cur):
        self._cur = cur

    def fetch(self, num):
        return self._cur.fetchmany(num)

    def close(self):
        self._cur.close()

    def fetchall(self):
        return self._cur.fetchall()


class PlPython(object):
    def __init__(self):
        self._conn = psycopg2.connect(host="192.168.8.138", port=35432, dbname="dev", user="gpadmin")
        self._conn.autocommit = True

    def cursor(self, sql):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        return PlPythonCursor(cur)

    def execute(self, sql, args=None):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, args)
        try:
            return cur.fetchall()
        except psycopg2.ProgrammingError:
            return None
        finally:
            cur.close()

    def prepare(self, sql, args):
        return re.sub("\$\d+", "%s", sql)


plpy = PlPython()


if __name__ == "__main__":
    pass
