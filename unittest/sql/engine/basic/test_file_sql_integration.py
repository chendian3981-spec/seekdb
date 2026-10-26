#!/usr/bin/env python3
"""Run local file SQL regressions against a seekdb test instance (PyMySQL)."""
import json
import base64
import os
from pathlib import Path
import tempfile
import unittest

import pymysql


class FileSqlIntegration(unittest.TestCase):
    def setUp(self):
        self.connection = pymysql.connect(host=os.getenv('SEEKDB_HOST', '127.0.0.1'),
            port=int(os.getenv('SEEKDB_PORT', '2881')), user=os.getenv('SEEKDB_USER', 'root'),
            password=os.getenv('SEEKDB_PASSWORD', ''), charset='utf8mb4', autocommit=True)
        self.directory = tempfile.TemporaryDirectory(prefix='seekdb_file_sql_')
        self.csv = Path(self.directory.name) / 'records.csv'
        self.csv.write_text('id,name,score,active,day,created,note,id\n'
            '1,Alice,98.5,true,2026-08-01,2026-08-01 10:11:12,"hello,\nworld",101\n'
            '2,Bob,87,false,,,plain,102\n'
            '3,Chen,90.25,true,2026-08-03,2026-08-03T12:13:14,"say ""hi""",103\n')
        self.json = Path(self.directory.name) / 'records.jsonl'
        self.json.write_text('\n'.join(json.dumps(row) for row in [
            {'id': 1, 'category': 'one'}, {'id': 2, 'category': 'two'}, {'id': 4, 'category': 'four'}]) + '\n')
        self.sql('SET SESSION enable_file_sql=ON')

    def tearDown(self):
        self.connection.close()
        self.directory.cleanup()

    def sql(self, query, args=None):
        with self.connection.cursor() as cursor:
            cursor.execute(query, args)
            return cursor.fetchall()

    def test_csv_projection_filter_aggregate_and_metadata(self):
        rows = self.sql('SELECT id,name,id__2,note FROM read_csv(%s) f WHERE id>1 ORDER BY id DESC LIMIT 1', (str(self.csv),))
        self.assertEqual(((3, 'Chen', 103, 'say "hi"'),), rows)
        rows = self.sql('SELECT count(*),sum(id),round(avg(score),2),sum(active) FROM read_csv(%s,header=true,sample_size=-1)', (str(self.csv),))
        self.assertEqual((3, 6, 91.92, 2), tuple(float(value) for value in rows[0]))
        rows = self.sql('SELECT active,count(*) FROM read_csv(%s) GROUP BY active HAVING count(*)>1', (str(self.csv),))
        self.assertEqual(((1, 2),), rows)
        rows = self.sql('SELECT day,created FROM read_csv(%s) WHERE id=2', (str(self.csv),))
        self.assertEqual(((None, None),), rows)
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT * FROM read_csv(%s) LIMIT 0', (str(self.csv),))
            self.assertEqual(['id','name','score','active','day','created','note','id__2'], [column[0] for column in cursor.description])

    def test_json_and_cross_format_joins(self):
        rows = self.sql('SELECT c.id,c.name,j.category FROM read_csv(%s) c LEFT JOIN read_ndjson(%s) j ON c.id=j.id ORDER BY c.id',
                        (str(self.csv), str(self.json)))
        self.assertEqual(((1, 'Alice', 'one'), (2, 'Bob', 'two'), (3, 'Chen', None)), rows)
        rows = self.sql("SELECT c.id FROM read_csv(%s) c INNER JOIN read_json(%s,format='newline_delimited') j ON c.id=j.id ORDER BY c.id",
                        (str(self.csv), str(self.json)))
        self.assertEqual(((1,), (2,)), rows)

    def test_feature_switch_glob_and_error_recovery(self):
        self.sql('SET SESSION enable_file_sql=OFF')
        with self.assertRaises(pymysql.MySQLError):
            self.sql('SELECT count(*) FROM read_csv(%s)', (str(self.csv),))
        self.sql('SET SESSION enable_file_sql=ON')
        rows = self.sql('SELECT file FROM glob(%s) ORDER BY file', (self.directory.name + '/*',))
        self.assertEqual(sorted([str(self.csv), str(self.json)]), [row[0] for row in rows])
        with self.assertRaises(pymysql.MySQLError):
            self.sql('SELECT * FROM read_csv(%s,sample_size=10)', (str(self.csv),))
        malformed = Path(self.directory.name) / 'bad.csv'
        malformed.write_text('a,b\n1\n')
        with self.assertRaises(pymysql.MySQLError):
            self.sql('SELECT * FROM read_csv(%s)', (str(malformed),))
        self.assertEqual(((4,),), self.sql('SELECT 2+2'))

    def test_parquet_projection_and_footer_count(self):
        fixture = Path(__file__).resolve().parents[4] / 'tools/deploy/mysql_test/test_suite/file_sql/data/simple.parquet.b64'
        parquet = Path(self.directory.name) / 'simple.parquet'
        parquet.write_bytes(base64.b64decode(fixture.read_bytes()))
        rows = self.sql('SELECT count(*),min(c1),max(c1),sum(c1) FROM read_parquet(%s)', (str(parquet),))
        self.assertEqual((7, 0, 6, 21), tuple(int(value) for value in rows[0]))
        self.assertEqual(((7,),), self.sql('SELECT count(*) FROM read_parquet(%s)', (str(parquet),)))
        self.assertEqual(((6,), (5,)), self.sql('SELECT c1 FROM read_parquet(%s) WHERE c1>4 ORDER BY c1 DESC', (str(parquet),)))

    def test_describe_and_copy_csv_round_trip(self):
        rows = self.sql('DESCRIBE SELECT * FROM read_csv(%s)', (str(self.csv),))
        self.assertEqual(['id','name','score','active','day','created','note','id__2'], [row[0] for row in rows])
        self.assertEqual(['BIGINT','VARCHAR','DOUBLE','BOOLEAN','DATE','DATETIME','VARCHAR','BIGINT'], [row[1] for row in rows])
        self.assertEqual(['NO','NO','NO','NO','YES','YES','NO','NO'], [row[2] for row in rows])
        output = Path(self.directory.name) / 'export.csv'
        self.sql("COPY (SELECT id,name,score,note,day FROM read_csv(%s) ORDER BY id) TO %s (FORMAT CSV, HEADER, NULLSTR 'NULL')", (str(self.csv), str(output)))
        expected = self.sql('SELECT id,name,score,note,day FROM read_csv(%s) ORDER BY id', (str(self.csv),))
        self.assertEqual(expected, self.sql("SELECT * FROM read_csv(%s,nullstr='NULL') ORDER BY id", (str(output),)))
        previous = output.read_bytes()
        with self.assertRaises(pymysql.MySQLError):
            self.sql('COPY (SELECT 1) TO %s', (str(output),))
        self.assertEqual(previous, output.read_bytes())
        self.assertFalse(list(Path(self.directory.name).glob('.seekdb-copy-*')))


if __name__ == '__main__':
    unittest.main(verbosity=2)
