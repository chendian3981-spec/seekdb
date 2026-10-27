#!/usr/bin/env python3
"""AI-column transaction visibility and malformed-result regressions.

Requires PyMySQL and a freshly built server. SEEKDB_HOST/PORT/USER/PASSWORD
select the instance; all database/model names are unique to this run.
"""
import json
import os
import threading
import time
import unittest
import uuid
from http.server import ThreadingHTTPServer

import pymysql
from test_ai_function_integration import MockHandler


class AIColumnIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.options = dict(host=os.getenv('SEEKDB_HOST', '127.0.0.1'),
                           port=int(os.getenv('SEEKDB_PORT', '2881')),
                           user=os.getenv('SEEKDB_USER', 'root'),
                           password=os.getenv('SEEKDB_PASSWORD', ''),
                           charset='utf8mb4', autocommit=True)
        cls.connection = pymysql.connect(**cls.options)
        cls.name = 'pipe_aicol_' + uuid.uuid4().hex[:12]
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), MockHandler)
        cls.server.events, cls.server.lock = [], threading.Lock()
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.sql('CREATE DATABASE ' + cls.name)
        cls.sql('USE ' + cls.name)
        cls.sql('CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)',
                (cls.name, json.dumps({'type': 'completion', 'model_name': 'echo'})))
        cls.sql('CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)',
                (cls.name + '_ep', json.dumps(dict(ai_model_name=cls.name,
                 url=f'http://127.0.0.1:{cls.server.server_port}/v1/chat/completions',
                 access_key='local-test', provider='openai', request_model_name='echo'))))

    @classmethod
    def tearDownClass(cls):
        try:
            cls.sql('SET SESSION ai_column_strict_read=OFF')
            cls.sql('DROP DATABASE ' + cls.name)
            cls.sql('CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT(%s)', (cls.name + '_ep',))
            cls.sql('CALL DBMS_AI_SERVICE.DROP_AI_MODEL(%s)', (cls.name,))
        finally:
            cls.connection.close()
            cls.server.shutdown()
            cls.server.server_close()
            cls.thread.join()

    @classmethod
    def sql(cls, query, args=None):
        with cls.connection.cursor() as cursor:
            cursor.execute(query, args)
            return cursor.fetchall()

    def wait_for(self, query, expected):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.sql(query) == expected:
                return
            time.sleep(.05)
        self.assertEqual(self.sql(query), expected)

    def test_strict_read_observes_own_transaction_and_rollback(self):
        self.sql(f"CREATE TABLE own_tx(id INT PRIMARY KEY, source VARCHAR(100), "
                 f"result VARCHAR(100) AI COLUMN(AI_COMPLETE('{self.name}',source)))")
        self.sql("INSERT INTO own_tx(id,source) VALUES(1,'before')")
        self.wait_for('SELECT result FROM own_tx', (('before',),))
        self.sql('SET SESSION ai_column_strict_read=ON')
        self.sql('START TRANSACTION')
        try:
            self.sql("UPDATE own_tx SET source='after' WHERE id=1")
            # An implicit AI-result maintenance assignment is not a user read.
            self.sql("UPDATE own_tx SET source='again' WHERE id=1")
            with self.assertRaises(pymysql.MySQLError) as failure:
                self.sql('SELECT result FROM own_tx')
            self.assertEqual(failure.exception.args[0], 11120)
        finally:
            self.sql('ROLLBACK')
        self.assertEqual(self.sql('SELECT result FROM own_tx'), (('before',),))
        self.sql('SET SESSION ai_column_strict_read=OFF')

    def test_invalid_typed_result_reaches_terminal_failure(self):
        self.check_invalid_typed_result('JSON', 'invalid_json')

    def test_non_finite_number_reaches_terminal_failure(self):
        self.check_invalid_typed_result('DOUBLE', 'invalid_number', 'NaN')

    def check_invalid_typed_result(self, datatype, table, value='invalid-json'):
        self.sql(f"CREATE TABLE {table}(id INT PRIMARY KEY, source VARCHAR(100), "
                 f"result {datatype} AI COLUMN(AI_COMPLETE('{self.name}',source)) "
                 "WITH(max_retries=0))")
        # The echo model returns invalid data despite the typed system prompt.
        self.sql(f"INSERT INTO {table}(id,source) VALUES(1,%s)", (value,))
        query = ("SELECT COUNT(*) FROM oceanbase.__all_ai_column_task task "
                 "JOIN oceanbase.__all_table t ON t.table_id=task.table_id "
                 "JOIN oceanbase.__all_database d ON d.database_id=t.database_id "
                 f"WHERE d.database_name='{self.name}' AND t.table_name='{table}'")
        self.wait_for(query, ((0,),))
        self.assertEqual(self.sql(f'SELECT result FROM {table}'), ((None,),))
        self.sql('SET SESSION ai_column_strict_read=ON')
        try:
            with self.assertRaises(pymysql.MySQLError) as failure:
                self.sql(f'SELECT result FROM {table}')
            self.assertEqual(failure.exception.args[0], 11120)
        finally:
            self.sql('SET SESSION ai_column_strict_read=OFF')


if __name__ == '__main__':
    unittest.main()
