#!/usr/bin/env python3
"""AI-function regressions against a running, freshly built seekdb.

Run with python3 -m unittest discover -s unittest/sql/engine/expr -p 'test_ai_function_integration.py'.
Requires PyMySQL; SEEKDB_HOST/SEEKDB_PORT/SEEKDB_USER/SEEKDB_PASSWORD select
the test instance. The suite owns only its randomly named database/models.
"""

import hashlib
import json
import os
import threading
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pymysql


def embedding(text, dimension):
    digest = hashlib.sha256(text.encode()).digest()
    return [digest[i % len(digest)] / 256.0 for i in range(dimension)]


class MockHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        with self.server.lock:
            self.server.events.append((self.path, body))
        if self.path.endswith('/embeddings') or self.path.endswith('/embed'):
            dimension = body.get('dimensions', 4)
            inputs = body['input']
            if not isinstance(inputs, list):
                inputs = [inputs]
            vectors = [embedding(text, dimension) for text in inputs]
            if '/bad-vector/' in self.path:
                vectors[0] = ['invalid'] * dimension
            if self.path.endswith('/embed'):
                result = {'embeddings': vectors}
            else:
                data = [{'index': i, 'embedding': vector} for i, vector in enumerate(vectors)]
                if '/bad-index/' in self.path:
                    data[0]['index'] = len(data)
                result = {'data': list(reversed(data))}
        elif self.path.endswith('/rerank'):
            result = {'results': [
                {'index': i, 'relevance_score': 1}
                for i in reversed(range(len(body['documents'])))]}
            if '/duplicate/' in self.path and len(result['results']) > 1:
                result['results'][0]['index'] = result['results'][1]['index']
        elif self.path.endswith('/generate'):
            if body.get('stream') is not False:
                self.send_error(400, 'non-streaming request required')
                return
            result = {'response': body['prompt']}
        else:
            text = body['messages'][-1]['content']
            result = {'choices': [{'message': {'content': '' if '/empty/' in self.path else text}}]}
        payload = json.dumps(result, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class AIFunctionIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.connection = pymysql.connect(
            host=os.getenv('SEEKDB_HOST', '127.0.0.1'),
            port=int(os.getenv('SEEKDB_PORT', '2881')),
            user=os.getenv('SEEKDB_USER', 'root'),
            password=os.getenv('SEEKDB_PASSWORD', ''),
            charset='utf8mb4', autocommit=True,
        )
        cls.prefix = 'pipe_ai_' + uuid.uuid4().hex[:12]
        cls.models = []
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), MockHandler)
        cls.server.events = []
        cls.server.lock = threading.Lock()
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.run_sql('CREATE DATABASE ' + cls.prefix)
        cls.run_sql('USE ' + cls.prefix)

    @classmethod
    def tearDownClass(cls):
        try:
            for name in cls.models:
                cls.run_sql('CALL DBMS_AI_SERVICE.DROP_AI_MODEL(%s)', (name,))
                cls.run_sql('CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT(%s)', (name + '_ep',))
            cls.run_sql('DROP DATABASE ' + cls.prefix)
        finally:
            cls.connection.close()
            cls.server.shutdown()
            cls.server.server_close()
            cls.thread.join()

    @classmethod
    def run_sql(cls, sql, args=None):
        with cls.connection.cursor() as cursor:
            cursor.execute(sql, args)
            return cursor.fetchall()

    def model(self, kind, path, provider='openai'):
        name = self.prefix + '_' + str(len(self.models))
        self.run_sql('CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)',
                     (name, json.dumps({'type': kind, 'model_name': 'regression-model'})))
        endpoint = {'ai_model_name': name, 'scope': 'all', 'provider': provider,
                    'url': 'http://127.0.0.1:%d%s' % (self.server.server_port, path),
                    'access_key': '' if provider.lower() == 'ollama' else 'test-key'}
        self.run_sql('CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)',
                     (name + '_ep', json.dumps(endpoint)))
        self.models.append(name)
        return name

    def test_prompt_json_longtext_nested_and_repeated(self):
        model = self.model('completion', '/v1/chat/completions')
        argument = '你好\\"' * 1000
        actual = self.run_sql("SELECT AI_COMPLETE(%s,AI_PROMPT('{0}|{0}|{0}',%s),NULL)",
                              (model, argument))[0][0]
        self.assertEqual('|'.join([argument] * 3), actual)
        actual = self.run_sql("SELECT AI_COMPLETE(%s,AI_PROMPT('outer {0}',AI_PROMPT('inner {0}','你好')))",
                              (model,))[0][0]
        self.assertEqual('outer inner 你好', actual)
        actual = self.run_sql("SELECT AI_COMPLETE(%s,AI_PROMPT('{0}',JSON_OBJECT('key',JSON_ARRAY(1,TRUE))))",
                              (model,))[0][0]
        self.assertEqual({'key': [1, True]}, json.loads(actual))
        self.run_sql('CREATE TABLE prompt_lob(t LONGTEXT,a TEXT)')
        self.run_sql('INSERT INTO prompt_lob VALUES(%s,%s)', ('{0}:{0}', argument))
        self.assertEqual(argument + ':' + argument,
                         self.run_sql('SELECT AI_COMPLETE(%s,AI_PROMPT(t,a)) FROM prompt_lob', (model,))[0][0])
        for invalid in ('{1}', '{9999999999999999999999999999999999999}'):
            with self.assertRaises(pymysql.MySQLError):
                self.run_sql('SELECT AI_COMPLETE(%s,AI_PROMPT(%s,%s))', (model, invalid, argument))

    def test_complete_config_and_invalid_response(self):
        model = self.model('completion', '/v1/chat/completions')
        self.assertEqual('hello', self.run_sql(
            "SELECT AI_COMPLETE(%s,'hello',JSON_OBJECT('temperature',0.2,'max_tokens',16))", (model,))[0][0])
        with self.server.lock:
            body = self.server.events[-1][1]
        self.assertEqual(0.2, body['temperature'])
        self.assertEqual(16, body['max_tokens'])
        empty_model = self.model('completion', '/empty/v1/chat/completions')
        with self.assertRaises(pymysql.MySQLError):
            self.run_sql("SELECT AI_COMPLETE(%s,'hello')", (empty_model,))

    def test_embedding_batch_order_null_filter_and_multiple_requests(self):
        model = self.model('dense_embedding', '/v1/embeddings')
        self.run_sql('CREATE TABLE embed_rows(id INT PRIMARY KEY,content LONGTEXT)')
        rows = [(i, None if i % 7 == 0 else '内容-%d' % i) for i in range(180)]
        with self.connection.cursor() as cursor:
            cursor.executemany('INSERT INTO embed_rows VALUES(%s,%s)', rows)
        with self.server.lock:
            before = len(self.server.events)
        actual = self.run_sql('SELECT id,AI_EMBED(%s,content,4) FROM embed_rows WHERE id%%3<>1 ORDER BY id LIMIT 100', (model,))
        self.assertEqual(100, len(actual))
        for row_id, vector in actual:
            expected = rows[row_id][1]
            self.assertEqual(None if expected is None else embedding(expected, 4),
                             None if vector is None else json.loads(vector))
        with self.server.lock:
            requests = [body for path, body in self.server.events[before:] if path.endswith('/embeddings')]
        self.assertTrue(any(len(body['input']) > 1 for body in requests), requests)
        self.assertTrue(all(None not in body['input'] for body in requests))

    def test_embedding_dimension_and_response_validation(self):
        model = self.model('dense_embedding', '/v1/embeddings')
        self.assertEqual(embedding('hello', 7), json.loads(self.run_sql("SELECT AI_EMBED(%s,'hello',7)", (model,))[0][0]))
        for dimension in (0, -1, 1.5):
            with self.assertRaises(pymysql.MySQLError):
                self.run_sql("SELECT AI_EMBED(%s,'hello',%s)", (model, dimension))
        for path in ('/bad-vector/v1/embeddings', '/bad-index/v1/embeddings'):
            bad_model = self.model('dense_embedding', path)
            with self.assertRaises(pymysql.MySQLError):
                self.run_sql("SELECT AI_EMBED(%s,'hello',4)", (bad_model,))

    def test_ollama_native_protocol_and_case_insensitive_provider(self):
        model = self.model('completion', '/api/generate', 'oLlAmA')
        self.assertEqual('原生', self.run_sql("SELECT AI_COMPLETE(%s,'原生')", (model,))[0][0])
        self.run_sql('CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s,%s)',
                     (model + '_ep', json.dumps({'provider': 'OLLAMA'})))
        model = self.model('dense_embedding', '/api/embed', 'ollama')
        self.assertEqual(embedding('hello', 4), json.loads(self.run_sql("SELECT AI_EMBED(%s,'hello')", (model,))[0][0]))

    def test_rerank_multibatch_ties_and_original_ids(self):
        model = self.model('rerank', '/v1/rerank', 'siliconflow')
        docs = [{'id': i, 'text': '同分文档'} for i in range(47)]
        result = json.loads(self.run_sql("SELECT AI_RERANK(%s,'query',%s,'text')", (model, json.dumps(docs)))[0][0])
        self.assertEqual(list(range(47)), [doc['id'] for doc in result])
        strings = ['document-%d' % i for i in range(47)]
        result = json.loads(self.run_sql("SELECT AI_RERANK(%s,'query',%s,NULL)", (model, json.dumps(strings)))[0][0])
        self.assertEqual(list(range(47)), [doc['index'] for doc in result])
        bad_model = self.model('rerank', '/duplicate/v1/rerank', 'siliconflow')
        with self.assertRaises(pymysql.MySQLError):
            self.run_sql("SELECT AI_RERANK(%s,'query',%s,NULL)", (bad_model, json.dumps(strings)))


if __name__ == '__main__':
    unittest.main(verbosity=2)
