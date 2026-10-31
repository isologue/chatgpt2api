"""本地虚拟时钟对照真实轮询实现；不访问上游或写入配置。"""
import argparse
import ast
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services import image_diagnostics as diagnostics
from services import openai_backend_api as module
from services.config import config


def baseline_method(ref):
    source = subprocess.check_output(['git', 'show', ref + ':services/openai_backend_api.py']).decode('utf8')
    tree = ast.parse(source)
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'OpenAIBackendAPI')
    method = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == '_poll_image_results_impl')
    scope = vars(module).copy()
    exec(compile(ast.Module(body=[method], type_ignores=[]), '<git-poll-baseline>', 'exec'), scope)
    return scope['_poll_image_results_impl']


def trial(method, ready_at, interval):
    clock = SimpleNamespace(now=1000.)
    queries, waits, tasks = [], [], []
    api = module.OpenAIBackendAPI.__new__(module.OpenAIBackendAPI)
    api._poll_image_results_impl = method.__get__(api)
    def query(cid):
        queries.append(round(clock.now - 1000, 6))
        clock.now += .1
        return {'ready': ready_at is not None and clock.now - 1000 >= ready_at}
    def task_query(**kwargs):
        tasks.append(1)
        clock.now += .1
        return []
    def sleep(seconds):
        waits.append(round(seconds, 6))
        clock.now += seconds
    api._get_conversation = query
    api._query_backend_tasks = task_query
    api._extract_image_tool_records = lambda value: ([{'file_ids': ['fixture'], 'sediment_ids': []}] if value['ready'] else [])
    api._find_content_policy_error_in_conversation = lambda value: ''
    settings = {**config.data, 'image_poll_initial_wait_secs': 10, 'image_poll_interval_secs': interval,
        'image_check_before_hit_enabled': False, 'image_settle_enabled': False}
    with patch.object(config, 'data', settings), patch.object(diagnostics.time, 'monotonic', lambda: clock.now), patch.object(diagnostics.time, 'sleep', sleep), patch.object(module.random, 'uniform', return_value=0):
        try:
            api._poll_image_results('fixture', 10)
            outcome = 'success'
        except diagnostics.ImagePollTimeoutError:
            outcome = 'timeout'
    return {'ready_at_s': ready_at, 'interval_s': interval, 'outcome': outcome,
        'elapsed_s': round(clock.now - 1000, 6), 'query_at_s': queries,
        'task_queries': len(tasks), 'waits_s': waits}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-ref', default='main')
    parser.add_argument('--output')
    options = parser.parse_args()
    baseline = baseline_method(options.baseline_ref)
    result = {'scope': 'Virtual clock: 100ms conversation query and 100ms first tasks query; no upstream or persistence. Not evidence of production speedup.',
        'baseline_ref': options.baseline_ref,
        'baseline': [trial(baseline, value, 10) for value in (0, 4, 7, 9.3, None)],
        'current': [trial(module.OpenAIBackendAPI._poll_image_results_impl, value, 10) for value in (0, 4, 7, 9.3, None)]}
    if options.output:
        Path(options.output).write_text(json.dumps(result, indent=2), encoding='utf8')
    print(json.dumps(result, indent=2))
