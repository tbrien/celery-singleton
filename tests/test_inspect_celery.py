import pytest
import sys
from unittest import mock
from celery_singleton.inspect_celery import list_celery_tasks_with_name, are_worker_active, CeleryTask


class TestInspectCelery:
    def test__list_celery_tasks_with_name__should_return_matching_task(self):
        # Given
        active_tasks = {'celery@worker_host': [{'id': '262a1cf9-2c4f-4680-8261-7498fb39756c', 'name': 'simple_task', 'args': [1, 2, 3], 'kwargs': {}, 'type': 'simple_task', 'hostname': 'celery@worker_host', 'time_start': 1588508284.207397, 'acknowledged': True, 'delivery_info': {'exchange': '', 'routing_key': 'celery', 'priority': 0, 'redelivered': None}, 'worker_pid': 45895}]}
        inspect_mock = mock.MagicMock()
        inspect_mock.active.return_value = active_tasks
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        task_list = list_celery_tasks_with_name(mock_app, "simple_task")

        # Then
        assert len(task_list) == 1
        actual = task_list[0]
        assert actual.name == 'simple_task'
        assert actual.args == [1, 2, 3]
        assert actual.kwargs == {}
    
    def test__list_celery_tasks_with_name__should_return_empty_list_when_no_matching_taskname(self):
        # Given
        active_tasks = {'celery@worker_host': [{'id': '262a1cf9-2c4f-4680-8261-7498fb39756c', 'name': 'simple_task', 'args': [1, 2, 3], 'kwargs': {}, 'type': 'simple_task', 'hostname': 'celery@worker_host', 'time_start': 1588508284.207397, 'acknowledged': True, 'delivery_info': {'exchange': '', 'routing_key': 'celery', 'priority': 0, 'redelivered': None}, 'worker_pid': 45895}]}
        inspect_mock = mock.MagicMock()
        inspect_mock.active.return_value = active_tasks
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        task_list = list_celery_tasks_with_name(mock_app, "bad_task_name")

        # Then
        assert len(task_list) == 0
    
    def test__list_celery_tasks_with_name__should_return_empty_list_when_no_active_tasts(self):
        # Given
        inspect_mock = mock.MagicMock()
        inspect_mock.active.return_value = None
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        task_list = list_celery_tasks_with_name(mock_app, "any_name")

        # Then
        assert len(task_list) == 0
    
    def test__list_celery_tasks_with_name__should_return_empty_list_when_worker_answer_cannot_be_parsed(self):
        # Given
        inspect_mock = mock.MagicMock()
        inspect_mock.active.return_value = {'worker': ['bad_task_definition']}
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        task_list = list_celery_tasks_with_name(mock_app, "bad_task_name")

        # Then
        assert len(task_list) == 0
    
    def test__are_worker_active__should_return_true_if_worker_responds_to_ping(self):
        # Given
        active_workers = {u'celery@host': {u'ok': u'pong'}}
        inspect_mock = mock.MagicMock()
        inspect_mock.ping.return_value = active_workers
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        active_workers_found = are_worker_active(mock_app)

        # Then
        assert active_workers_found
    
    def test__are_worker_active__should_return_false_if_worker_does_not_respond_to_ping(self):
        # Given
        active_workers = None
        inspect_mock = mock.MagicMock()
        inspect_mock.ping.return_value = active_workers
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        active_workers_found = are_worker_active(mock_app)

        # Then
        assert not active_workers_found
    
    def test__are_worker_active__should_return_false_if_worker_respond_ko_to_ping(self):
        # Given
        active_workers = {u'celery@host': {u'not_ok': u'pong'}}
        inspect_mock = mock.MagicMock()
        inspect_mock.ping.return_value = active_workers
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        active_workers_found = are_worker_active(mock_app)

        # Then
        assert not active_workers_found

