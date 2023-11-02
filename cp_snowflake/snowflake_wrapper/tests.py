from django.test import TestCase
from unittest.mock import MagicMock, patch

from snowflake_wrapper.base import SnowflakeWrapper


class SnowflakeWrapperTests(TestCase):

    # stub test
    @patch('snowflake_wrapper.base.SnowflakeWrapper')
    def test_connection(self, snowflake_wrapper_mock):
        conn = snowflake_wrapper_mock.get_new_connection({})
        self.assertIsNotNone(conn)

