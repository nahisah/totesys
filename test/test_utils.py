import pytest
from utils.default_serialiser import default_serialiser
from utils.normalise_datetime import normalise_datetimes
from utils.db_connection import create_conn
from unittest.mock import patch, Mock
from datetime import datetime
from decimal import Decimal
import os


class TestDefaultSerialiser:

    @pytest.mark.it("serialise datetime object into a ISO 8601 string")
    def test_serialises_correctly(self):
        datetime_input = datetime(2025, 6, 3, 13, 59, 59)
        serialised_output = "2025-06-03T13:59:59"
        assert default_serialiser(datetime_input) == serialised_output

    @pytest.mark.it("raise TypeError for unsupported data types")
    def test_raises_type_error_on_unsupported_type(self):
        with pytest.raises(TypeError) as error:
            default_serialiser(456)
        assert "not serializable" in str(error.value)

    @pytest.mark.it("serialises decimal object to float")
    def test_serialises_decimal_object(self):
        decimal_input = Decimal("150.09")
        serialisedoutput = 150.09
        assert default_serialiser(decimal_input) == serialisedoutput


class TestNormaliseDatetimes:
    @pytest.mark.it("Converts datetime values in list of dicts to formatted strings")
    def test_normalise_datetimes(self):
        input_data = [{"created_at": datetime(2025, 6, 5, 12, 10, 12, 123000)}]
        result = normalise_datetimes(input_data)
        assert result == [{"created_at": "2025-06-05 12:10:12.123"}]

    @pytest.mark.it("Only converts datetime objects")
    def test_non_datetime_object_conversion(self):
        input_data = [
            {"price": 200, "created_at": datetime(2025, 6, 5, 12, 10, 12, 123000)}
        ]
        result = normalise_datetimes(input_data)
        assert result == [{"price": 200, "created_at": "2025-06-05 12:10:12.123"}]


class TestDBConnection:
    @pytest.mark.it("Creates a connection to the database using environment variables")
    @patch.dict(
        os.environ,
        {
            "DBUSER": "test-user",
            "DBNAME": "test_db",
            "DBPASSWORD": "test_pass",
            "HOST": "localhost",
        },
    )
    @patch("utils.db_connection.Connection")
    def test_create_conn(self, mock_connection):
        mock_conn_first = Mock()
        mock_connection.return_value = mock_conn_first
        result = create_conn()

        mock_connection.assert_called_with(
            database="test_db", user="test-user", password="test_pass", host="localhost"
        )
        assert result == mock_conn_first
