import pytest 
from utils.default_serialiser import default_serialiser
from datetime import datetime

@pytest.mark.it('serialise datetime object into a ISO 8601 string')
def test_serialises_correctly():
    datetime_input = datetime(2025, 6, 3, 13, 59, 59)
    serialised_output = "2025-06-03T13:59:59"
    assert default_serialiser(datetime_input) == serialised_output