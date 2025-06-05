from datetime import datetime
from decimal import Decimal

import pytest

from utils.default_serialiser import default_serialiser


@pytest.mark.it("serialise datetime object into a ISO 8601 string")
def test_serialises_correctly():
    datetime_input = datetime(2025, 6, 3, 13, 59, 59)
    serialised_output = "2025-06-03T13:59:59"
    assert default_serialiser(datetime_input) == serialised_output


@pytest.mark.it("raise TypeError for unsupported data types")
def test_raises_type_error_on_unsupported_type():
    with pytest.raises(TypeError) as error:
        default_serialiser(456)
    assert "not serializable" in str(error.value)


@pytest.mark.it("serialises decimal object to float")
def test_serialises_decimal_object():
    decimal_input = Decimal("150.09")
    serialised__output = 150.09
    assert default_serialiser(decimal_input) == serialised__output
