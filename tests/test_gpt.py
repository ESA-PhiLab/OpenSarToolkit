import pytest

from ost.helpers.helpers import run_command
from ost.helpers.settings import GPT_FILE


@pytest.mark.skip(reason="not running in pip build")
def test_gpt():
    assert run_command(GPT_FILE, logfile=None) == 0
