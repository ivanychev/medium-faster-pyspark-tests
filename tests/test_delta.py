import importlib_metadata

from tests.conftest import DELTA_JAR


def test_delta_version_correct():
    assert str(DELTA_JAR)[:-len(".jar")].endswith(
        importlib_metadata.version('delta_spark')
    )