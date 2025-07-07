from conftest import ROOT
from pystolint.api import check

MODULES = ['pystatsd', 'tests']


def test_codestyle() -> None:
    result = check(MODULES, local_toml_path_provided=f'{ROOT}/pyproject.toml')

    assert len(result.items) == 0, '\n'.join(str(item) for item in result.items)
    assert len(result.errors) == 0, '\n'.join(error for error in result.errors)
