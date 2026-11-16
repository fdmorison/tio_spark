from typing import Any

from tiozin import Runner


class RunnerStub(Runner):
    def __init__(self, **options) -> None:
        super().__init__(**options)
        self._session = None

    @property
    def session(self) -> Any:
        return self._session

    def setup(self) -> None:
        pass

    def run(self, execution_plan: Any, **options) -> None:
        pass

    def teardown(self) -> None:
        pass
