from tiozin import Job


class JobStub(Job):
    def setup(self) -> None:
        pass

    def submit(self) -> None:
        pass

    def teardown(self) -> None:
        pass
