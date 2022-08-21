import glob
import os
import os.path
from tempfile import TemporaryDirectory

from analog.atomic_update import atomic_update


DIR = os.path.dirname(__file__)
PREFIX = "test_atomic_update-"


def read_all(path: str) -> str:
    with open(path, mode="r", encoding="utf8") as file:
        return file.read()


def test_atomic_update() -> None:
    with TemporaryDirectory(dir=DIR, prefix=PREFIX) as directory:
        path = os.path.join(directory, "file.txt")

        ### 1) Write Original

        with atomic_update(path) as file:
            file.write("original")

        assert read_all(path) == "original"
        assert len(glob.glob(path + "-*")) == 0

        ### 2) Fail to Write Update

        with open(path, mode="w", encoding="utf8") as file:
            file.write("original")

        try:
            with atomic_update(path) as file:
                file.write("update")
                file.flush()
                raise Exception()
        except:
            pass

        assert read_all(path) == "original"

        tmp_files = glob.glob(path + "-*")
        assert len(tmp_files) == 1
        assert read_all(tmp_files[0]) == "update"

        ### 3) Write Update

        with atomic_update(path) as file:
            file.write("update")

        assert read_all(path) == "update"
        assert len(glob.glob(path + "-*")) == 1
