import pandas as pd

# The subs below are obviously incomplete. They cover only the bare necessities.

class Schema: ...

class Table:
    @classmethod
    def from_pandas(cls, df: pd.DataFrame) -> Table: ...
    @property
    def schema(self) -> Schema: ...
