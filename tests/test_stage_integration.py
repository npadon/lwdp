import unittest

import pandas as pd

from src.stage import Stage


class TestSimple(unittest.TestCase):

    def test_some_stuff(self):
        @Stage(stage1="tests/test_raw.csv",cache=True)
        def wbj_raw(**kwargs) -> pd.DataFrame:
            return pd.read_csv(kwargs.get('stage1'))

        @Stage(wbj=wbj_raw, cache=True)
        def wbj_process_something_else(**kwargs):
            raw = kwargs.get("wbj")
            raw['new'] = 3
            return raw

        @Stage(wbj=wbj_process_something_else)
        def merge_wbj_with_other_stuff(**kwargs):
            raw = kwargs.get("wbj")
            raw['wizard'] = 5
            return raw

        merge_wbj_with_other_stuff()
