import unittest

import pandas as pd

from src.lwdp.stage import stage


class TestSimple(unittest.TestCase):

    def test_some_stuff(self):
        @stage(stage1="tests/test_raw.csv", cache=True)
        def wbj_raw(**kwargs) -> pd.DataFrame:
            return pd.read_csv(kwargs.get('stage1'))

        @stage(wbj=wbj_raw, cache=True)
        def wbj_process_something_else(**kwargs):
            raw = kwargs.get("wbj")
            raw['new'] = 3
            return raw

        @stage(wbj=wbj_process_something_else)
        def merge_wbj_with_other_stuff(**kwargs):
            raw = kwargs.get("wbj")
            raw['wizard'] = 5
            return raw

        merge_wbj_with_other_stuff()
