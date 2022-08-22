import os
import unittest

import pandas as pd

from src.lwdp.stage import stage


class TestComplex(unittest.TestCase):

    def test_multiple_types(self):
        @stage(stage1="tests/test_raw.csv", cache=True, cache_format='parquet')
        def wbj_raw(**kwargs) -> pd.DataFrame:
            return pd.read_csv(kwargs.get('stage1'))

        @stage(wbj=wbj_raw, cache=True, cache_format='json')
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

        first_cache = wbj_raw.cache.output_path / f"{wbj_raw.hash_path}.parquet"
        self.assertTrue(first_cache.exists())

        second_cache = wbj_process_something_else.cache.output_path / f"{wbj_process_something_else.hash_path}.json"
        self.assertTrue(second_cache.exists())

        os.remove(first_cache)
        os.remove(second_cache)
