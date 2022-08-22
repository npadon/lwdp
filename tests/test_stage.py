import os
import unittest

import pandas as pd

from src.lwdp.stage import stage


class TestStage(unittest.TestCase):

    def test_two_single_hashes_are_different(self):
        @stage(stage1="tests/test_raw.csv")
        def some_stage(**kwargs) -> pd.DataFrame:
            return pd.read_csv(kwargs.get('stage1'))

        @stage(stage1="tests/test_raw2.csv")
        def some_other_stage(**kwargs) -> pd.DataFrame:
            return pd.read_csv(kwargs.get('stage1'))

        self.assertNotEqual(some_stage._single_stage_hash.hexdigest(),
                            some_other_stage._single_stage_hash.hexdigest())

    def test_that_cache_works(self):
        @stage(stage1="tests/test_raw.csv", cache=True)
        def some_stage(**kwargs) -> pd.DataFrame:
            return pd.read_csv(kwargs.get('stage1'))

        # run the simple pipeline
        some_stage()
        output_path_for_cacheing = some_stage.cache.output_path / f"{some_stage.hash_path}.{some_stage.cache.format}"
        self.assertTrue(output_path_for_cacheing.exists())  # check that cache written
        os.remove(output_path_for_cacheing)
