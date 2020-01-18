#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_log_splice.py

Unit tests for functions in the main.py.

- get_las_uwi
- merge_las_files

"""

import unittest
import pandas as pd
import re, shutil
import os, sys
from unittest.mock import MagicMock
from unittest.mock import patch

module_path = os.path.abspath(os.path.join(".."))
if module_path not in sys.path:
    sys.path.append(module_path)
module_path = os.path.abspath(os.path.join("."))
if module_path not in sys.path:
    sys.path.append(module_path)


import spark_merge

# When the execution of merge function is finished, its output will then trigger splice function.
# las_splice can not be tested on local machine. It has to run via GCP cloud function.
# las_merge can not be tested on local machine. It has to run via GCP DAG
# with a composer.


class Mergefiles(unittest.TestCase):
    """
    Tests related to the utilities/log_splice_helper.py/splice_las function...
    """

    def setUp(self):
        self.log_data = pd.DataFrame({
            "cal": [1, 2, 3, 4, 5, 6],
            "nphil": [2, 4, 5, 1, 1, 6],
            "nphil_1": [5, 6, 1, 7, 9, 9],
            "rhob": [2, 3, 4, 1, 2, 1],
            "nphil_2": [0, 1, 3, 4, 4, 1],
            "rhob_1": [2, 3, 2, 2, 2, 2]})
        self.log_data_spliced = pd.DataFrame({
            "cal": [1, 2, 3, 4, 5, 6],
            "nphil": [2, 4, 5, 1, 1, 6],
            "rhob": [2, 3, 4, 1, 2, 1]})


    def test_get_las_uwi(self):
        uwi_re = re.compile("[0-9]{14,}")
        result = spark_merge.get_las_uwi('RAW_42495335780000.las')
        self.assertEqual(result[0], '42495335780000')


    def test_get_las_uwi_from_las(self):
        uwi_re = re.compile("[0-9]{14,}")
        las_path = 'UWI.las'
        result = spark_merge.get_las_uwi(las_path)
        self.assertEqual(result[0], '42495335780000')


    def test_merge_las_files(self):
        las_path_list = ['RAW_42301318860000_MWD.las', 'RAW_42301318860000_WL_R1.las']
        merged_las, _, _ = spark_merge.merge_files(las_path_list)
        self.assertEqual(merged_las.shape[1], 18)


if __name__ == "__main__":
    unittest.main(verbosity=2)
