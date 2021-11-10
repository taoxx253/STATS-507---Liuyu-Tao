# ---
# jupyter:
#   jupytext:
#     formats: py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# ## Time Delta
# ##### STATS 507, Fall 2021
# ##### Liuyu Tao
# ##### liuyutao@umich.edu
# ##### October 15, 2021

# ## Overview
# - Parsing
# - to_timedelta

# ## Parsing
# - There are several different methods to construct the Timeselta, below are the examples

# +
import pandas as pd
import datetime

# read as "string"
print(pd.Timedelta("2 days 3 minutes 36 seconds"))
# similar to "datetime.timedelta"
print(pd.Timedelta(days=2, minutes=3, seconds=36))
# specify the integer and the unit of the integer
print(pd.Timedelta(2.0025, unit="d"))
