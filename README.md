# Lightweight Data Pipeline (LWDP)

LWDP attempts to fill the niche for structuring pure-Python data transformations,
with robust data- and code-based-cacheing across a few locales. 

Because sometimes Spark or Dask or AWS Glue or anything other than a 5kb library and some
dumbly hashed files is just too much.

LWDP is meant for the case where you're doing a few data transformations, possibly across
multiple input file types (csvs, Excel, parquet, etc.). Each of these files can generally (although
not strictly) be held in memory. 25 csvs with structured transformations that you'd like to keep organized
and possibly streamline with cacheing? 

LWDP **could** be the answer.

If the data changes or your code changes, you want to be able to refresh the data pipeline *once* - and,
ideally, only those parts of the data pipeline who need to be refreshed.

# Installation

You should be able to install from PyPi with `pip install lwdp`

# Usage

Decorate functions to represent a stage; and chain together functions
to make a pipeline.

```python
# read from a raw file and cache
from lwdp import stage
import pandas as pd

@stage(some_raw_file="raw/input.csv", cache=True)
def stg_read_format_raw(**kwargs) -> pd.DataFrame:
    pdf = pd.read_csv(kwargs.get('some_raw'))
    # some stuff to clean it
    return pdf

# read from a previous stage and cache
@stage(basic_raw=stg_read_format_raw, cache=True)
def stg_format_more(**kwargs) -> pd.DataFrame:
    raw = kwargs.get('basic_raw')
    raw['new_analysis_column'] = 3
    return raw

# read from a previous stage without cacheing
@stage(formatted_src=stg_format_more)
def stg_final_process(**kwargs) -> pd.DataFrame:
    result = kwargs.get("formatted_src")
    result['wizard'] = 5
    return result

final_process()
```

Just call the last stage in the pipeline (as you would any
other function) to run all ancestors, reading/writing from
cached stages as needed.