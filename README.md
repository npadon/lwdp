# Lightweight Data Pipeline (LWDP)

LWDP attempts to fill the niche for structuring pure-Python data transformations,
with robust data- and code-based-cacheing across a few locales. 

Because sometimes Spark or Dask or AWS Glue or anything other than a 5kb library and some
dumbly hashed files is just too much.

LWDP is meant for the case where you're doing a few data transformations, possibly across
multiple input file types (csvs, Excel, parquet, etc.). Each of these files can generally (although
not strictly) be held in memory. 25 csvs with structured transformations that you'd like to keep organized
and possibly streamline with cacheing? LWDP could be the answer.

If the data changes or your code changes, you want to be able to refresh the data pipeline *once* - and,
ideally, only those parts of the data pipeline who need to be refreshed.

# Installation

You should be able to install from PyPi with `pip install lwdp`