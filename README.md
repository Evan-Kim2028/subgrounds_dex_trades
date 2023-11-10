# Subgrounds_dex_trades

Run a python script file to download data from a subgraph query. Can combine the subgraph output to create a table similar to dune's `dex_trades`

### Instructions

Run with `python wip/univ3_async.py 2023-10-01 30` to download 30 days worth of data starting at 2023-10-01

### Notes

- Sometimes I get an installation error when I do `pip install -e`, but it gets fixed with `pip install -U setuptools setuptools_scm wheel`

- unsure how to deal with this error:

```python
ResourceWarning: Enable tracemalloc to get the object allocation traceback
/usr/lib/python3.10/asyncio/selector_events.py:710: ResourceWarning: unclosed transport <_SelectorSocketTransport fd=25 read=idle write=<idle, bufsize=0>>
  _warn(f"unclosed transport {self!r}", ResourceWarning, source=self)
ResourceWarning: Enable tracemalloc to get the object allocation traceback
```

### Script Structure

Uses asyncio to make asynchronous queries to the subgraph and aiofiles to asynchronously write the data the data storage folder.

- query size is currently hard coded (univ3 messari doesn't work for larger query sizes of 5000, only smaller ones like 250)
- can't consistently download files, sometimes the async loop crashes before the query is finished...its a weird behavior. It doesn't seem to be a problem for the univ3 substreams subgraph, but is consistenyly a problem with the univ3 messari subgraph. It's also interesting to observe that the substreams subgraph has a much higher query output speed as well.
