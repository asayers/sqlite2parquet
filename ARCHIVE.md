# Sqlite archive

You can think of `sqlite2parquet` as a way to archive a sqlite database into
a compressed, read-only form.  You lose the indices, so complex queries will
be slow, but you gain serious space savings.  Testing on a real-world database:

    8.9G	db.sqlite3
    2.2G	db.sqlite3.zst
    1.1G	db.parquet

Let's compare parquet archival to simple zstd.  For zstd:

* Compression ratio of 4x (in this test)
* In-place queries are impossible; you have to decompress first.
* Decompressing back to the original database is quite fast, and you get
  the exact same sqlite database file you started with.

For parquet:

* Compression ratio of 8x (in this test)
* Some queries can be run directly on the parquet file.  You still have to
  decompress data, but only the data which is relevant to the query.
  Some queries (esp. complex queries) will be much slower than with sqlite,
  because there are no secondary indices.  Some queries (esp. simple queries)
  are faster, thanks to the pre-computed summary statistics.
* Restoring the original sqlite database is slower, in part because the
  indices need to be rebuilt.  Also, while the data should all be the same,
  the database may be slightly different internally compared to the original.
