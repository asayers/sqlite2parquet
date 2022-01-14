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
  back all your indices etc.

For parquet:

* Compression ratio of 8x (in this test)
* Some queries can be run directly on the parquet file.  You still have to
  decompress data, but only the data which is relevant to the query.
  Some queries (esp. complex queries) will be much slower than with sqlite,
  because there are no secondary indices.  Some queries (esp. simple queries)
  are faster, thanks to the pre-computed summary statistics.
* Restoring the original sqlite database is slower, in part because the
  indices need to be rebuilt.

Note that roundtripping through parquet and back will (_should_!) restore all
your data, but the internals of the sqlite DB may be different.  Bear this
in mind if you're eg. rsyncing sqlite files around.  You may want to look
into the [sha3sum] dot command.

[sha3sum]: https://www.sqlite.org/cli.html#cryptographic_hashes_of_database_content
