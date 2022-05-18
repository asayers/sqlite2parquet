# sqlite2parquet

sqlite2parquet lets you generate parquet files from a sqlite database.  It's
available as a [library] and as a CLI application; they're feature-equivalent,
so use whichever is most convenient.  See [here][archive] for some notes on
archiving sqlite databases as parquet files.  The code is in the public domain.

## The CLI application

Install it like this:

```console
$ cargo install sqlite2parquet
```

## The library

If you use the library directly, you'll probably want to remove the CLI-related
dependencies, so set `default-features = false` in your Cargo.toml.

[library]: https://docs.rs/sqlite2parquet
[archive]: https://github.com/asayers/sqlite2parquet/blob/master/ARCHIVE.md
