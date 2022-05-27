/*! Generate parquet files from sqlite databases

This library provides two things:

1. A flexible way to generate a parquet file from a bunch of SQL statements
2. A way to generate the neccessary config for writing a whole table to a
   parquet file

This package also contains a binary crate which lets you easily compress
a whole sqlite DB into a bunch of parquet files.  This typically
gets a better compression ratio than xz, and is much faster.  See
[ARCHIVE](https://github.com/asayers/sqlite2parquet/blob/master/ARCHIVE.md)
for a comparison.

## The easy way

If you just want to dump the whole table as-is into a parquet file, you can
use the handy [`infer_schema()`].  It tries to guess the best encoding based
on the sqlite schema.

```rust
# let conn = rusqlite::Connection::open_in_memory().unwrap();
# conn.execute("CREATE TABLE my_table (category TEXT, timestamp DATETIME)", []);
let cols = sqlite2parquet::infer_schema(&conn, "my_table")
    .unwrap()
    .collect::<anyhow::Result<Vec<_>>>()
    .unwrap();
let out_path = std::path::PathBuf::from("my_table.parquet");
sqlite2parquet::write_table(&conn, "my_table", &cols, &out_path, 1_000_000).unwrap();
```

## The flexible way

Explicitly define the columns that will go in the parquet file.  One thing
to be careful about: the `SELECT` queries must all return the same number
of rows.  If not, you'll get a runtime error.

```rust
# let conn = rusqlite::Connection::open_in_memory().unwrap();
# conn.execute("CREATE TABLE my_table (category TEXT, timestamp DATETIME)", []);
use sqlite2parquet::*;
let cols = vec![
    Column {
        name: "category".to_string(),
        required: true,
        physical_type: PhysicalType::ByteArray,
        logical_type: Some(LogicalType::String),
        encoding: None,
        dictionary: true,
        query: "SELECT category FROM my_table GROUP BY category ORDER BY MIN(timestamp)".to_string(),
    },
    Column {
        name: "first_timestamp".to_string(),
        required: true,
        physical_type: PhysicalType::Int64,
        logical_type: Some(LogicalType::Timestamp(TimeType { utc: true, unit: TimeUnit::Nanos })),
        encoding: Some(Encoding::DeltaBinaryPacked),
        dictionary: false,
        query: "SELECT MIN(timestamp) FROM my_table GROUP BY category ORDER BY MIN(timestamp)".to_string(),
    },
];

let out_path = std::path::PathBuf::from("category_start_times.parquet");
write_table(&conn, "category_start_times", &cols, &out_path, 1_000_000).unwrap();
```

 */

mod conversion;
mod schema;

use crate::conversion::FromSqlite;
pub use crate::schema::*;
use anyhow::{Context, Result};
use fallible_streaming_iterator::FallibleStreamingIterator;
use parquet::file::writer::FileWriter;
use rusqlite::Connection;
use std::path::Path;
use std::sync::Arc;

fn mk_writer(
    table_name: &str,
    cols: &[Column],
    out: &Path,
) -> Result<impl parquet::file::writer::FileWriter> {
    let mut fields = cols
        .iter()
        .map(|col| Arc::new(col.as_parquet().unwrap()))
        .collect::<Vec<_>>();
    let schema = parquet::schema::types::Type::group_type_builder(table_name)
        .with_fields(&mut fields)
        .build()?;
    let mut bldr = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD);
    for col in cols {
        let path = parquet::schema::types::ColumnPath::new(vec![col.name.clone()]);
        if let Some(enc) = col.encoding() {
            bldr = bldr.set_column_encoding(path.clone(), enc)
        }
        bldr = bldr.set_column_dictionary_enabled(path, col.dictionary);
    }
    let props = bldr.build();
    Ok(parquet::file::writer::SerializedFileWriter::new(
        std::fs::File::create(out)?,
        Arc::new(schema),
        Arc::new(props),
    )?)
}

/// Creates a parquet file from a set of SQL queries.
///
/// Data is read from the sqlite DB `conn` and written to the file `out`.
/// The contents of the parquet file are defined by `cols`.  `table_name`
/// is just metadata that will go in the parquet schema - it can be an
/// arbitrary string.
///
/// The data is split into row groups of length `group_size`.  Make this
/// too small and you won't get very good compression; but be aware that in
/// parquet the row group is the unit of random access: eg. if a consumer
/// wants to read the final row in a group, it has to start at the beginning
/// of the group and decompress to the end.  I would suggest 100k to 1M is
/// a sensible default.
pub fn write_table(
    conn: &Connection,
    table_name: &str,
    cols: &[Column],
    out: &Path,
    group_size: usize,
) -> Result<parquet_format::FileMetaData> {
    write_table_with_progress(conn, table_name, cols, out, group_size, |_| Ok(()))
}

#[derive(Default, Debug, Copy, Clone)]
pub struct Progress {
    /// Number of columns written within the current (incomplete) row group
    pub n_cols: u64,
    /// Number of rows fully written
    pub n_rows: u64,
    /// Number of row groups fully written
    pub n_groups: u64,
}

/// Like [`write_table()`], but lets you provide a callback which is called
/// regularly.
///
/// For more information, see the docs for [`write_table()`].
pub fn write_table_with_progress(
    conn: &Connection,
    table_name: &str,
    cols: &[Column],
    out: &Path,
    group_size: usize,
    mut progress_cb: impl FnMut(Progress) -> Result<()>,
) -> Result<parquet_format::FileMetaData> {
    let mut wtr = mk_writer(table_name, cols, out)?;

    let mut stmnts = cols
        .iter()
        .map(|col| conn.prepare(&col.query).unwrap())
        .collect::<Vec<_>>();
    let mut selects = stmnts
        .iter_mut()
        .map(|x| x.query([]).unwrap())
        .collect::<Vec<rusqlite::Rows>>();
    for s in &mut selects {
        s.advance()?;
    }

    let mut progress = Progress::default();
    while selects[0].get().is_some() {
        write_group(&mut wtr, &mut selects, group_size, |n_cols| {
            progress_cb(Progress { n_cols, ..progress })
        })
        .context(format!("Group {}", progress.n_groups))?;
        progress.n_rows += group_size as u64;
        progress.n_groups += 1;
    }
    let metadata = wtr.close()?;
    Ok(metadata)
}

fn write_group(
    wtr: &mut impl parquet::file::writer::FileWriter,
    selects: &mut [rusqlite::Rows],
    group_size: usize,
    mut progress_cb: impl FnMut(u64) -> Result<()>,
) -> Result<()> {
    let mut group_wtr = wtr.next_row_group()?;
    let mut selects_iter = selects.iter_mut();
    let mut n_cols_written = 0;
    while let Some(mut col_wtr) = group_wtr.next_column()? {
        progress_cb(n_cols_written)?;
        let select = selects_iter.next().unwrap();

        use parquet::column::writer::ColumnWriter::*;
        let x = match &mut col_wtr {
            BoolColumnWriter(wtr) => write_col(select, group_size, wtr),
            Int32ColumnWriter(wtr) => write_col(select, group_size, wtr),
            Int64ColumnWriter(wtr) => write_col(select, group_size, wtr),
            Int96ColumnWriter(wtr) => write_col(select, group_size, wtr),
            FloatColumnWriter(wtr) => write_col(select, group_size, wtr),
            DoubleColumnWriter(wtr) => write_col(select, group_size, wtr),
            ByteArrayColumnWriter(wtr) => write_col(select, group_size, wtr),
            FixedLenByteArrayColumnWriter(wtr) => write_col(select, group_size, wtr),
        };
        x.context(format!("Column {}", n_cols_written))?;
        group_wtr
            .close_column(col_wtr)
            .context(format!("Column {}", n_cols_written))?;
        n_cols_written += 1;
    }
    wtr.close_row_group(group_wtr)?;
    Ok(())
}

fn write_col<T>(
    iter: &mut rusqlite::Rows,
    group_size: usize,
    wtr: &mut parquet::column::writer::ColumnWriterImpl<T>,
) -> Result<()>
where
    T: parquet::data_type::DataType,
    T::T: FromSqlite,
{
    let mut defs = vec![];
    let mut vals = vec![];
    for _ in 0..group_size {
        let x = match iter.get() {
            Some(x) => x,
            None => break,
        };
        let x = x.get_ref(0)?;
        if x == rusqlite::types::ValueRef::Null {
            // This is an OPTIONAL column so the max definition level is 1.
            // This is less than that, so the value is null.
            defs.push(0);
        } else {
            // If the column is OPTIONAL then the max definition level is 1.
            // This is equal to that, so the value is not null.
            //
            // If the column is REQUIRED then the definition levels should
            // technically all be zeroes, but in that case the levels will
            // be discarded so it doesn't matter.
            defs.push(1);
            vals.push(T::T::from_sqlite(x));
        }
        iter.advance()?;
    }
    wtr.write_batch(&vals, Some(&defs), None).unwrap();
    Ok(())
}
