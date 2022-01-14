/*! Generate parquet files from sqlite databases

This library provides two things:

1. A flexible way to generate a parquet file from a bunch of SQL statements
2. A way to generate the neccessary config for writing a whole table to a parquet file

## The flexible way

Explicitly define the columns that will go in the parquet file.  One thing
to be careful about: the `SELECT` queries must all return the same number
of rows.  If not, you'll get a runtime error.

```rust
let conn = rusqlite::Connection::open_in_memory().unwrap();
conn.execute("CREATE TABLE my_table (category TEXT, timestamp DATETIME)", []);

use parquet::basic::*;
use parquet_format::NanoSeconds;
let cols = vec![
    sqlite2parquet::Column {
        name: "category".to_string(),
        repetition: Repetition::REQUIRED,
        length: 0,
        physical_type: Type::BYTE_ARRAY,
        logical_type: Some(LogicalType::STRING(StringType::new())),
        encoding: None,
        query: "SELECT category FROM my_table GROUP BY category ORDER BY MIN(timestamp)".to_string(),
    },
    sqlite2parquet::Column {
        name: "first_timestamp".to_string(),
        repetition: Repetition::REQUIRED,
        length: 0,
        physical_type: Type::INT64,
        logical_type: Some(LogicalType::TIMESTAMP(TimestampType::new(true, TimeUnit::NANOS(NanoSeconds::new())))),
        encoding: Some(Encoding::DELTA_BINARY_PACKED),
        query: "SELECT MIN(timestamp) FROM my_table GROUP BY category ORDER BY MIN(timestamp)".to_string(),
    },
];

let out_path = std::path::PathBuf::from("category_start_times.parquet");
let cb = |_, _, _| Ok(());
sqlite2parquet::write_table(&conn, "category_start_times", &cols, &out_path, 1_000_000, cb).unwrap();
```

## The easy way

If you just want to dump the whole table as-is into a parquet file, you can
use the handy [`infer_schema()`].  It tries to guess the best encoding based
on the sqlite schema.

```rust
let conn = rusqlite::Connection::open_in_memory().unwrap();
conn.execute("CREATE TABLE my_table (category TEXT, timestamp DATETIME)", []);

let cols = sqlite2parquet::infer_schema(&conn, "my_table").unwrap();
let out_path = std::path::PathBuf::from("my_table.parquet");
let cb = |_, _, _| Ok(());
sqlite2parquet::write_table(&conn, "my_table", &cols, &out_path, 1_000_000, cb).unwrap();
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
        .map(|col| Arc::new(col.clone().as_parquet().unwrap()))
        .collect::<Vec<_>>();
    let schema = parquet::schema::types::Type::group_type_builder(table_name)
        .with_fields(&mut fields)
        .build()?;
    let mut bldr = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD);
    for col in cols {
        if let Some(enc) = col.encoding {
            bldr = bldr.set_column_encoding(
                parquet::schema::types::ColumnPath::new(vec![col.name.clone()]),
                enc,
            )
        }
    }
    let props = bldr.build();
    Ok(parquet::file::writer::SerializedFileWriter::new(
        std::fs::File::create(out)?,
        Arc::new(schema),
        Arc::new(props),
    )?)
}

pub fn write_table<'a>(
    conn: &Connection,
    table_name: &str,
    cols: &[Column],
    out: &Path,
    group_size: usize,
    mut progress_cb: impl FnMut(u64, u64, u64) -> Result<()>,
) -> Result<(u64, parquet_format::FileMetaData)> {
    let mut wtr = mk_writer(table_name, &cols, out)?;

    let mut stmnts = cols
        .iter()
        .map(|col| conn.prepare(&col.query).unwrap())
        .collect::<Vec<_>>();
    let mut selects = stmnts
        .iter_mut()
        .map(|x| x.query([]).unwrap())
        .collect::<Vec<_>>();
    for s in &mut selects {
        s.advance()?;
    }

    let mut n_rows_written = 0;
    let mut n_groups_written = 0;
    while selects[0].get().is_some() {
        let selects = selects
            .iter_mut()
            .map(|x| x.take(group_size))
            .collect::<Vec<_>>();
        write_group(&mut wtr, selects, |n_cols_written| {
            progress_cb(n_cols_written, n_rows_written, n_groups_written)
        })
        .context(format!("Group {}", n_groups_written))?;
        n_rows_written += group_size as u64;
        n_groups_written += 1;
    }
    let metadata = wtr.close()?;
    Ok((n_groups_written, metadata))
}

fn write_group<'a>(
    wtr: &mut impl parquet::file::writer::FileWriter,
    mut selects: Vec<
        impl FallibleStreamingIterator<Item = rusqlite::Row<'a>, Error = rusqlite::Error>,
    >,
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
            BoolColumnWriter(wtr) => write_col(select, wtr),
            Int32ColumnWriter(wtr) => write_col(select, wtr),
            Int64ColumnWriter(wtr) => write_col(select, wtr),
            Int96ColumnWriter(wtr) => write_col(select, wtr),
            FloatColumnWriter(wtr) => write_col(select, wtr),
            DoubleColumnWriter(wtr) => write_col(select, wtr),
            ByteArrayColumnWriter(wtr) => write_col(select, wtr),
            FixedLenByteArrayColumnWriter(wtr) => write_col(select, wtr),
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

fn write_col<'a, T>(
    mut iter: impl FallibleStreamingIterator<Item = rusqlite::Row<'a>, Error = rusqlite::Error>,
    wtr: &mut parquet::column::writer::ColumnWriterImpl<T>,
) -> Result<()>
where
    T: parquet::data_type::DataType,
    T::T: FromSqlite,
{
    let mut reps = vec![];
    let mut defs = vec![];
    let mut vals = vec![];
    loop {
        let x = match iter.get() {
            Some(x) => x,
            None => break,
        };
        let x = x.get_ref(0)?;
        if x == rusqlite::types::ValueRef::Null {
            reps.push(0);
            defs.push(0);
        } else {
            reps.push(1);
            defs.push(1);
            vals.push(T::T::from_sqlite(x));
        }
        iter.advance()?;
    }
    wtr.write_batch(&vals, Some(&defs), Some(&reps)).unwrap();
    Ok(())
}
