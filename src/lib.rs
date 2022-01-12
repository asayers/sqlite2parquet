mod conversion;
mod schema;

use crate::conversion::FromSqlite;
pub use crate::schema::*;
use anyhow::Result;
use fallible_streaming_iterator::FallibleStreamingIterator;
use parquet::file::writer::FileWriter;
use rusqlite::Connection;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use structopt::StructOpt;

/// Extracts data from a sqlite3 DB and writes to parquet files.
///
/// One parquet file will be creates per table in the DB.  If you want to
/// select which tables to extract, use the --table flag.  It can be passed
/// multiple times to specify a set of tables.  If --table is not pass,
/// all tables will be extracted.
///
/// We do our best to guess a good encoding for each column.  This is mostly
/// done based on the SQL schema.  More advanced analysis is planned.
///
/// The output directory will be created if it doesn't exist.  If it already
/// contains parquet files with conflicting names, those files will be
/// overwritten.
///
/// Rows will be written in batches.  This is mostly to ensure that
/// sqlite2parquet can run in constant memory.  You can choose the batch
/// size with --group-size.  Increasing this number improves the amount of
/// compression we're able to achieve, but will cause sqlite2parquet to use
/// more memory.
#[derive(StructOpt)]
pub struct Opts {
    /// The sqlite3 database to read from
    pub sqlite: PathBuf,
    /// The directory to put parquet files in
    pub out_dir: PathBuf,
    /// The table(s) to extract
    #[structopt(long, short)]
    pub table: Vec<String>,
    /// The size of each row group
    #[structopt(long, short, default_value = "1000000")]
    pub group_size: usize,
    #[structopt(long)]
    pub include_schema: bool,
}

pub fn run(opts: Opts) -> Result<()> {
    let conn = rusqlite::Connection::open(&opts.sqlite)?;
    let mut tables = if opts.table.is_empty() {
        let mut table_info = conn.prepare(
            "SELECT name
            FROM sqlite_schema
            WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'",
        )?;
        let x = table_info
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;
        x
    } else {
        opts.table
    };
    if opts.include_schema {
        tables.push("sqlite_schema".to_string());
    }
    std::fs::create_dir_all(&opts.out_dir)?;
    for table in tables {
        let out = opts.out_dir.join(format!("{}.parquet", table));
        mk_table(&conn, &table, &out, opts.group_size)?;
    }
    Ok(())
}

pub fn mk_writer(
    table_name: &str,
    cols: &[ColInfo],
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

fn mk_table(conn: &Connection, table: &str, out: &Path, group_size: usize) -> Result<()> {
    let cols = infer_schema(conn, table)?;
    let n_cols = cols.len() as u64;

    print!("Counting rows...");
    std::io::stdout().flush()?;
    let n_rows: u64 = conn.query_row(&format!("SELECT COUNT(1) FROM {}", table), [], |row| {
        row.get(0)
    })?;
    println!(" {}", n_rows);

    let group_size = group_size.max(1);
    println!("Group size: {}", group_size);

    let mut wtr = mk_writer(table, &cols, out)?;

    let mut select_statements = cols
        .iter()
        .map(|col| {
            conn.prepare(&format!("SELECT {} FROM {}", col.name, table))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut selects = select_statements
        .iter_mut()
        .map(|x| x.query([]).unwrap())
        .collect::<Vec<_>>();
    for s in &mut selects {
        s.advance()?;
    }

    let mut n_rows_written = 0;
    let mut n_groups_written = 0;
    let t_start = std::time::Instant::now();
    while selects[0].get().is_some() {
        let selects = selects
            .iter_mut()
            .map(|x| x.take(group_size))
            .collect::<Vec<_>>();
        write_group(&mut wtr, selects, |n_cols_written| {
            print_progress(
                n_cols_written,
                n_cols,
                n_rows_written,
                n_rows,
                n_groups_written,
                group_size,
                t_start.elapsed(),
                false,
            )
        })?;
        n_rows_written += group_size as u64;
        n_groups_written += 1;
    }
    print_progress(
        n_cols,
        n_cols,
        n_rows,
        n_rows,
        n_groups_written,
        group_size,
        t_start.elapsed(),
        true,
    )?;

    let metadata = wtr.close()?;
    summarize(&cols, metadata);
    Ok(())
}

pub fn write_group<'a>(
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
        match &mut col_wtr {
            BoolColumnWriter(wtr) => write_col(select, wtr)?,
            Int32ColumnWriter(wtr) => write_col(select, wtr)?,
            Int64ColumnWriter(wtr) => write_col(select, wtr)?,
            Int96ColumnWriter(wtr) => write_col(select, wtr)?,
            FloatColumnWriter(wtr) => write_col(select, wtr)?,
            DoubleColumnWriter(wtr) => write_col(select, wtr)?,
            ByteArrayColumnWriter(wtr) => write_col(select, wtr)?,
            FixedLenByteArrayColumnWriter(wtr) => write_col(select, wtr)?,
        }
        group_wtr.close_column(col_wtr)?;
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

fn print_progress(
    n_cols_written: u64,
    n_cols: u64,
    n_rows_written: u64,
    n_rows: u64,
    n_groups: u64,
    group_size: usize,
    time: std::time::Duration,
    finished: bool,
) -> Result<()> {
    use crossterm::*;
    let out = std::io::stderr();
    let mut out = out.lock();
    let this_group = (n_rows - n_rows_written).min(group_size as u64) as f64;
    let pc = (n_rows_written as f64 + this_group * n_cols_written as f64 / n_cols as f64)
        / n_rows as f64
        * 100.0;
    out.queue(cursor::MoveToColumn(0))?
        .queue(terminal::Clear(terminal::ClearType::CurrentLine))?
        .queue(style::Print(format_args!(
            "[{:.2}%] Wrote {}{} rows as {} group{} in {:.1?}{}",
            pc,
            if finished {
                "".to_string()
            } else {
                format!("{} of ", n_rows_written)
            },
            n_rows,
            n_groups,
            if n_groups == 1 { "" } else { "s" },
            time,
            if finished { "\n" } else { "..." },
        )))?
        .flush()?;
    Ok(())
}

fn summarize(cols: &[ColInfo], metadata: parquet_format::FileMetaData) {
    fn fmt_bytes(bytes: i64) -> String {
        use thousands::Separable;
        format!("{:>9} KiB", (bytes / 1024).separate_with_commas())
    }

    let mut total_bytes = 0;
    let mut by_col_bytes = cols.iter().map(|_| 0).collect::<Vec<_>>();
    for group in &metadata.row_groups {
        total_bytes += group.total_byte_size;
        for (meta, col_bytes) in group.columns.iter().zip(&mut by_col_bytes) {
            if let Some(meta) = &meta.meta_data {
                *col_bytes += meta.total_compressed_size;
            }
        }
    }
    println!("Total                  {}", fmt_bytes(total_bytes));
    for (col, col_bytes) in cols.iter().zip(by_col_bytes) {
        println!(
            "  {:20} {} ({:>2.0}%)",
            col.name,
            fmt_bytes(col_bytes),
            col_bytes as f64 / total_bytes as f64 * 100.0,
        );
    }
}
