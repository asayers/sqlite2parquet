use anyhow::Result;
use clap::Parser;
use rusqlite::Connection;
use sqlite2parquet::*;
use std::io::Write;
use std::path::{Path, PathBuf};

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
#[derive(Parser)]
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

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
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

fn mk_table(conn: &Connection, table: &str, out: &Path, group_size: usize) -> Result<()> {
    print!("Counting rows...");
    std::io::stdout().flush()?;
    let n_rows: u64 = conn.query_row(&format!("SELECT COUNT(1) FROM {}", table), [], |row| {
        row.get(0)
    })?;
    println!(" {n_rows}");

    let cols = sqlite2parquet::infer_schema(conn, table, n_rows)?;
    println!("{}:", table);
    for col in &cols {
        println!("    {}", col);
    }

    let n_cols = cols.len() as u64;

    let group_size = group_size.max(1);
    println!("Group size: {}", group_size);

    let t_start = std::time::Instant::now();
    let metadata = sqlite2parquet::write_table_with_progress(
        conn,
        table,
        &cols,
        out,
        group_size,
        |n_cols_written, n_rows_written, n_groups_written| {
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
        },
    )?;
    print_progress(
        n_cols,
        n_cols,
        n_rows,
        n_rows,
        metadata.row_groups.len() as u64,
        group_size,
        t_start.elapsed(),
        true,
    )?;

    summarize(&cols, metadata);
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
    out.queue(cursor::MoveToColumn(1))?
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

fn summarize(cols: &[Column], metadata: parquet_format::FileMetaData) {
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
