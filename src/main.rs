use anyhow::Result;
use clap::Parser;
use rusqlite::Connection;
use sqlite2parquet::*;
use std::collections::HashMap;
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
    #[structopt(long)]
    pub config: Option<PathBuf>,
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
    tracing_subscriber::fmt::init();

    let mut config: HashMap<String, Vec<Column>> = if let Some(path) = opts.config {
        serde_yaml::from_reader(std::fs::File::open(path)?)?
    } else {
        HashMap::default()
    };

    let conn = rusqlite::Connection::open(&opts.sqlite)?;

    let mut tables: Vec<String> = if !opts.table.is_empty() {
        opts.table
    } else if !config.is_empty() {
        config.keys().cloned().collect()
    } else {
        let mut table_info = conn.prepare(
            "SELECT name
            FROM sqlite_schema
            WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'",
        )?;
        let x = table_info
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        x
    };
    if opts.include_schema {
        tables.push("sqlite_schema".to_string());
    }

    std::fs::create_dir_all(&opts.out_dir)?;
    for table in tables {
        let out = opts.out_dir.join(format!("{}.parquet", &table));
        let config = config.remove(&table);
        mk_table(&conn, &table, &out, config, opts.group_size)?;
    }
    Ok(())
}

const COLUMN_HEADER: &str =
    "Column                 Physical type   Encoding             Logical type               SQL";

fn mk_table(
    conn: &Connection,
    table: &str,
    out: &Path,
    // Infer if `None`
    config: Option<Vec<Column>>,
    group_size: usize,
) -> Result<()> {
    print!("Counting rows...");
    std::io::stdout().flush()?;
    let n_rows: u64 = if let Some(config) = config.as_ref() {
        conn.query_row(
            &format!("SELECT COUNT(1) FROM ({})", config[0].query),
            [],
            |row| row.get(0),
        )?
    } else {
        conn.query_row(&format!("SELECT COUNT(1) FROM {}", table), [], |row| {
            row.get(0)
        })?
    };
    println!(" {n_rows}");

    let cols: Vec<Column> = if let Some(cols) = config {
        println!("    {}", COLUMN_HEADER);
        for col in &cols {
            println!("    {}", col);
        }
        cols
    } else {
        println!("Inferring schema for {table}...");
        println!("    {}", COLUMN_HEADER);
        let t_start = std::time::Instant::now();
        let cols = sqlite2parquet::infer_schema(conn, table)?
            .inspect(|col| {
                if let Ok(col) = col {
                    println!("    {}", col)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        println!("Inferred schema in {:?}", t_start.elapsed());
        cols
    };

    let total = Progress {
        n_cols: cols.len() as u64,
        n_rows,
        n_groups: (n_rows + group_size as u64 - 1) / group_size as u64,
    };

    let group_size = group_size.max(1);
    println!("Group size: {}", group_size);

    let t_start = std::time::Instant::now();
    let metadata = sqlite2parquet::write_table_with_progress(
        conn,
        table,
        &cols,
        out,
        group_size,
        |written| print_progress(written, total, group_size, t_start.elapsed(), false),
    )?;
    let final_prog = Progress {
        n_cols: total.n_cols,
        n_rows: metadata.num_rows as u64,
        n_groups: metadata.row_groups.len() as u64,
    };
    print_progress(final_prog, final_prog, group_size, t_start.elapsed(), true)?;

    summarize(&cols, metadata);
    Ok(())
}

fn print_progress(
    written: Progress,
    total: Progress,
    group_size: usize,
    time: std::time::Duration,
    finished: bool,
) -> Result<()> {
    use crossterm::*;
    let out = std::io::stderr();
    let mut out = out.lock();
    let this_group = (total.n_rows - written.n_rows).min(group_size as u64) as f64;
    let pc = (written.n_rows as f64 + this_group * written.n_cols as f64 / total.n_cols as f64)
        / total.n_rows as f64
        * 100.0;
    out.queue(cursor::MoveToColumn(1))?
        .queue(terminal::Clear(terminal::ClearType::CurrentLine))?
        .queue(style::Print(format_args!(
            "[{:.2}%] Wrote {}{} rows as {} group{} in {:.1?}{}",
            pc,
            written.n_rows,
            if finished {
                "".to_string()
            } else {
                format!(" of {} ", total.n_rows)
            },
            written.n_groups,
            if written.n_groups == 1 { "" } else { "s" },
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
