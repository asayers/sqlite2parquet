use sqlite2parquet::*;
use structopt::StructOpt;

fn main() -> anyhow::Result<()> {
    run(Opts::from_args())
}
