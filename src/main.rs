use sqlite2parquet::*;
use structopt::StructOpt;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run(Opts::from_args())
}
