use anyhow::Result;
use clap::{Parser, Subcommand};

use kvs::client::Client;

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[clap(long)]
    addr: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(about = "get value from store by key")]
    Get { key: String },
    #[command(about = "set value from store by key")]
    Set { key: String, value: String },
    #[command(name = "rm", about = "remove key from kv store")]
    Remove { key: String },
    #[command(name = "V", about = "print the version")]
    Version {},
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut client = Client::connect(&cli.addr).await?;

    match cli.command {
        Commands::Get { key } => {
            let resp = client.get(key).await?;
            println!("{}", resp)
        }
        Commands::Set { key, value } => {
            let resp = client.set(key, value).await?;
            println!("{}", resp)
        }
        Commands::Remove { key } => {
            let resp = client.remove(key).await?;
            println!("{}", resp)
        }
        Commands::Version {} => {
            let version = env!("CARGO_PKG_VERSION");
            println!("kvs version {:}", version);
        }
    };

    Ok(())
}
