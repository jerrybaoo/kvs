use std::env::current_dir;

use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};

use kvs::store::KVStore;

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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

fn main() -> Result<()> {
    let mut server: KVStore = KVStore::new(&current_dir().map_err(|e| anyhow!(e))?)?;
 
    let cli = Cli::parse();

    let res = match cli.command {
        Commands::Get { key } => {
            let value = server.get(&key)?;
            println!("key:{}, value:{}", &key, value);
            Ok(())
        }
        Commands::Set { key, value } => server.set(&key, &value),
        Commands::Remove { key } => server.remove(&key),
        Commands::Version {} => {
            let version = env!("CARGO_PKG_VERSION");
            println!("kvs version {:}", version);
            Ok(())
        }
    };

    if let Err(e) = res {
        eprintln!("error:{:}", e)
    }

    Ok(())
}
