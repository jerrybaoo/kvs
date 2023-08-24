use std::env::current_dir;

use anyhow::{anyhow, Result};
use clap::Parser;
use kvs::sled::Sled;
use kvs::{kvs::KVStore, server::Server};

#[derive(Parser, Debug)]
struct ServerCommand {
    #[arg(short, long)]
    listen_addr: String,

    #[arg(short, long, default_value_t = String::from("kvs"))]
    engine: String,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = ServerCommand::parse();

    if cli.engine.eq("kvs") {
        let engine = KVStore::new(&current_dir().map_err(|e| anyhow!(e))?)?;
        Server::<KVStore>::new(engine)?.serve(cli.listen_addr).await
    } else {
        let engine = Sled::new(&current_dir().map_err(|e| anyhow!(e))?)?;
        Server::<Sled>::new(engine)?.serve(cli.listen_addr).await
    }
}
