use std::env::current_dir;

use anyhow::{anyhow, Result};
use clap::Parser;
use kvs::{server::Server, store::KVStore};

#[derive(Parser, Debug)]
struct ServerCommand {
    #[arg(short, long)]
    listen_addr: String,

    #[arg(short, long, default_value_t = String::from("kvs"))]
    engine: String,
}

fn main() -> Result<()> {
    let cli = ServerCommand::parse();

    let mut server = if cli.engine.eq("kvs") {
        let engine = KVStore::new(&current_dir().map_err(|e| anyhow!(e))?)?;
        Server::<KVStore>::new(engine)
    } else {
        let engine = KVStore::new(&current_dir().map_err(|e| anyhow!(e))?)?;
        Server::<KVStore>::new(engine)
    }?;

    server.serve(cli.listen_addr)?;

    Ok(())
}
