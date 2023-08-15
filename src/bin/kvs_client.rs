use std::io::Write;
use std::net::TcpStream;

use anyhow::Result;
use clap::{Parser, Subcommand};

use kvs::server::{Request, Response};

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(short, long)]
    server_addr: String,
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
    let cli = Cli::parse();
    let mut stream = TcpStream::connect(cli.server_addr)?;

    let request = match cli.command {
        Commands::Get { key } => {
            let req = bson::to_vec(&Request::Get(key))?;
            Some(req)
        }
        Commands::Set { key, value } => {
            let req = bson::to_vec(&Request::Set(key, value))?;
            Some(req)
        }
        Commands::Remove { key } => {
            let req = bson::to_vec(&Request::Remove(key))?;
            Some(req)
        }
        Commands::Version {} => {
            let version = env!("CARGO_PKG_VERSION");
            println!("kvs version {:}", version);
            None
        }
    };

    if let Some(bz) = request {
        stream.write(&bz)?;
        bson::from_reader::<_, Response>(stream).map_or_else(
            |e| println!("read response from stream failed, reason:{:}", e),
            |r| println!("response: {:}", r.response),
        )
    }

    Ok(())
}
