use std::io::Write;
use std::net::TcpStream;

use anyhow::Result;
use clap::{Parser, Subcommand};

use kvs::server::{Request, Response};

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(about = "get value from store by key")]
    Get { key: String, server_addr: String },
    #[command(about = "set value from store by key")]
    Set {
        key: String,
        value: String,
        server_addr: String,
    },
    #[command(name = "rm", about = "remove key from kv store")]
    Remove { key: String, server_addr: String },
    #[command(name = "V", about = "print the version")]
    Version {},
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let request = match cli.command {
        Commands::Get { key, server_addr } => {
            let req = bson::to_vec(&Request::Get(key))?;
            Some((req, server_addr))
        }
        Commands::Set {
            key,
            value,
            server_addr,
        } => {
            let req = bson::to_vec(&Request::Set(key, value))?;
            Some((req, server_addr))
        }
        Commands::Remove { key, server_addr } => {
            let req: Vec<u8> = bson::to_vec(&Request::Remove(key))?;
            Some((req, server_addr))
        }
        Commands::Version {} => {
            let version = env!("CARGO_PKG_VERSION");
            println!("kvs version {:}", version);
            None
        }
    };

    if let Some((bz, server_addr)) = request {
        let mut stream = TcpStream::connect(&server_addr)?;
        stream.write(&bz)?;
        bson::from_reader::<_, Response>(stream).map_or_else(
            |e| println!("read response from stream failed, reason:{:}", e),
            |r| {
                if !r.response.is_empty() {
                    println!("{}", r.response)
                }
            },
        )
    }

    Ok(())
}
