//! A transaction engine that takes transcation history as input
//! and outputs the result of transactions in stdout
//!
//! #Input
//!
//! Input is a csv file with following format
//! type, client, tx, Decimal
//! deposit, 1, 1, 1.0
//!
//! #Output
//!
//! Output is a csv file with following format
//! client, available, held, total, locked
//! 1, 1.5, 0.0, 1.5, false
//! 2, 2.0, 0.0, 2.0, false
//!
//! #How to run
//! cargo run -- <path-for-input>

use engine::Engine;
use io::{reader::Reader, writer::Writer};
use log::error;
use std::path::PathBuf;
use structopt::StructOpt;

mod engine;
mod error;
mod io;

#[derive(Debug, StructOpt)]
#[structopt(name = "transaction-handler")]
struct Opt {
    #[structopt(parse(from_os_str))]
    transaction_path: PathBuf,
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    let mut engine = Engine::new();
    match Reader::new(opt.transaction_path).await {
        Err(err) => {
            //some irrecoverable happend, so log this error then exit
            error!("{:?}", err)
        }
        Ok(mut reader) => {
            let mut writer = Writer::new(); //write to std::out
            if let Err(err) = engine.process(&mut reader, &mut writer).await {
                //some irrecoverable happend, so log this error then exit
                error!("{:?}", err)
            }
        }
    }
}
