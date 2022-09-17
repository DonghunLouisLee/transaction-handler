//todo documentation should come here

use engine::Engine;
use io::{reader::Reader, writer::Writer};
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

//todo use logger
#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    let mut engine = Engine::new();
    let mut reader = Reader::new(opt.transaction_path).await.unwrap();
    let mut writer = Writer::new(); //write to std::out
    if let Err(err) = engine.process(&mut reader, &mut writer).await {
        //some irrecoverable happend, so log this error then exit
        println!("{:?}", err)
    }
}
