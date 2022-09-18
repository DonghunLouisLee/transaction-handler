use std::num::ParseIntError;

use thiserror::Error;
use tokio::io;

#[derive(Error, Debug)]
pub(crate) enum CustomError {
    ///Following errors are not okay to happen, and should stop the engine since this means input file is corrupted
    #[error("Undefined Action")]
    UndefinedAction,
    #[error("string could not be parsed into decimal")]
    DecimalParseError(#[from] rust_decimal::Error),
    #[error("string could not be parsed into int")]
    IntParseError(#[from] ParseIntError),
    #[error("file could not be opened")]
    FileOpenError(#[from] io::Error),

    ///Following Errors are okay to happen and should not stop the engine
    #[error("Not enough account balance")]
    AccountBalanceNotEnough,
    #[error("Account is Locked")]
    LockedAccount,
    #[error("Duplicated transcation id")]
    DuplicatedTransactionId,
    #[error("Non existing transaction id")]
    NonExistingTransactionId,
    #[error("Undefined Behaviour")]
    UndefinedBehaviour,
    #[error("Not under dispute")]
    NotUnderDispute,
}
