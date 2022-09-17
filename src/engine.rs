use std::{collections::HashMap, str::FromStr};

use crate::{
    error::CustomError,
    io::{reader::Reader, writer::Writer},
};

type ClientId = u16;
type TransactionId = u32;
pub(crate) struct Engine {
    clients: HashMap<ClientId, Account>,
}
use anyhow::Result;
use csv_async::StringRecord;
use futures::stream::StreamExt;
use rust_decimal::Decimal;
use tokio::io::AsyncWriteExt;

const PRECISION: u32 = 4;

//this engine can be used as a separate module
impl Engine {
    pub(crate) fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }
    pub(crate) async fn process(
        &mut self,
        reader: &mut Reader,
        writer: &mut Writer,
    ) -> Result<(), CustomError> {
        while let Some(value) = reader.get_inner().records().next().await {
            //skip if value is
            let transaction = Transaction::from_record(value.unwrap()).unwrap();
            let client_id = transaction.get_client_id();
            let account = self.clients.entry(client_id);

            match account {
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    let mut new_account = Account::new(client_id);
                    //todo handle error here, should we stop or now depending on the error types
                    if let Err(err) = new_account.handle_transaction(transaction) {
                        match err {
                            CustomError::UndefinedAction
                            | CustomError::DecimalParseError(_)
                            | CustomError::IntParseError(_) => return Err(err),
                            CustomError::AccountBalanceNotEnough | CustomError::LockedAccount => {
                                //simply log error and continue
                                continue;
                            }
                        }
                    }
                    vacant.insert(new_account);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if let Err(err) = entry.get_mut().handle_transaction(transaction) {
                        match err {
                            CustomError::UndefinedAction
                            | CustomError::DecimalParseError(_)
                            | CustomError::IntParseError(_) => return Err(err),
                            CustomError::AccountBalanceNotEnough | CustomError::LockedAccount => {
                                //simply log error and continue
                                continue;
                            }
                        }
                    }
                }
            }
        }

        //now updating state is done
        // writer header
        let header = format!("client,available,held,total,locked\n");
        writer
            .get_inner()
            .write_all(header.as_bytes())
            .await
            .unwrap();
        //write out to stdout
        for (client_id, account) in &self.clients {
            let output = format!(
                "{},{},{},{},{}\n",
                client_id, account.available, account.held, account.total, account.is_locked
            );
            writer
                .get_inner()
                .write_all(output.as_bytes())
                .await
                .unwrap();
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Transaction {
    action_type: Action,
    client_id: ClientId,
    transaction_id: TransactionId,
    decimal: Option<Decimal>,
    is_under_dispute: bool,
}

//import assumption I've made is that
//withdrawl cannot be a subject of dispute, thus nor resolve and chargeback
//only deposit can be da subject of dispute
#[derive(Copy, Clone, Debug)]
enum Action {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl FromStr for Action {
    type Err = CustomError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "deposit" => Ok(Action::Deposit),
            "withdrawal" => Ok(Action::Withdrawal),
            "dispute" => Ok(Action::Dispute),
            "resolve" => Ok(Action::Resolve),
            "chargeback" => Ok(Action::Chargeback),
            _ => Err(CustomError::UndefinedAction),
        }
    }
}

impl Transaction {
    fn from_record(record: StringRecord) -> Result<Self, CustomError> {
        let action_type = Action::from_str(record.get(0).unwrap())?;
        let client_id = ClientId::from_str(record.get(1).unwrap())?;
        let transaction_id = TransactionId::from_str(record.get(2).unwrap())?;
        match action_type {
            Action::Deposit | Action::Withdrawal => {
                let decimal = Decimal::from_str(record.get(2).unwrap())?;
                Ok(Transaction {
                    action_type,
                    client_id,
                    transaction_id,
                    decimal: Some(decimal),
                    is_under_dispute: false,
                })
            }
            Action::Dispute | Action::Resolve | Action::Chargeback => Ok(Transaction {
                action_type,
                client_id,
                transaction_id,
                decimal: None,
                is_under_dispute: false,
            }),
        }
    }

    fn get_action_type(&self) -> Action {
        self.action_type
    }

    fn get_client_id(&self) -> ClientId {
        self.client_id
    }
}

#[derive(Debug)]
struct Account {
    _client_id: ClientId,
    //should keep track of following transactions
    //deposit, withdrawl, dispute
    //resolve and chargeback are final status so do not need to be saved
    transactions: HashMap<TransactionId, Transaction>,
    is_locked: bool,
    available: Decimal, //The total funds that are available for trading, staking, withdrawal, etc. This should be equal to the total - held Decimals
    held: Decimal, //The total funds that are held for dispute. This should be equal to total available Decimals
    total: Decimal, //The total funds that are available or held. This should be equal to available held
}

impl Account {
    fn new(client_id: ClientId) -> Self {
        Self {
            _client_id: client_id,
            transactions: HashMap::new(),
            is_locked: false,
            available: Decimal::new(0, PRECISION),
            held: Decimal::new(0, PRECISION),
            total: Decimal::new(0, PRECISION),
        }
    }

    fn handle_transaction(&mut self, transaction: Transaction) -> Result<(), CustomError> {
        //first check if this account is not locked,
        //if locked, return error
        if self.is_locked {
            return Err(CustomError::LockedAccount);
        }
        match transaction.get_action_type() {
            Action::Deposit => {
                self.available += transaction.decimal.unwrap();
                self.total += transaction.decimal.unwrap();
                self.transactions
                    .insert(transaction.transaction_id, transaction);
            }
            Action::Withdrawal => {
                //withdrawal should fail it total amount is not enough
                if self.available < transaction.decimal.unwrap() {
                    return Err(CustomError::AccountBalanceNotEnough);
                }
                self.available -= transaction.decimal.unwrap();
                self.total -= transaction.decimal.unwrap();
                self.transactions
                    .insert(transaction.transaction_id, transaction);
            }
            Action::Dispute => {
                let original_transaction = self.transactions.get_mut(&transaction.transaction_id);
                match original_transaction {
                    None => {
                        //this dispute is erroneous
                        println!(
                            "Err: erroneous dispute with transaction_id: {}",
                            transaction.transaction_id
                        );
                    }
                    Some(original_transaction) => {
                        //check if original_transaction is type deposit, if not, print error
                        match original_transaction.action_type {
                            Action::Deposit => {
                                //if deposit,
                                self.available -= original_transaction.decimal.unwrap();
                                self.held += original_transaction.decimal.unwrap();
                            }
                            _ => {
                                println!(
                                    "Err: erroneous dispute with transaction_id: {}, original transaction with id: {}, is type: {:?}",
                                    transaction.transaction_id, original_transaction.transaction_id,  original_transaction.action_type
                                );
                            }
                        }
                        original_transaction.is_under_dispute = true;
                    }
                }
            }

            Action::Resolve => {
                let original_transaction = self.transactions.get_mut(&transaction.transaction_id);
                match original_transaction {
                    None => {
                        //this dispute is erroneous
                        println!(
                            "Err: erroneous dispute with transaction_id: {}",
                            transaction.transaction_id
                        );
                    }
                    Some(original_transaction) => {
                        //check if original_transaction is type deposit, if not, print error
                        match original_transaction.action_type {
                            Action::Deposit => {
                                //if deposit,
                                if original_transaction.is_under_dispute {
                                    self.available += original_transaction.decimal.unwrap();
                                    self.held -= original_transaction.decimal.unwrap();
                                    original_transaction.is_under_dispute = false;
                                //no longer under dispute
                                } else {
                                    //not under dispute, print log
                                    println!(
                                    "Err: erroneous dispute with transaction_id: {}, original transaction with id: {}, is type: {:?}",
                                    transaction.transaction_id, original_transaction.transaction_id,  original_transaction.action_type
                                );
                                }
                            }
                            _ => {
                                println!(
                                    "Err: erroneous dispute with transaction_id: {}, original transaction with id: {}, is type: {:?}",
                                    transaction.transaction_id, original_transaction.transaction_id,  original_transaction.action_type
                                );
                            }
                        }
                    }
                }
            }
            Action::Chargeback => {
                let original_transaction = self.transactions.get_mut(&transaction.transaction_id);
                match original_transaction {
                    None => {
                        //this dispute is erroneous
                        println!(
                            "Err: erroneous dispute with transaction_id: {}",
                            transaction.transaction_id
                        );
                    }
                    Some(original_transaction) => {
                        //check if original_transaction is type deposit, if not, print error
                        match original_transaction.action_type {
                            Action::Deposit => {
                                //if deposit,
                                if original_transaction.is_under_dispute {
                                    self.held -= original_transaction.decimal.unwrap();
                                    self.total -= original_transaction.decimal.unwrap();
                                    original_transaction.is_under_dispute = false;
                                    self.is_locked = true;
                                //no longer under dispute
                                } else {
                                    //not under dispute, print log
                                    println!(
                                    "Err: erroneous dispute with transaction_id: {}, original transaction with id: {}, is type: {:?}",
                                    transaction.transaction_id, original_transaction.transaction_id,  original_transaction.action_type
                                );
                                }
                            }
                            _ => {
                                println!(
                                    "Err: erroneous dispute with transaction_id: {}, original transaction with id: {}, is type: {:?}",
                                    transaction.transaction_id, original_transaction.transaction_id,  original_transaction.action_type
                                );
                            }
                        }
                    }
                }
            }
        }
        //sanity check
        assert_eq!(self.total, self.available + self.held);
        Ok(())
    }
}
