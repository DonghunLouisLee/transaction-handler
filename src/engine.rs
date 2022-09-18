use std::{collections::HashMap, str::FromStr};

use crate::{
    error::CustomError,
    io::{reader::Reader, writer::Writer},
};
use anyhow::Result;
use csv_async::StringRecord;
use futures::stream::StreamExt;
use log::warn;
use rust_decimal::Decimal;
use tokio::io::AsyncWriteExt;

const PRECISION: u32 = 4;

type ClientId = u16;
type TransactionId = u32;

/// This is the transaction engine
pub(crate) struct Engine {
    clients: HashMap<ClientId, Account>,
}
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
            let transaction = Transaction::from_record(value.unwrap()).unwrap();
            let client_id = transaction.get_client_id();
            let transaction_id = transaction.transaction_id;
            let account = self.clients.entry(client_id);
            match account {
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    let mut new_account = Account::new(client_id);
                    //todo handle error here, should we stop or now depending on the error types
                    if let Err(err) = new_account.handle_transaction(transaction) {
                        match err {
                            CustomError::UndefinedAction
                            | CustomError::DecimalParseError(_)
                            | CustomError::IntParseError(_)
                            | CustomError::FileOpenError(_) => return Err(err),
                            CustomError::AccountBalanceNotEnough
                            | CustomError::LockedAccount
                            | CustomError::UndefinedBehaviour
                            | CustomError::NonExistingTransactionId
                            | CustomError::DuplicatedTransactionId
                            | CustomError::NotUnderDispute => {
                                //simply log error and continue
                                warn!("Client id: {}, with transaction_id: {} had following error: {}", client_id, transaction_id, err);
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
                            | CustomError::IntParseError(_)
                            | CustomError::FileOpenError(_) => return Err(err),
                            CustomError::AccountBalanceNotEnough
                            | CustomError::LockedAccount
                            | CustomError::UndefinedBehaviour
                            | CustomError::NonExistingTransactionId
                            | CustomError::DuplicatedTransactionId
                            | CustomError::NotUnderDispute => {
                                //simply log error and continue
                                warn!("Client id: {}, with transaction_id: {} had following error: {}", client_id, transaction_id, err);
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
                let decimal = Decimal::from_str(record.get(3).unwrap())?;
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

    /// Only for testing and debugging purpose
    fn _new(
        action_type: Action,
        client_id: ClientId,
        transaction_id: TransactionId,
        decimal: Option<Decimal>,
        is_under_dispute: bool,
    ) -> Self {
        Self {
            action_type,
            client_id,
            transaction_id,
            decimal,
            is_under_dispute,
        }
    }
}

/// Account represents status of individual client
#[derive(Debug)]
struct Account {
    _client_id: ClientId,
    /// Transactions only keep following actions: Deposit, Withdrawal, Dispute
    /// Since Resolve and Chargeback cannot be overturned
    /// Transaction number is unique
    transactions: HashMap<TransactionId, Transaction>,
    /// is_locked is set to true only if chargeback takes place
    is_locked: bool,
    /// The total funds that are available for trading, staking, withdrawal, etc. This should be equal to the (total - held)
    available: Decimal,
    /// The total funds that are held for dispute. This should be equal to (total - available)
    held: Decimal,
    /// The total funds that are available or held. This should be equal to (available - held)
    total: Decimal,
}

impl Account {
    /// Intitializes a new account
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

    /// Takes transaction as input and will update it's status
    /// This method will return Err if and only if itself is locked or account balance is not enough
    /// For other unwanted situations such as transaction_id for dispute is missing,
    /// it will continue while logging the incident
    fn handle_transaction(&mut self, transaction: Transaction) -> Result<(), CustomError> {
        //first check if this account is not locked,
        //if locked, return error
        if self.is_locked {
            return Err(CustomError::LockedAccount);
        }
        match transaction.get_action_type() {
            Action::Deposit => {
                //check if transaction number is unique,
                if self.transactions.contains_key(&transaction.transaction_id) {
                    return Err(CustomError::DuplicatedTransactionId);
                }
                self.available += transaction.decimal.unwrap();
                self.total += transaction.decimal.unwrap();
                self.transactions
                    .insert(transaction.transaction_id, transaction);
            }
            Action::Withdrawal => {
                //check if transaction number is unique,
                if self.transactions.contains_key(&transaction.transaction_id) {
                    return Err(CustomError::DuplicatedTransactionId);
                }
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
                        return Err(CustomError::NonExistingTransactionId);
                    }
                    Some(original_transaction) => {
                        //check if original_transaction is type deposit, if not disregard and return error
                        match original_transaction.action_type {
                            Action::Deposit => {
                                //if deposit,
                                self.available -= original_transaction.decimal.unwrap();
                                self.held += original_transaction.decimal.unwrap();
                                original_transaction.is_under_dispute = true;
                            }
                            _ => {
                                return Err(CustomError::UndefinedBehaviour);
                            }
                        }
                    }
                }
            }

            Action::Resolve => {
                let original_transaction = self.transactions.get_mut(&transaction.transaction_id);
                match original_transaction {
                    None => {
                        //this dispute is erroneous
                        return Err(CustomError::NonExistingTransactionId);
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
                                    //not under dispute, return err
                                    return Err(CustomError::NotUnderDispute);
                                }
                            }
                            _ => {
                                return Err(CustomError::UndefinedBehaviour);
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
                        return Err(CustomError::NonExistingTransactionId);
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
                                    //not under dispute, print o
                                    return Err(CustomError::NotUnderDispute);
                                }
                            }
                            _ => {
                                return Err(CustomError::UndefinedBehaviour);
                            }
                        }
                    }
                }
            }
        }
        //sanity check
        // println!(
        //     "client_id: {}, total: {}, available: {}, held: {}",
        //     self._client_id, self.total, self.available, self.held
        // );
        assert_eq!(self.total, self.available + self.held);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test case for only deposit
    #[test]
    fn test_one_deposit() {
        let client_id = 1;
        let mut account = Account::new(client_id);
        let transaction = Transaction::_new(
            Action::Deposit,
            client_id,
            1,
            Some(Decimal::new(1, PRECISION)),
            false,
        );
        account.handle_transaction(transaction).unwrap();

        assert_eq!(account.total, Decimal::new(1, PRECISION));
        assert_eq!(account.available, Decimal::new(1, PRECISION));
        assert_eq!(account.held, Decimal::new(0, PRECISION));
    }

    /// Test case for duplicated deposit
    #[test]
    fn test_duplicated_deposit() {
        let client_id = 1;
        let mut account = Account::new(client_id);
        let transaction1 = Transaction::_new(
            Action::Deposit,
            client_id,
            1,
            Some(Decimal::new(1, PRECISION)),
            false,
        );
        let transaction2 = Transaction::_new(
            Action::Deposit,
            client_id,
            1,
            Some(Decimal::new(2, PRECISION)),
            false,
        );
        account.handle_transaction(transaction1).unwrap();
        if let Ok(()) = account.handle_transaction(transaction2) {
            //this should fail
            panic!()
        }
    }

    /// Test case for multiple deposit
    #[test]
    fn test_multiple_deposit() {
        let client_id = 1;
        let mut account = Account::new(client_id);
        for i in 1..11 {
            let transaction = Transaction::_new(
                Action::Deposit,
                client_id,
                i,
                Some(Decimal::new(i.into(), PRECISION)),
                false,
            );
            account.handle_transaction(transaction).unwrap();
        }

        assert_eq!(account.total, Decimal::new(55, PRECISION));
        assert_eq!(account.available, Decimal::new(55, PRECISION));
        assert_eq!(account.held, Decimal::new(0, PRECISION));
    }

    #[test]
    fn test_simple_withdrawal() {
        let client_id = 1;
        let mut account = Account::new(client_id);
        let deposit = Transaction::_new(
            Action::Deposit,
            client_id,
            1,
            Some(Decimal::new(1, PRECISION)),
            false,
        );
        account.handle_transaction(deposit).unwrap();

        let withdrawal = Transaction::_new(
            Action::Withdrawal,
            client_id,
            2,
            Some(Decimal::new(1, PRECISION)),
            false,
        );

        account.handle_transaction(withdrawal).unwrap();

        assert_eq!(account.total, Decimal::new(0, PRECISION));
        assert_eq!(account.available, Decimal::new(0, PRECISION));
        assert_eq!(account.held, Decimal::new(0, PRECISION));
    }

    #[test]
    fn test_faulty_withdrawal() {
        let client_id = 1;
        let mut account = Account::new(client_id);
        let deposit = Transaction::_new(
            Action::Deposit,
            client_id,
            1,
            Some(Decimal::new(1, PRECISION)),
            false,
        );
        account.handle_transaction(deposit).unwrap();

        let withdrawal = Transaction::_new(
            Action::Withdrawal,
            client_id,
            2,
            Some(Decimal::new(2, PRECISION)),
            false,
        );

        if let Ok(()) = account.handle_transaction(withdrawal) {
            //value should not change
            assert_eq!(account.total, Decimal::new(1, PRECISION));
            assert_eq!(account.available, Decimal::new(1, PRECISION));
            assert_eq!(account.held, Decimal::new(0, PRECISION));
            panic!()
        }
    }

    #[test]
    fn test_dispute() {
        let client_id = 1;
        let mut account = Account::new(client_id);
        let deposit = Transaction::_new(
            Action::Deposit,
            client_id,
            1,
            Some(Decimal::new(1, PRECISION)),
            false,
        );
        account.handle_transaction(deposit).unwrap();

        let dispute = Transaction::_new(Action::Dispute, client_id, 1, None, false);

        account.handle_transaction(dispute).unwrap();
        assert_eq!(account.total, Decimal::new(1, PRECISION));
        assert_eq!(account.available, Decimal::new(0, PRECISION));
        assert_eq!(account.held, Decimal::new(1, PRECISION));
        assert_eq!(account.transactions.get(&1).unwrap().is_under_dispute, true)
    }

    #[test]
    fn test_faulty_dispute() {
        let client_id = 1;
        let mut account = Account::new(client_id);
        let deposit = Transaction::_new(
            Action::Deposit,
            client_id,
            1,
            Some(Decimal::new(1, PRECISION)),
            false,
        );
        account.handle_transaction(deposit).unwrap();

        let dispute = Transaction::_new(Action::Dispute, client_id, 2, None, false);

        if let Ok(()) = account.handle_transaction(dispute) {
            panic!()
        }
        assert_eq!(account.total, Decimal::new(1, PRECISION));
        assert_eq!(account.available, Decimal::new(1, PRECISION));
        assert_eq!(account.held, Decimal::new(0, PRECISION));
    }

    #[test]
    fn test_resolve() {
        let client_id = 1;
        let mut account = Account::new(client_id);
        let deposit = Transaction::_new(
            Action::Deposit,
            client_id,
            1,
            Some(Decimal::new(1, PRECISION)),
            false,
        );
        account.handle_transaction(deposit).unwrap();

        let dispute = Transaction::_new(Action::Dispute, client_id, 1, None, false);

        account.handle_transaction(dispute).unwrap();

        let resolve = Transaction::_new(Action::Resolve, client_id, 1, None, false);

        account.handle_transaction(resolve).unwrap();

        assert_eq!(account.total, Decimal::new(1, PRECISION));
        assert_eq!(account.available, Decimal::new(1, PRECISION));
        assert_eq!(account.held, Decimal::new(0, PRECISION));
        assert_eq!(
            account.transactions.get(&1).unwrap().is_under_dispute,
            false
        )
    }

    #[test]
    fn test_chargeback() {
        let client_id = 1;
        let mut account = Account::new(client_id);
        let deposit = Transaction::_new(
            Action::Deposit,
            client_id,
            1,
            Some(Decimal::new(1, PRECISION)),
            false,
        );
        account.handle_transaction(deposit).unwrap();

        let dispute = Transaction::_new(Action::Dispute, client_id, 1, None, false);

        account.handle_transaction(dispute).unwrap();

        let chargeback = Transaction::_new(Action::Chargeback, client_id, 1, None, false);

        account.handle_transaction(chargeback).unwrap();

        assert_eq!(account.total, Decimal::new(0, PRECISION));
        assert_eq!(account.available, Decimal::new(0, PRECISION));
        assert_eq!(account.held, Decimal::new(0, PRECISION));
        assert_eq!(
            account.transactions.get(&1).unwrap().is_under_dispute,
            false
        );
        assert_eq!(account.is_locked, true);
    }

    #[test]
    fn test_locked_account() {
        let client_id = 1;
        let mut account = Account::new(client_id);
        let deposit = Transaction::_new(
            Action::Deposit,
            client_id,
            1,
            Some(Decimal::new(1, PRECISION)),
            false,
        );
        account.handle_transaction(deposit).unwrap();

        let dispute = Transaction::_new(Action::Dispute, client_id, 1, None, false);

        account.handle_transaction(dispute).unwrap();

        let chargeback = Transaction::_new(Action::Chargeback, client_id, 1, None, false);

        account.handle_transaction(chargeback).unwrap();

        let deposit2 = Transaction::_new(
            Action::Deposit,
            client_id,
            2,
            Some(Decimal::new(1, PRECISION)),
            false,
        );
        if let Ok(()) = account.handle_transaction(deposit2) {
            panic!()
        }
        assert_eq!(account.total, Decimal::new(0, PRECISION));
        assert_eq!(account.available, Decimal::new(0, PRECISION));
        assert_eq!(account.held, Decimal::new(0, PRECISION));
        assert_eq!(
            account.transactions.get(&1).unwrap().is_under_dispute,
            false
        );
        assert_eq!(account.is_locked, true);
    }
}
