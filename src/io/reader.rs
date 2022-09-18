use csv_async::AsyncReader;
use std::path::PathBuf;
use tokio::fs::File;

use crate::error::CustomError;
pub(crate) struct Reader {
    inner: AsyncReader<File>,
}

impl Reader {
    pub(crate) async fn new(file_path: PathBuf) -> Result<Reader, CustomError> {
        let file = File::open(file_path).await?;
        let reader = csv_async::AsyncReaderBuilder::new()
            .trim(csv_async::Trim::All)
            .create_reader(file);
        Ok(Self { inner: reader })
    }

    pub(crate) fn get_inner(&mut self) -> &mut AsyncReader<File> {
        &mut self.inner
    }
}
