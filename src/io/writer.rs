use tokio::io::Stdout;

pub(crate) struct Writer {
    inner: Stdout,
}

impl Writer {
    pub(crate) fn new() -> Self {
        let writer = tokio::io::stdout();
        Self { inner: writer }
    }

    pub(crate) fn get_inner(&mut self) -> &mut Stdout {
        &mut self.inner
    }
}
