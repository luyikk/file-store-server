use netxserver::prelude::*;

#[build]
pub trait IClientController {
    /// write buff to file by key
    #[tag(2001)]
    async fn write_file_by_key(&self, key: u64, offset: u64, data: &[u8]);
}
