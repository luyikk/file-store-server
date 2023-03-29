use netxserver::prelude::{tcpserver::IPeer, *};
use std::sync::Arc;

#[build(FileStoreService)]
pub trait IFileStoreService {
    #[tag(connect)]
    async fn connect(&self) -> anyhow::Result<()>;
}

pub struct FileStoreService {
    token: NetxToken,
}

#[build_impl]
impl IFileStoreService for FileStoreService {
    #[inline]
    async fn connect(&self) -> anyhow::Result<()> {
        if let Some(weak) = self.token.get_peer().await? {
            if let Some(peer) = weak.upgrade() {
                log::info!(
                    "addr:{} session {} connect",
                    peer.addr(),
                    self.token.get_session_id()
                )
            }
        }
        Ok(())
    }
}

pub struct ImplCreateController;
impl ICreateController for ImplCreateController {
    #[inline]
    fn create_controller(&self, token: NetxToken) -> anyhow::Result<Arc<dyn IController>> {
        Ok(Arc::new(FileStoreService { token }))
    }
}
