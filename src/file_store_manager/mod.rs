mod non_hasher;
mod user_store;

pub use user_store::*;

use crate::service::io::get_path_prefix;
use anyhow::{bail, ensure, Context};
use aqueue::Actor;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use tokio::sync::OnceCell;

pub static FILE_STORE_MANAGER: OnceCell<Actor<FileStoreManager>> = OnceCell::const_new();

/// file store manager
pub struct FileStoreManager {
    root: PathBuf,
    // current write file
    writes: HashSet<u64>,
    // current user lock write file
    // allow only specified users to write
    // key is file key, value is user session id
    w_locks: HashMap<u64, i64>,
    // current read file table
    // modification is not allowed as long as it exists
    // key is file key, value is read user table,
    // Invalidate when table is empty
    r_locks: HashMap<u64, HashSet<i64>>,
}

impl FileStoreManager {
    pub fn new(root: PathBuf) -> anyhow::Result<Actor<FileStoreManager>> {
        let root_path = if root.is_absolute() && root.is_dir() {
            if !root.exists() {
                std::fs::create_dir_all(&root)?;
                log::trace!("create root dir:{:?}", root);
            }
            root
        } else {
            let mut current_exec_path = crate::service::io::get_current_exec_path()?;
            current_exec_path.push(root);

            if !current_exec_path.exists() {
                std::fs::create_dir_all(&current_exec_path)?;
                log::trace!("create root dir:{:?}", current_exec_path);
            }
            current_exec_path
        };

        let root_path = if let Some(prefix) = get_path_prefix(&root_path) {
            if prefix.is_verbatim() {
                if let std::path::Prefix::VerbatimDisk(u) = prefix {
                    log::trace!("path:{} is VerbatimDisk", root_path.display());
                    PathBuf::from(
                        root_path
                            .to_string_lossy()
                            .into_owned()
                            .strip_prefix(r#"\\?\"#)
                            .with_context(|| format!(r#"VerbatimDisk not is \\?\{}"#, u as char))?,
                    )
                } else {
                    bail!("error prefix:{:?}", prefix);
                }
            } else {
                root_path
            }
        } else {
            root_path
        };

        Ok(Actor::new(FileStoreManager {
            root: root_path,
            writes: Default::default(),
            w_locks: Default::default(),
            r_locks: Default::default(),
        }))
    }

    /// create new store
    fn new_user_store(&self) -> Actor<UserStore> {
        UserStore::new(self.root.clone())
    }

    /// create file write key
    fn create_write_key(&mut self, filename: &Path, session_id: i64) -> anyhow::Result<u64> {
        let key = {
            let mut hasher = DefaultHasher::new();
            filename.hash(&mut hasher);
            hasher.finish()
        };
        ensure!(
            !self.writes.contains(&key),
            "file is being uploaded:{:?}",
            filename.file_name()
        );

        ensure!(
            !self.r_locks.contains_key(&key),
            "file is being read:{:?}",
            filename.file_name()
        );

        if let Some(id) = self.w_locks.get(&key) {
            ensure!(*id == session_id, "file is being lock:{:?}", filename);
        }

        self.writes.insert(key);
        Ok(key)
    }

    /// finish write key
    fn finish_write_key(&mut self, key: u64) {
        self.writes.remove(&key);
        self.w_locks.remove(&key);
    }

    /// create read key
    fn create_read_key(&mut self, filename: &Path, session_id: i64) -> anyhow::Result<u64> {
        let key = {
            let mut hasher = DefaultHasher::new();
            filename.hash(&mut hasher);
            hasher.finish()
        };

        self.r_locks
            .entry(key)
            .or_default()
            .insert(session_id);

        Ok(key)
    }

    fn finish_read_key(&mut self, key: u64, session_id: i64) {
        if let Some(table) = self.r_locks.get_mut(&key) {
            table.remove(&session_id);
            if table.is_empty() {
                self.r_locks.remove(&key);
            }
        }
    }

    /// lock file
    fn lock(&mut self, paths: &[PathBuf], session_id: i64) -> anyhow::Result<()> {
        let mut keys = Vec::with_capacity(paths.len());
        for path in paths {
            let key = {
                let mut hasher = DefaultHasher::new();
                path.hash(&mut hasher);
                hasher.finish()
            };

            ensure!(
                !self.writes.contains(&key),
                "file is being uploaded:{:?}",
                path.file_name()
            );

            ensure!(
                !self.r_locks.contains_key(&key),
                "file is being read:{:?}",
                path.file_name()
            );

            if let Some(id) = self.w_locks.get(&key) {
                if *id != session_id {
                    bail!("file is being uploaded:{:?}", path)
                }
            }

            keys.push(key);
        }

        for key in keys {
            self.w_locks.insert(key, session_id);
        }

        Ok(())
    }

    /// clear user lock file
    fn clear_lock(&mut self, session_id: i64) {
        self.w_locks.retain(|_, v| *v != session_id);
        self.r_locks.iter_mut().for_each(|(_, v)| {
            v.remove(&session_id);
        });
        self.r_locks.retain(|_, v| !v.is_empty());
    }
}


pub(crate) trait IFileStoreManager {
    /// create new store
    fn new_user_store(&self) -> Actor<UserStore>;
    /// create file write key
    async fn create_write_key(&self, filename: &Path, session_id: i64) -> anyhow::Result<u64>;
    /// finish write key
    async fn finish_write_key(&self, key: u64);
    /// create read key
    async fn create_read_key(&self, filename: &Path, session_id: i64) -> anyhow::Result<u64>;
    /// finish write key
    async fn finish_read_key(&self, key: u64, session_id: i64);
    /// has file name
    async fn lock_write(&self, paths: &[PathBuf], session_id: i64) -> anyhow::Result<()>;
    /// check file is lock
    async fn is_lock_write(&self, path: &Path) -> bool;
    /// check file is lock
    async fn is_lock_read(&self, path: &Path) -> bool;
    /// clear user lock files
    async fn clear_lock(&self, session_id: i64);
}


impl IFileStoreManager for Actor<FileStoreManager> {
    fn new_user_store(&self) -> Actor<UserStore> {
        unsafe { self.deref_inner().new_user_store() }
    }

    async fn create_write_key(&self, filename: &Path, session_id: i64) -> anyhow::Result<u64> {
        self.inner_call(
            |inner| async move { inner.get_mut().create_write_key(filename, session_id) },
        )
        .await
    }

    async fn finish_write_key(&self, key: u64) {
        self.inner_call(|inner| async move { inner.get_mut().finish_write_key(key) })
            .await
    }

    async fn create_read_key(&self, filename: &Path, session_id: i64) -> anyhow::Result<u64> {
        self.inner_call(
            |inner| async move { inner.get_mut().create_read_key(filename, session_id) },
        )
        .await
    }

    async fn finish_read_key(&self, key: u64, session_id: i64) {
        self.inner_call(|inner| async move { inner.get_mut().finish_read_key(key, session_id) })
            .await
    }

    async fn lock_write(&self, paths: &[PathBuf], session_id: i64) -> anyhow::Result<()> {
        self.inner_call(|inner| async move { inner.get_mut().lock(paths, session_id) })
            .await
    }

    async fn is_lock_write(&self, filename: &Path) -> bool {
        let key = {
            let mut hasher = DefaultHasher::new();
            filename.hash(&mut hasher);
            hasher.finish()
        };

        self.inner_call(|inner| async move {
            inner.get().w_locks.contains_key(&key) || inner.get().writes.contains(&key)
        })
        .await
    }

    async fn is_lock_read(&self, path: &Path) -> bool {
        let key = {
            let mut hasher = DefaultHasher::new();
            path.hash(&mut hasher);
            hasher.finish()
        };

        self.inner_call(|inner| async move { inner.get().r_locks.contains_key(&key) })
            .await
    }

    async fn clear_lock(&self, session_id: i64) {
        self.inner_call(|inner| async move { inner.get_mut().clear_lock(session_id) })
            .await
    }
}
