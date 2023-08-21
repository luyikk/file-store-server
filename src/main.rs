mod controller;
pub mod file_store_manager;
mod service;

use crate::controller::ImplCreateController;
use crate::file_store_manager::{FileStoreManager, FILE_STORE_MANAGER};
use anyhow::{anyhow, ensure};
use netxserver::prelude::NetXServer;
use rustls_pemfile::{certs, rsa_private_keys};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use tokio_rustls::rustls::server::AllowAnyAuthenticatedClient;
use tokio_rustls::rustls::{Certificate, PrivateKey, RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

#[cfg(unix)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Some(config) = service::service_opt::service()? {
        service::logger::install_logger()?;
        log::info!("start unix run");
        start(config).await?;
    }
    Ok(())
}

#[cfg(windows)]
fn main() -> anyhow::Result<()> {
    if let Some(config_file) = service::service_opt::service()? {
        service::logger::install_logger()?;
        log::info!("start windows run");
        service::windows_service::run(config_file)?;
    }
    Ok(())
}

#[inline]
async fn start(config_file: PathBuf) -> anyhow::Result<()> {
    let config = service::config::Config::try_from(config_file)?;
    log::trace!("load config info:{:#?}", config);
    FILE_STORE_MANAGER
        .set(FileStoreManager::new(config.root)?)
        .map_err(|err| anyhow!("init file store manager error:{}", err))?;

    if let Some(tls) = config.tls {
        let cert_path = if tls.cert.exists() && tls.cert.is_absolute() {
            tls.cert
        } else {
            let mut current_exec_path = service::io::get_current_exec_path()?;
            current_exec_path.push(&tls.cert);
            ensure!(
                current_exec_path.exists(),
                "not found file:{:?}",
                current_exec_path
            );
            current_exec_path
        };

        let key_path = if tls.key.exists() && tls.key.is_absolute() {
            tls.key
        } else {
            let mut current_exec_path = service::io::get_current_exec_path()?;
            current_exec_path.push(&tls.key);
            ensure!(
                current_exec_path.exists(),
                "not found file:{:?}",
                current_exec_path
            );
            current_exec_path
        };

        let tls_acceptor = if let Some(ca) = tls.ca {
            let ca_path = if ca.exists() && ca.is_absolute() {
                ca
            } else {
                let mut current_exec_path = service::io::get_current_exec_path()?;
                current_exec_path.push(ca);
                ensure!(
                    current_exec_path.exists(),
                    "not found file:{:?}",
                    current_exec_path
                );
                current_exec_path
            };

            let ca_file = &mut BufReader::new(File::open(ca_path)?);
            let cert_file = &mut BufReader::new(File::open(cert_path)?);
            let key_file = &mut BufReader::new(File::open(key_path)?);

            let ca_certs = certs(ca_file)?;
            let keys = PrivateKey(rsa_private_keys(key_file)?.remove(0));
            let cert_chain = certs(cert_file)?
                .iter()
                .map(|c| Certificate(c.to_vec()))
                .collect::<Vec<_>>();

            let mut client_auth_roots = RootCertStore::empty();
            client_auth_roots.add_parsable_certificates(&ca_certs);

            let client_auth = Arc::new(AllowAnyAuthenticatedClient::new(client_auth_roots));

            let tls_config = ServerConfig::builder()
                .with_safe_defaults()
                .with_client_cert_verifier(client_auth)
                .with_single_cert(cert_chain, keys)?;

            TlsAcceptor::from(Arc::new(tls_config))
        } else {
            let cert_file = &mut BufReader::new(File::open(cert_path)?);
            let key_file = &mut BufReader::new(File::open(key_path)?);
            let keys = PrivateKey(rsa_private_keys(key_file)?.remove(0));
            let cert_chain = certs(cert_file)?
                .iter()
                .map(|c| Certificate(c.to_vec()))
                .collect::<Vec<_>>();

            let tls_config = ServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth()
                .with_single_cert(cert_chain, keys)?;

            TlsAcceptor::from(Arc::new(tls_config))
        };

        let tls_acceptor = Box::leak(Box::new(tls_acceptor));
        let server = NetXServer::new_tls(tls_acceptor, config.service, ImplCreateController).await;
        log::info!("start service");
        server.start_block().await?;
    } else {
        let server = NetXServer::new(config.service, ImplCreateController).await;
        log::info!("start service");
        server.start_block().await?;
    }

    Ok(())
}
