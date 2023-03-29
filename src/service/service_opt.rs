use crate::SERVICE_LIABLE;
use clap::{Parser, Subcommand};
use service_manager::*;
use std::ffi::OsString;
use std::path::PathBuf;

pub fn service() -> anyhow::Result<Option<PathBuf>> {
    let current_exe = match std::env::current_exe() {
        Ok(path) => path,
        Err(err) => panic!("current_exe_path get error:{err:?}"),
    };

    match OptArgs::parse() {
        OptArgs::Exec { config } => Ok(Some(config)),
        OptArgs::Create { path } => {
            let config = include_str!("../../config.toml");
            if path.has_root() {
                std::fs::write(path, config)?;
            } else {
                let mut path_t = super::io::get_current_exec_path()?;
                path_t.push(path);
                std::fs::write(path_t, config)?;
            }
            Ok(None)
        }
        OptArgs::Service(ServiceArgs::Install { config }) => {
            let label: ServiceLabel = SERVICE_LIABLE.parse().unwrap();
            let manager = <dyn ServiceManager>::native()?;
            manager
                .install(ServiceInstallCtx {
                    label,
                    program: current_exe,
                    args: vec![OsString::from("exec"), OsString::from(config)],
                })
                .expect("Failed to install");
            println!("service install success");
            Ok(None)
        }
        OptArgs::Service(ServiceArgs::Uninstall) => {
            let label: ServiceLabel = SERVICE_LIABLE.parse().unwrap();
            let manager = <dyn ServiceManager>::native()?;
            manager
                .uninstall(ServiceUninstallCtx { label })
                .expect("Failed to uninstall");
            println!("service uninstall success");
            Ok(None)
        }
        OptArgs::Service(ServiceArgs::Start) => {
            let label: ServiceLabel = SERVICE_LIABLE.parse().unwrap();
            let manager = <dyn ServiceManager>::native()?;
            manager
                .start(ServiceStartCtx { label })
                .expect("Failed to start");
            println!("service start success");
            Ok(None)
        }
        OptArgs::Service(ServiceArgs::Stop) => {
            let label: ServiceLabel = SERVICE_LIABLE.parse().unwrap();
            let manager = <dyn ServiceManager>::native()?;
            manager
                .stop(ServiceStopCtx { label })
                .expect("Failed to stop");
            println!("service stop success");
            Ok(None)
        }
        OptArgs::Service(ServiceArgs::Restart) => {
            let label: ServiceLabel = SERVICE_LIABLE.parse().unwrap();
            let manager = <dyn ServiceManager>::native()?;

            manager
                .stop(ServiceStopCtx {
                    label: label.clone(),
                })
                .expect("Failed to stop");

            manager
                .start(ServiceStartCtx { label })
                .expect("Failed to start");

            println!("service restart success");
            Ok(None)
        }
    }
}

#[derive(Parser)]
#[clap(name = SERVICE_LIABLE,version)]
enum OptArgs {
    /// run service
    Exec {
        /// config path;(by default, read from the current exec path config.toml)
        #[arg(value_parser, default_value = "config")]
        config: PathBuf,
    },
    /// create config file; by default, create default config.toml to current exec path
    Create {
        /// create config file path
        #[arg(value_parser, default_value = "config")]
        path: PathBuf,
    },
    /// service manager
    #[command(subcommand)]
    Service(ServiceArgs),
}

#[derive(Subcommand)]
enum ServiceArgs {
    /// install service to system
    Install {
        #[arg(value_parser, default_value = "config")]
        config: String,
    },
    /// start service
    Start,
    /// stop service
    Stop,
    /// restart service
    Restart,
    /// uninstall service to system
    Uninstall,
}
