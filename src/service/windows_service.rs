use std::ffi::OsString;
use std::io;
use std::path::PathBuf;
use std::time::Duration;
use windows_service::{
    define_windows_service,
    service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    },
    service_control_handler::{self, ServiceControlHandlerResult},
    service_dispatcher, Result,
};

pub static CONFIG_FILE: tokio::sync::OnceCell<PathBuf> = tokio::sync::OnceCell::const_new();

const SERVICE_NAME: &str = crate::SERVICE_LIABLE;
const SERVICE_TYPE: ServiceType = ServiceType::OWN_PROCESS;

pub fn run(config_file: PathBuf) -> anyhow::Result<()> {
    CONFIG_FILE.set(config_file.clone())?;
    if let Err(err) =
        service_dispatcher::start(SERVICE_NAME, ffi_service_main).map_err(|err| match err {
            windows_service::Error::Winapi(err) => err,
            err => io::Error::new(io::ErrorKind::Other, err),
        })
    {
        if Some(1063) == err.raw_os_error() {
            start(config_file)?;
        } else {
            Err(err)?;
        }
    }
    Ok(())
}

define_windows_service!(ffi_service_main, service_main);

pub fn service_main(arguments: Vec<OsString>) {
    log::info!("service arguments:{arguments:?}");
    if let Err(err) = run_service() {
        log::error!("run service error:{err:?}");
    }
    super::logger::LOGGER_HANDLER.get().unwrap().shutdown();
}

fn run_service() -> Result<()> {
    log::info!("Starting windows service for {SERVICE_NAME}");

    // Create a channel to be able to poll a stop event from the service worker loop.
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
    // Define system service event handler that will be receiving service events.
    let event_handler = {
        move |control_event| -> ServiceControlHandlerResult {
            match control_event {
                // Notifies a service to report its current status information to the service
                // control manager. Always return NoError even if not implemented.
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,

                // Handle stop
                ServiceControl::Stop => {
                    if shutdown_tx.send(()).is_err() {
                        log::error!("shutdown_tx send error");
                    };
                    ServiceControlHandlerResult::NoError
                }

                _ => ServiceControlHandlerResult::NotImplemented,
            }
        }
    };

    log::info!("Registering service control handler for {SERVICE_NAME}");
    let status_handle = service_control_handler::register(SERVICE_NAME, event_handler)?;

    // Tell the system that service is running
    log::info!("Setting service status as running for {SERVICE_NAME}");

    status_handle.set_service_status(ServiceStatus {
        service_type: SERVICE_TYPE,
        current_state: ServiceState::Running,
        controls_accepted: ServiceControlAccept::STOP,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    })?;

    log::info!("Spawning CLI thread for {SERVICE_NAME}");

    std::thread::spawn(|| {
        if let Err(err) = start(CONFIG_FILE.get().unwrap().clone()) {
            log::error!("Spawning CLI thread error:{err:#?}")
        }
    });

    loop {
        match shutdown_rx.recv_timeout(Duration::from_millis(100)) {
            // Break the loop either upon stop or channel disconnect
            Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,

            // Continue work if no events were received within the timeout
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => (),
        };
    }

    // Tell the system that service has stopped.
    log::info!("Setting service status as stopped for {SERVICE_NAME}");
    status_handle.set_service_status(ServiceStatus {
        service_type: SERVICE_TYPE,
        current_state: ServiceState::Stopped,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    })?;
    Ok(())
}

#[inline]
fn start(config_file: PathBuf) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(crate::start(config_file))?;
    Ok(())
}
