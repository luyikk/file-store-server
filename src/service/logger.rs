use anyhow::Context;
use flexi_logger::writers::LogWriter;
use flexi_logger::{style, DeferredNow, TS_DASHES_BLANK_COLONS_DOT_BLANK};
use log::{LevelFilter, Record};
use std::io::Write;
use std::path::Path;

pub static LOGGER_HANDLER: tokio::sync::OnceCell<flexi_logger::LoggerHandle> =
    tokio::sync::OnceCell::const_new();

pub fn install_logger() -> anyhow::Result<()> {
    use flexi_logger::{Age, Cleanup, Criterion, FileSpec, Logger, Naming, WriteMode};

    let mut current_exec_path = super::io::get_current_exec_path()?;
    current_exec_path.push("logs");

    let logger = Logger::try_with_str("trace, mio=error")?
        .log_to_file_and_writer(
            FileSpec::default()
                .directory(current_exec_path)
                .suppress_timestamp()
                .suffix("log"),
            Box::new(StdErrLog::new()),
        )
        .format(flexi_logger::opt_format)
        .rotate(
            Criterion::AgeOrSize(Age::Day, 1024 * 1024 * 5),
            Naming::Numbers,
            Cleanup::KeepLogFiles(200),
        )
        .print_message()
        .set_palette("196;190;2;4;8".into())
        .write_mode(WriteMode::Async)
        .start()?;

    LOGGER_HANDLER
        .set(logger)
        .map_err(|_| anyhow::anyhow!("logger set error"))?;

    Ok(())
}

pub struct StdErrLog;

impl StdErrLog {
    #[allow(dead_code)]
    pub fn new() -> StdErrLog {
        StdErrLog
    }
}
fn get_file_name(path: Option<&str>) -> anyhow::Result<&str> {
    match path {
        Some(v) => Ok(Path::new(v)
            .file_name()
            .context("<unnamed>")?
            .to_str()
            .context("<unnamed>")?),
        None => Ok("<unnamed>"),
    }
}

impl LogWriter for StdErrLog {
    #[inline]
    fn write(&self, now: &mut DeferredNow, record: &Record) -> std::io::Result<()> {
        let level = record.level();
        write!(
            std::io::stderr(),
            "[{} {} {}:{}] {}\r\n",
            now.format(TS_DASHES_BLANK_COLONS_DOT_BLANK),
            style(level).paint(level.to_string()),
            get_file_name(record.file()).unwrap_or("<unnamed>"),
            record.line().unwrap_or(0),
            record.args()
        )
    }

    fn flush(&self) -> std::io::Result<()> {
        std::io::stderr().flush()
    }

    fn max_log_level(&self) -> LevelFilter {
        LevelFilter::Error
    }
}
