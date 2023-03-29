use std::path::PathBuf;

#[inline]
pub fn get_current_exec_path() -> std::io::Result<PathBuf> {
    Ok(match std::env::current_exe() {
        Ok(path) => {
            if let Some(current_exe_path) = path.parent() {
                current_exe_path.to_path_buf()
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "current_exe_path get error: is none",
                ));
            }
        }
        Err(err) => return Err(err),
    })
}
