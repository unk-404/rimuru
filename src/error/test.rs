use super::*;
#[test]
fn io_conversion_preserves_source_and_kind() {
    use std::error::Error as _;
    let error: Error = io::Error::new(io::ErrorKind::PermissionDenied, "denied").into();
    assert!(error.source().is_some());
    assert!(error.to_string().contains("denied"));
    assert!(matches!(error,Error::Io(ref e) if e.kind()==io::ErrorKind::PermissionDenied));
}
