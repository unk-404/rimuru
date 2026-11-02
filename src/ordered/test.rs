#[test]
fn public_handles_are_send_and_sync() {
    fn check<T: Send + Sync>() {}
    check::<super::Channel>();
    check::<crate::SendTicket>();
    check::<crate::Endpoint>();
}
