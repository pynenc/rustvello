// tests/it/test_element.rs
use super::common::TestContext;

#[tokio::test]
async fn test_element_registration() {
    let ctx = TestContext::new(None).await;
    let local_elem_id = ctx.register_test_element().await;
    if let Some(remote_element_id) = ctx.muse.get_remote_element_id(&local_elem_id) {
        println!(
            "Element with LocalElementId {:?} was registered with ElementId {:?}",
            local_elem_id, remote_element_id
        );
    } else {
        panic!("Remote element id not found")
    }
}
