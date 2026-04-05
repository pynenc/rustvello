#[rustvello::task]
async fn my_async_task(x: i32) -> i32 {
    x + 1
}

fn main() {}
