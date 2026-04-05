#[rustvello::task(max_retries = 3, max_retries = 5)]
fn my_task(x: i32) -> i32 {
    x + 1
}

fn main() {}
