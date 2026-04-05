#[rustvello::task(concurrency = "invalid_value")]
fn my_task(x: i32) -> i32 {
    x + 1
}

fn main() {}
