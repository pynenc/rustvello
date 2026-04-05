#[rustvello::task(key_arguments = ["nonexistent"])]
fn my_task(x: i32) -> i32 {
    x + 1
}

fn main() {}
