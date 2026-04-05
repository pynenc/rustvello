#[rustvello::task]
unsafe fn my_unsafe_task(x: i32) -> i32 {
    x + 1
}

fn main() {}
