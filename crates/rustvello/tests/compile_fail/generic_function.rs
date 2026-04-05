#[rustvello::task]
fn my_generic_task<T: Clone>(x: T) -> T {
    x.clone()
}

fn main() {}
