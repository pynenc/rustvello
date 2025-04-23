Some examples

```sh
cargo run -- get-finest-resolution
cargo run -- get-node-state
cargo run -- get-node-elem-ranges

cargo run -- register-element-kind --code EK0 --name "Kind 0" --description "Desc 0"
cargo run -- register-element-kind --code EK01 --name "Kind 01" --description "Desc 01" --parent-code EK0
cargo run -- register-element-kind --code EK02 --name "Kind 02" --description "Desc 02" --parent-code EK0

cargo run -- register-element --kind EK0 --name "Elem 0"

cargo run -- get-node-elem-ranges

cargo run -- register-metric --code CPU --name "CPU %" --description "CPU usage on %"
cargo run -- register-metric --code MEM --name "MEM %" --description "MEM usage on %"

cargo run -- get-metric-order

cargo run -- send-metric --element-id 1 --metric-code "CPU" --value 5.5

cargo run -- get-metrics

```
