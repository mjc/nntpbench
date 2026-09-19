#!/usr/bin/env bash
# Run through nix develop. No dependency rebuild with changing RUSTFLAGS:
# only the crate under test receives these compile-contract cfgs.
set -euo pipefail
cd "$(dirname "$0")/.."
log_dir=target/response-contracts
mkdir -p "$log_dir"
cargo rustc --lib -- --cfg response_contract --emit=metadata >"$log_dir/positive.log" 2>&1
echo "PASS: positive controls"
if cargo rustc --lib -- --cfg response_contract --cfg 'response_contract="coordinate"' --emit=metadata >"$log_dir/coordinate.log" 2>&1; then
    echo "FAIL: coordinate unexpectedly compiled"
    exit 1
fi
test "$(rg -o 'error\[E[0-9]+\]' "$log_dir/coordinate.log" | sort -u)" = 'error[E0308]'
rg -q 'expected.*ChunkConsumed' "$log_dir/coordinate.log"
echo "PASS: coordinate rejected with E0308"
if cargo rustc --lib -- --cfg response_contract --cfg 'response_contract="receive_alias"' --emit=metadata >"$log_dir/receive_alias.log" 2>&1; then
    echo "FAIL: receive_alias unexpectedly compiled"
    exit 1
fi
test "$(rg -o 'error\[E[0-9]+\]' "$log_dir/receive_alias.log" | sort -u)" = 'error[E0499]'
rg -q 'receiver' "$log_dir/receive_alias.log"
echo "PASS: receive_alias rejected with E0499"
if cargo rustc --lib -- --cfg response_contract --cfg 'response_contract="validated_mutation"' --emit=metadata >"$log_dir/validated_mutation.log" 2>&1; then
    echo "FAIL: validated_mutation unexpectedly compiled"
    exit 1
fi
test "$(rg -o 'error\[E[0-9]+\]' "$log_dir/validated_mutation.log" | sort -u)" = 'error[E0502]'
rg -q 'bytes' "$log_dir/validated_mutation.log"
echo "PASS: validated_mutation rejected with E0502"
if cargo rustc --lib -- --cfg response_contract --cfg 'response_contract="validated_rebind"' --emit=metadata >"$log_dir/validated_rebind.log" 2>&1; then
    echo "FAIL: validated_rebind unexpectedly compiled"
    exit 1
fi
test "$(rg -o 'error\[E[0-9]+\]' "$log_dir/validated_rebind.log" | sort -u)" = 'error[E0451]'
rg -q 'private field' "$log_dir/validated_rebind.log"
echo "PASS: validated_rebind rejected with E0451"
