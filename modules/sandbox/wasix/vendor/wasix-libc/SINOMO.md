# Sinomo WASIX libc

This directory vendors `wasix-org/wasix-libc` at tag `v2026-05-12.1` and carries Sinomo's WASIX/POSIX libc surface directly in the libc source tree.

Sinomo changes belong here instead of package-local sysroot patch functions. Toolchain recipes should build this libc and consume the generated sysroot.

The sysroot recipe is
`crates/sinomo-sandbox/toolchains/build-tools/prepare-wasix-sysroot.sh`; its
`SINOMO_WASIX_LIBC_SOURCE` input selects this directory.
