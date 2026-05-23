# Sinomo WASIX libc

This directory vendors `wasix-org/wasix-libc` at tag `v2026-05-12.1` and carries Sinomo's WASIX/POSIX libc surface directly in the libc source tree.

Sinomo changes belong here instead of package-local sysroot patch functions. Toolchain recipes should build this libc and consume the generated sysroot.
