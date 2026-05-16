pub(in crate::tests) fn wasi_stdout_module() -> &'static str {
    r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 0) "\08\00\00\00\06\00\00\00hello\0a")
          (func (export "_start")
            i32.const 1
            i32.const 0
            i32.const 1
            i32.const 20
            call $fd_write
            drop))
        "#
}

pub(in crate::tests) fn wasi_environ_to_stdout_module() -> &'static str {
    r#"
        (module
          (import "wasi_snapshot_preview1" "environ_sizes_get"
            (func $environ_sizes_get (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "environ_get"
            (func $environ_get (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; env_count at 0, env_buf_size at 4, ciovec at 16, nwritten at 28.
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 0
            i32.const 4
            call $environ_sizes_get
            call $exit_if_errno

            i32.const 32
            i32.const 128
            call $environ_get
            call $exit_if_errno

            i32.const 16
            i32.const 128
            i32.store
            i32.const 20
            i32.const 4
            i32.load
            i32.store
            i32.const 1
            i32.const 16
            i32.const 1
            i32.const 28
            call $fd_write
            call $exit_if_errno))
        "#
}

pub(in crate::tests) fn wasi_spawn_echo_preview1_module() -> &'static str {
    wasi_spawn_preview1_module("echo")
}

pub(in crate::tests) fn wasi_spawn_envdump_preview1_module() -> &'static str {
    wasi_spawn_preview1_module("envdump")
}

pub(in crate::tests) fn wasi_spawn_loop_preview1_module() -> &'static str {
    wasi_spawn_preview1_module("loop")
}

pub(in crate::tests) fn wasi_spawn_burn_twice_preview1_module() -> &'static str {
    r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait"
            (func $proc_spawn_wait (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 32: ["burn", 0]. string at 16.
          (data (i32.const 32) "\10\00\00\00\00\00\00\00")
          (data (i32.const 16) "burn\00")
          (func (export "_start")
            i32.const 32
            i32.const 0
            call $proc_spawn_wait
            i32.eqz
            if
            else
              i32.const 1
              call $proc_exit
            end
            i32.const 32
            i32.const 0
            call $proc_spawn_wait
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
}

pub(in crate::tests) fn wasi_spawn_echo_at_cwd_preview1_module(cwd: &str) -> String {
    let cwd_bytes = wat_bytes(cwd.as_bytes());
    format!(
        r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait_at"
            (func $proc_spawn_wait_at (param i32 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 32: ["echo", 0]. string at 16. cwd at 48.
          (data (i32.const 16) "echo\00")
          (data (i32.const 32) "\10\00\00\00\00\00\00\00")
          (data (i32.const 48) "{cwd_bytes}")
          (func (export "_start")
            i32.const 32
            i32.const 0
            i32.const 48
            i32.const {cwd_len}
            call $proc_spawn_wait_at
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#,
        cwd_len = cwd.len(),
    )
}

pub(in crate::tests) fn wasi_spawn_work_echo_preview1_module() -> &'static str {
    wasi_spawn_preview1_module("/work/echo")
}

pub(in crate::tests) fn wasi_spawn_echo_to_work_link_preview1_module() -> &'static str {
    r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait_at_io"
            (func $proc_spawn_wait_at_io
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 64: ["echo", 0]. string at 16.
          (data (i32.const 16) "echo\00")
          (data (i32.const 64) "\10\00\00\00\00\00\00\00")
          (data (i32.const 80) "/work")
          (data (i32.const 96) "/work/link")
          (func (export "_start")
            i32.const 64 ;; argv
            i32.const 0  ;; envp
            i32.const 80 ;; cwd
            i32.const 5  ;; cwd len
            i32.const 0  ;; stdin path
            i32.const 0  ;; stdin path len
            i32.const 96 ;; stdout path
            i32.const 10 ;; stdout path len
            i32.const 0  ;; stdout append
            i32.const 0  ;; stderr path
            i32.const 0  ;; stderr path len
            i32.const 0  ;; stderr append
            i32.const 0  ;; stderr to stdout
            call $proc_spawn_wait_at_io
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
}

pub(in crate::tests) fn wasi_spawn_echo_to_out_file_preview1_module() -> &'static str {
    r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait_at_io"
            (func $proc_spawn_wait_at_io
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 64: ["echo", 0]. cwd at 80. stdout at 96.
          (data (i32.const 16) "echo\00")
          (data (i32.const 64) "\10\00\00\00\00\00\00\00")
          (data (i32.const 80) "/work")
          (data (i32.const 96) "out.txt")
          (func (export "_start")
            i32.const 64 ;; argv
            i32.const 0  ;; envp
            i32.const 80 ;; cwd
            i32.const 5  ;; cwd len
            i32.const 0  ;; stdin path
            i32.const 0  ;; stdin path len
            i32.const 96 ;; stdout path
            i32.const 7  ;; stdout path len
            i32.const 0  ;; stdout append
            i32.const 0  ;; stderr path
            i32.const 0  ;; stderr path len
            i32.const 0  ;; stderr append
            i32.const 0  ;; stderr to stdout
            call $proc_spawn_wait_at_io
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
}

pub(in crate::tests) fn wasi_spawn_writer_to_out_file_preview1_module() -> &'static str {
    r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait_at_io"
            (func $proc_spawn_wait_at_io
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 64: ["writer", 0]. cwd at 80. stdout at 96.
          (data (i32.const 16) "writer\00")
          (data (i32.const 64) "\10\00\00\00\00\00\00\00")
          (data (i32.const 80) "/work")
          (data (i32.const 96) "out.txt")
          (func (export "_start")
            i32.const 64 ;; argv
            i32.const 0  ;; envp
            i32.const 80 ;; cwd
            i32.const 5  ;; cwd len
            i32.const 0  ;; stdin path
            i32.const 0  ;; stdin path len
            i32.const 96 ;; stdout path
            i32.const 7  ;; stdout path len
            i32.const 0  ;; stdout append
            i32.const 0  ;; stderr path
            i32.const 0  ;; stderr path len
            i32.const 0  ;; stderr append
            i32.const 0  ;; stderr to stdout
            call $proc_spawn_wait_at_io
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
}

pub(in crate::tests) fn wasi_spawn_writer_to_err_file_preview1_module() -> &'static str {
    r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait_at_io"
            (func $proc_spawn_wait_at_io
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 64: ["writer", 0]. cwd at 80. stderr at 96.
          (data (i32.const 16) "writer\00")
          (data (i32.const 64) "\10\00\00\00\00\00\00\00")
          (data (i32.const 80) "/work")
          (data (i32.const 96) "err.txt")
          (func (export "_start")
            i32.const 64 ;; argv
            i32.const 0  ;; envp
            i32.const 80 ;; cwd
            i32.const 5  ;; cwd len
            i32.const 0  ;; stdin path
            i32.const 0  ;; stdin path len
            i32.const 0  ;; stdout path
            i32.const 0  ;; stdout path len
            i32.const 0  ;; stdout append
            i32.const 96 ;; stderr path
            i32.const 7  ;; stderr path len
            i32.const 0  ;; stderr append
            i32.const 0  ;; stderr to stdout
            call $proc_spawn_wait_at_io
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
}

pub(in crate::tests) fn wasi_spawn_echo_with_stderr_file_preview1_module() -> &'static str {
    r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait_at_io"
            (func $proc_spawn_wait_at_io
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 64: ["echo", 0]. cwd at 80. stderr at 96.
          (data (i32.const 16) "echo\00")
          (data (i32.const 64) "\10\00\00\00\00\00\00\00")
          (data (i32.const 80) "/work")
          (data (i32.const 96) "err.txt")
          (func (export "_start")
            i32.const 64 ;; argv
            i32.const 0  ;; envp
            i32.const 80 ;; cwd
            i32.const 5  ;; cwd len
            i32.const 0  ;; stdin path
            i32.const 0  ;; stdin path len
            i32.const 0  ;; stdout path
            i32.const 0  ;; stdout path len
            i32.const 0  ;; stdout append
            i32.const 96 ;; stderr path
            i32.const 7  ;; stderr path len
            i32.const 0  ;; stderr append
            i32.const 0  ;; stderr to stdout
            call $proc_spawn_wait_at_io
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
}

pub(in crate::tests) fn wasi_spawn_cat_with_file_stdin_preview1_module() -> &'static str {
    r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait_at_io"
            (func $proc_spawn_wait_at_io
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 64: ["cat", 0]. cwd at 80. stdin at 96.
          (data (i32.const 16) "cat\00")
          (data (i32.const 64) "\10\00\00\00\00\00\00\00")
          (data (i32.const 80) "/work")
          (data (i32.const 96) "in.txt")
          (func (export "_start")
            i32.const 64 ;; argv
            i32.const 0  ;; envp
            i32.const 80 ;; cwd
            i32.const 5  ;; cwd len
            i32.const 96 ;; stdin path
            i32.const 6  ;; stdin path len
            i32.const 0  ;; stdout path
            i32.const 0  ;; stdout path len
            i32.const 0  ;; stdout append
            i32.const 0  ;; stderr path
            i32.const 0  ;; stderr path len
            i32.const 0  ;; stderr append
            i32.const 0  ;; stderr to stdout
            call $proc_spawn_wait_at_io
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
}

pub(in crate::tests) fn wasi_spawn_outside_bin_with_relative_child_stderr_preview1_module()
-> &'static str {
    r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait_at_io"
            (func $proc_spawn_wait_at_io
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 64: ["/work/echo", 0]. cwd at 80. stderr at 112.
          (data (i32.const 16) "/work/echo\00")
          (data (i32.const 64) "\10\00\00\00\00\00\00\00")
          (data (i32.const 80) "/work/subdir")
          (data (i32.const 112) "err.txt")
          (func (export "_start")
            i32.const 64  ;; argv
            i32.const 0   ;; envp
            i32.const 80  ;; cwd
            i32.const 12  ;; cwd len
            i32.const 0   ;; stdin path
            i32.const 0   ;; stdin path len
            i32.const 0   ;; stdout path
            i32.const 0   ;; stdout path len
            i32.const 0   ;; stdout append
            i32.const 112 ;; stderr path
            i32.const 7   ;; stderr path len
            i32.const 0   ;; stderr append
            i32.const 0   ;; stderr to stdout
            call $proc_spawn_wait_at_io
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
}

pub(in crate::tests) fn wasi_spawn_echo_with_ambiguous_stderr_preview1_module() -> &'static str {
    r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait_at_io"
            (func $proc_spawn_wait_at_io
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 64: ["echo", 0]. cwd at 80. stdout at 96. stderr at 112.
          (data (i32.const 16) "echo\00")
          (data (i32.const 64) "\10\00\00\00\00\00\00\00")
          (data (i32.const 80) "/work")
          (data (i32.const 96) "out.txt")
          (data (i32.const 112) "err.txt")
          (func (export "_start")
            i32.const 64  ;; argv
            i32.const 0   ;; envp
            i32.const 80  ;; cwd
            i32.const 5   ;; cwd len
            i32.const 0   ;; stdin path
            i32.const 0   ;; stdin path len
            i32.const 96  ;; stdout path
            i32.const 7   ;; stdout path len
            i32.const 0   ;; stdout append
            i32.const 112 ;; stderr path
            i32.const 7   ;; stderr path len
            i32.const 0   ;; stderr append
            i32.const 1   ;; stderr to stdout
            call $proc_spawn_wait_at_io
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
}

pub(in crate::tests) fn wasi_spawn_echo_with_oversized_stdout_path_len_preview1_module()
-> &'static str {
    r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait_at_io"
            (func $proc_spawn_wait_at_io
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 64: ["echo", 0]. cwd at 80. stdout starts at 96.
          (data (i32.const 16) "echo\00")
          (data (i32.const 64) "\10\00\00\00\00\00\00\00")
          (data (i32.const 80) "/work")
          (data (i32.const 96) "out.txt")
          (func (export "_start")
            i32.const 64 ;; argv
            i32.const 0  ;; envp
            i32.const 80 ;; cwd
            i32.const 5  ;; cwd len
            i32.const 0  ;; stdin path
            i32.const 0  ;; stdin path len
            i32.const 96 ;; stdout path
            i32.const 4097 ;; stdout path len
            i32.const 0  ;; stdout append
            i32.const 0  ;; stderr path
            i32.const 0  ;; stderr path len
            i32.const 0  ;; stderr append
            i32.const 0  ;; stderr to stdout
            call $proc_spawn_wait_at_io
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
}

pub(in crate::tests) fn wasi_spawn_oversized_argv0_preview1_module() -> String {
    let argv0 = "a".repeat(4097);
    format!(
        r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait"
            (func $proc_spawn_wait (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 16: [oversized argv0, 0]. argv0 string at 32.
          (data (i32.const 16) "\20\00\00\00\00\00\00\00")
          (data (i32.const 32) "{argv0}\00")
          (func (export "_start")
            i32.const 16
            i32.const 0
            call $proc_spawn_wait
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
    )
}

pub(in crate::tests) fn wasi_spawn_echo_with_invalid_env_preview1_module() -> &'static str {
    r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait"
            (func $proc_spawn_wait (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 64: ["echo", 0]. env array at 96: ["BAD-NAME=1", 0].
          (data (i32.const 16) "echo\00")
          (data (i32.const 32) "BAD-NAME=1\00")
          (data (i32.const 64) "\10\00\00\00\00\00\00\00")
          (data (i32.const 96) "\20\00\00\00\00\00\00\00")
          (func (export "_start")
            i32.const 64
            i32.const 96
            call $proc_spawn_wait
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
}

pub(in crate::tests) fn wasi_spawn_echo_with_malformed_env_preview1_module() -> &'static str {
    r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait"
            (func $proc_spawn_wait (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 64: ["echo", 0]. env array at 96: ["MALFORMED", 0].
          (data (i32.const 16) "echo\00")
          (data (i32.const 32) "MALFORMED\00")
          (data (i32.const 64) "\10\00\00\00\00\00\00\00")
          (data (i32.const 96) "\20\00\00\00\00\00\00\00")
          (func (export "_start")
            i32.const 64
            i32.const 96
            call $proc_spawn_wait
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
}

pub(in crate::tests) fn wasi_spawn_preview1_module(argv0: &str) -> &'static str {
    match argv0 {
        "echo" => {
            r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait"
            (func $proc_spawn_wait (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 32: ["echo", 0]. string at 16.
          (data (i32.const 32) "\10\00\00\00\00\00\00\00")
          (data (i32.const 16) "echo\00")
          (func (export "_start")
            i32.const 32
            i32.const 0
            call $proc_spawn_wait
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
        }
        "envdump" => {
            r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait"
            (func $proc_spawn_wait (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 32: ["envdump", 0]. string at 16.
          (data (i32.const 32) "\10\00\00\00\00\00\00\00")
          (data (i32.const 16) "envdump\00")
          (func (export "_start")
            i32.const 32
            i32.const 0
            call $proc_spawn_wait
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
        }
        "loop" => {
            r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait"
            (func $proc_spawn_wait (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 32: ["loop", 0]. string at 16.
          (data (i32.const 32) "\10\00\00\00\00\00\00\00")
          (data (i32.const 16) "loop\00")
          (func (export "_start")
            i32.const 32
            i32.const 0
            call $proc_spawn_wait
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
        }
        "/work/echo" => {
            r#"
        (module
          (import "sinomo_sandbox_preview1" "proc_spawn_wait"
            (func $proc_spawn_wait (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; argv array at 32: ["/work/echo", 0]. string at 16.
          (data (i32.const 32) "\10\00\00\00\00\00\00\00")
          (data (i32.const 16) "/work/echo\00")
          (func (export "_start")
            i32.const 32
            i32.const 0
            call $proc_spawn_wait
            i32.eqz
            if
              i32.const 0
              call $proc_exit
            else
              i32.const 1
              call $proc_exit
            end))
        "#
        }
        _ => unreachable!("unsupported preview1 spawn test module"),
    }
}

pub(in crate::tests) fn wasi_stdin_to_stdout_module() -> &'static str {
    r#"
        (module
          (import "wasi_snapshot_preview1" "fd_read"
            (func $fd_read (param i32 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; iovec/ciovec at 0: buffer ptr 128, len 64. nread/nwritten at 20.
          (data (i32.const 0) "\80\00\00\00\40\00\00\00")
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 0
            i32.const 0
            i32.const 1
            i32.const 20
            call $fd_read
            call $exit_if_errno
            i32.const 4
            i32.const 20
            i32.load
            i32.store
            i32.const 1
            i32.const 0
            i32.const 1
            i32.const 20
            call $fd_write
            call $exit_if_errno))
        "#
}

pub(in crate::tests) fn wasi_sock_send_exit_module() -> &'static str {
    r#"
        (module
          (import "wasi_snapshot_preview1" "sock_send"
            (func $sock_send (param i32 i32 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; ciovec at 0: data ptr 128, data len 1. nwritten at 20.
          (data (i32.const 0) "\80\00\00\00\01\00\00\00")
          (data (i32.const 128) "x")
          (func (export "_start")
            i32.const 3
            i32.const 0
            i32.const 1
            i32.const 0
            i32.const 20
            call $sock_send
            call $proc_exit))
        "#
}

pub(in crate::tests) fn wasi_exit_module(status: i32) -> String {
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (func (export "_start")
                i32.const {status}
                call $proc_exit))
            "#
    )
}

pub(in crate::tests) fn looping_module() -> &'static str {
    r#"
        (module
          (func (export "_start")
            (loop $again
              br $again)))
        "#
}

pub(in crate::tests) fn wasi_finite_burn_module(iterations: u32) -> String {
    format!(
        r#"
        (module
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (func (export "_start")
            (local $remaining i32)
            i32.const {iterations}
            local.set $remaining
            (loop $again
              local.get $remaining
              i32.eqz
              if
                i32.const 0
                call $proc_exit
              end
              local.get $remaining
              i32.const 1
              i32.sub
              local.set $remaining
              br $again)))"#
    )
}

pub(in crate::tests) fn memory_hungry_module() -> &'static str {
    r#"
        (module
          (memory 2)
          (func (export "_start")))
        "#
}

#[cfg(feature = "wasix-runtime")]
pub(in crate::tests) fn wasix_spawn_echo_module() -> &'static str {
    r#"
        (module
          (import "wasix_32v1" "proc_spawn"
            (func $proc_spawn
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasix_32v1" "proc_join"
            (func $proc_join (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; child name, argv lines, cwd, process handles, option pid, join status.
          (data (i32.const 64) "/bin/echo")
          (data (i32.const 80) "/bin/echo\0a")
          (data (i32.const 112) ".")
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 64
            i32.const 9
            i32.const 0
            i32.const 80
            i32.const 10
            i32.const 96
            i32.const 0
            i32.const 2
            i32.const 1
            i32.const 1
            i32.const 112
            i32.const 1
            i32.const 128
            call $proc_spawn
            call $exit_if_errno

            i32.const 160
            i32.const 1
            i32.store8
            i32.const 164
            i32.const 128
            i32.load
            i32.store
            i32.const 160
            i32.const 0
            i32.const 192
            call $proc_join
            call $exit_if_errno))
        "#
}

#[cfg(feature = "wasix-runtime")]
pub(in crate::tests) fn wasix_spawn_absolute_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    let argv = format!("{path}\\0a");
    let escaped_argv = wat_bytes(argv.as_bytes());
    let argv_len = argv.len();
    format!(
        r#"
        (module
          (import "wasix_32v1" "proc_spawn"
            (func $proc_spawn
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasix_32v1" "proc_join"
            (func $proc_join (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 64) "{escaped_path}")
          (data (i32.const 160) "{escaped_argv}")
          (data (i32.const 112) ".")
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 64
            i32.const {path_len}
            i32.const 0
            i32.const 160
            i32.const {argv_len}
            i32.const 96
            i32.const 0
            i32.const 2
            i32.const 1
            i32.const 1
            i32.const 112
            i32.const 1
            i32.const 128
            call $proc_spawn
            call $exit_if_errno

            i32.const 192
            i32.const 1
            i32.store8
            i32.const 196
            i32.const 128
            i32.load
            i32.store
            i32.const 192
            i32.const 0
            i32.const 224
            call $proc_join
            call $exit_if_errno))
        "#
    )
}

#[cfg(feature = "wasix-runtime")]
pub(in crate::tests) fn wasix_spawn_echo_via_path_module() -> &'static str {
    r#"
        (module
          (import "wasix_32v1" "proc_spawn2"
            (func $proc_spawn2
              (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
              (result i32)))
          (import "wasix_32v1" "proc_join"
            (func $proc_join (param i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          ;; child name, argv lines, child pid, option pid, join status.
          (data (i32.const 64) "echo")
          (data (i32.const 80) "echo\0a")
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 64
            i32.const 4
            i32.const 80
            i32.const 5
            i32.const 0
            i32.const 0
            i32.const 0
            i32.const 0
            i32.const 0
            i32.const 0
            i32.const 1
            i32.const 0
            i32.const 0
            i32.const 128
            call $proc_spawn2
            call $exit_if_errno

            i32.const 160
            i32.const 1
            i32.store8
            i32.const 164
            i32.const 128
            i32.load
            i32.store
            i32.const 160
            i32.const 0
            i32.const 192
            call $proc_join
            call $exit_if_errno))
        "#
}

pub(in crate::tests) fn wasi_write_file_module(path: &str) -> String {
    wasi_write_file_module_on_fd(3, path)
}

pub(in crate::tests) fn wasi_write_file_module_on_fd(fd: i32, path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_open"
                (func $path_open (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; ciovec at 0: data ptr 128, data len 7
              (data (i32.const 0) "\80\00\00\00\07\00\00\00")
              (data (i32.const 96) "{escaped_path}")
              (data (i32.const 128) "created")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const {fd}
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 9
                i64.const 64
                i64.const 0
                i32.const 0
                i32.const 64
                call $path_open
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 0
                i32.const 1
                i32.const 72
                call $fd_write
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasi_read_path_to_stdout_module(
    fd: i32,
    path: &str,
    read_len: usize,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_open"
                (func $path_open (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_read"
                (func $fd_read (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; iovec/ciovec at 0: data ptr 128, len read_len. nread/nwritten at 20.
              (data (i32.const 0) "\80\00\00\00")
              (data (i32.const 96) "{escaped_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 4
                i32.const {read_len}
                i32.store
                i32.const {fd}
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const 2
                i64.const 0
                i32.const 0
                i32.const 64
                call $path_open
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 0
                i32.const 1
                i32.const 20
                call $fd_read
                call $exit_if_errno

                i32.const 4
                i32.const 20
                i32.load
                i32.store
                i32.const 1
                i32.const 0
                i32.const 1
                i32.const 24
                call $fd_write
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasi_read_path_from_any_preopen_to_stdout_module(
    path: &str,
    read_len: usize,
    first_fd: i32,
    last_fd: i32,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    let mut attempts = String::new();
    for fd in first_fd..=last_fd {
        attempts.push_str(&format!(
            r#"
                i32.const {fd}
                call $try_read
                if
                  return
                end
                "#
        ));
    }
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_open"
                (func $path_open (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_read"
                (func $fd_read (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (memory (export "memory") 1 128)
              ;; iovec/ciovec at 0: data ptr 128, len read_len. nread/nwritten at 20.
              (data (i32.const 0) "\80\00\00\00")
              (data (i32.const 32) "\88\00\00\00\08\00\00\00")
              (data (i32.const 96) "{escaped_path}")
              (data (i32.const 136) "missing\0a")
              (func $try_read (param $dirfd i32) (result i32)
                (local $errno i32)
                (local $opened_fd i32)
                i32.const 4
                i32.const {read_len}
                i32.store
                local.get $dirfd
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const 2
                i64.const 0
                i32.const 0
                i32.const 64
                call $path_open
                local.set $errno
                local.get $errno
                i32.eqz
                if (result i32)
                  i32.const 64
                  i32.load
                  local.set $opened_fd
                  local.get $opened_fd
                  i32.const 0
                  i32.const 1
                  i32.const 20
                  call $fd_read
                  local.set $errno
                  local.get $errno
                  i32.eqz
                  if (result i32)
                    i32.const 4
                    i32.const 20
                    i32.load
                    i32.store
                    i32.const 1
                    i32.const 0
                    i32.const 1
                    i32.const 24
                    call $fd_write
                    drop
                    local.get $opened_fd
                    call $fd_close
                    drop
                    i32.const 1
                  else
                    local.get $opened_fd
                    call $fd_close
                    drop
                    i32.const 0
                  end
                else
                  i32.const 0
                end)
              (func (export "_start")
                {attempts}
                i32.const 1
                i32.const 32
                i32.const 1
                i32.const 72
                call $fd_write
                drop))
            "#
    )
}

pub(in crate::tests) fn wasi_chmod_module(path: &str, mode: u32) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "sinomo_sandbox_preview1" "path_chmod"
                (func $path_chmod (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_path}")
              (func (export "_start")
                i32.const 96
                i32.const {path_len}
                i32.const {mode}
                call $path_chmod
                call $proc_exit))
            "#
    )
}

pub(in crate::tests) fn wat_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

pub(in crate::tests) fn fd3_prestat_exit_module() -> &'static str {
    r#"
        (module
          (import "wasi_snapshot_preview1" "fd_prestat_get"
            (func $fd_prestat_get (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (func (export "_start")
            i32.const 3
            i32.const 0
            call $fd_prestat_get
            call $proc_exit))
        "#
}
