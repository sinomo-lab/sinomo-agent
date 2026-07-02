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

pub(in crate::tests) fn wasix_write_absolute_file_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
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
                (local $errno i32)
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 9
                i64.const 64
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
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
	                call $exit_if_errno
	                i32.const 0
	                call $proc_exit))
	            "#
    )
}

pub(in crate::tests) fn wasix_create_directory_then_write_absolute_file_module(
    directory: &str,
    file: &str,
) -> String {
    let escaped_directory = wat_bytes(directory.as_bytes());
    let directory_len = directory.len();
    let escaped_file = wat_bytes(file.as_bytes());
    let file_len = file.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_create_directory"
                (func $path_create_directory (param i32 i32 i32) (result i32)))
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; ciovec at 0: data ptr 224, data len 7
              (data (i32.const 0) "\e0\00\00\00\07\00\00\00")
              (data (i32.const 96) "{escaped_directory}")
              (data (i32.const 160) "{escaped_file}")
              (data (i32.const 224) "created")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 96
                i32.const {directory_len}
                call $path_create_directory
                call $exit_if_errno

                i32.const 3
                i32.const 0
                i32.const 160
                i32.const {file_len}
                i32.const 9
                i64.const 64
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
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
                call $exit_if_errno
                i32.const 0
                call $proc_exit))
            "#
    )
}

pub(in crate::tests) fn wasix_replace_file_with_directory_then_write_module(
    path: &str,
    child: &str,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    let escaped_child = wat_bytes(child.as_bytes());
    let child_len = child.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_unlink_file"
                (func $path_unlink_file (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_create_directory"
                (func $path_create_directory (param i32 i32 i32) (result i32)))
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; ciovec at 0: data ptr 224, data len 7
              (data (i32.const 0) "\e0\00\00\00\07\00\00\00")
              (data (i32.const 96) "{escaped_path}")
              (data (i32.const 160) "{escaped_child}")
              (data (i32.const 224) "created")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 96
                i32.const {path_len}
                call $path_unlink_file
                call $exit_if_errno

                i32.const 3
                i32.const 96
                i32.const {path_len}
                call $path_create_directory
                call $exit_if_errno

                i32.const 3
                i32.const 0
                i32.const 160
                i32.const {child_len}
                i32.const 9
                i64.const 64
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
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
                call $exit_if_errno
                i32.const 0
                call $proc_exit))
            "#
    )
}

pub(in crate::tests) fn wasix_truncate_absolute_file_module(path: &str, size: u64) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_filestat_set_size"
                (func $fd_filestat_set_size (param i32 i64) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                (local $errno i32)
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const 4194368
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 64
                i32.load
                i64.const {size}
                call $fd_filestat_set_size
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_pwrite_absolute_file_module(
    path: &str,
    offset: u64,
    contents: &[u8],
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    let escaped_contents = wat_bytes(contents);
    let contents_len = contents.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_pwrite"
                (func $fd_pwrite (param i32 i32 i32 i64 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_path}")
              (data (i32.const 160) "{escaped_contents}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const 64
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 0
                i32.const 160
                i32.store
                i32.const 4
                i32.const {contents_len}
                i32.store

                i32.const 64
                i32.load
                i32.const 0
                i32.const 1
                i64.const {offset}
                i32.const 72
                call $fd_pwrite
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_seek_truncate_then_write_absolute_file_module(
    path: &str,
    offset: u64,
    truncate_size: u64,
    contents: &[u8],
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    let escaped_contents = wat_bytes(contents);
    let contents_len = contents.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_seek"
                (func $fd_seek (param i32 i64 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_filestat_set_size"
                (func $fd_filestat_set_size (param i32 i64) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_path}")
              (data (i32.const 160) "{escaped_contents}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 64
                i32.load
                i64.const {offset}
                i32.const 0
                i32.const 72
                call $fd_seek
                call $exit_if_errno

                i32.const 64
                i32.load
                i64.const {truncate_size}
                call $fd_filestat_set_size
                call $exit_if_errno

                i32.const 0
                i32.const 160
                i32.store
                i32.const 4
                i32.const {contents_len}
                i32.store
                i32.const 64
                i32.load
                i32.const 0
                i32.const 1
                i32.const 80
                call $fd_write
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_allocate_absolute_file_module(
    path: &str,
    offset: u64,
    len: u64,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_allocate"
                (func $fd_allocate (param i32 i64 i64) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 64
                i32.load
                i64.const {offset}
                i64.const {len}
                call $fd_allocate
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_pwrite_then_allocate_first_fd_absolute_file_module(
    path: &str,
    pwrite_offset: u64,
    pwrite_payload: &[u8],
    allocate_offset: u64,
    allocate_len: u64,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    let escaped_payload = wat_bytes(pwrite_payload);
    let payload_len = pwrite_payload.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_pwrite"
                (func $fd_pwrite (param i32 i32 i32 i64 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_allocate"
                (func $fd_allocate (param i32 i64 i64) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_path}")
              (data (i32.const 256) "{escaped_payload}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func $open_path (param $fd_out i32)
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                local.get $fd_out
                call $path_open2
                call $exit_if_errno)
              (func (export "_start")
                i32.const 64
                call $open_path
                i32.const 68
                call $open_path

                i32.const 0
                i32.const 256
                i32.store
                i32.const 4
                i32.const {payload_len}
                i32.store
                i32.const 68
                i32.load
                i32.const 0
                i32.const 1
                i64.const {pwrite_offset}
                i32.const 84
                call $fd_pwrite
                call $exit_if_errno

                i32.const 64
                i32.load
                i64.const {allocate_offset}
                i64.const {allocate_len}
                call $fd_allocate
                call $exit_if_errno

                i32.const 68
                i32.load
                call $fd_close
                call $exit_if_errno
                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_exclusive_create_must_fail_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_path}")
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 5
                i64.const 64
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                i32.eqz
                if
                  i32.const 64
                  i32.load
                  call $fd_close
                  drop
                  i32.const 1
                  call $proc_exit
                else
                  i32.const 0
                  call $proc_exit
                end))
            "#
    )
}

pub(in crate::tests) fn wasix_open_absolute_path_expect_errno_module(
    path: &str,
    o_flags: u32,
    rights_base: u64,
    expected_errno: u32,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_path}")
              (func (export "_start")
                (local $errno i32)
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const {o_flags}
                i64.const {rights_base}
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                local.set $errno
                local.get $errno
                i32.eqz
                if
                  i32.const 64
                  i32.load
                  call $fd_close
                  drop
                  i32.const 99
                  call $proc_exit
                end
                local.get $errno
                i32.const {expected_errno}
                i32.eq
                if
                  i32.const 0
                  call $proc_exit
                else
                  local.get $errno
                  call $proc_exit
                end))
            "#
    )
}

pub(in crate::tests) fn wasix_append_absolute_file_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; ciovec at 0: data ptr 128, data len 9
              (data (i32.const 0) "\80\00\00\00\09\00\00\00")
              (data (i32.const 96) "{escaped_path}")
              (data (i32.const 128) "-appended")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const 64
                i64.const 0
                i32.const 1
                i32.const 0
                i32.const 64
                call $path_open2
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

pub(in crate::tests) fn wasix_seek_end_after_second_fd_append_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_seek"
                (func $fd_seek (param i32 i64 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; ciovec 0: data ptr 128, len 1; ciovec 16: data ptr 144, len 1
              (data (i32.const 0) "\80\00\00\00\01\00\00\00")
              (data (i32.const 16) "\90\00\00\00\01\00\00\00")
              (data (i32.const 96) "{escaped_path}")
              (data (i32.const 128) "B")
              (data (i32.const 144) "C")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                ;; fd1: write + seek
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const 68
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                ;; fd2: append write
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const 64
                i64.const 0
                i32.const 1
                i32.const 0
                i32.const 68
                call $path_open2
                call $exit_if_errno

                i32.const 68
                i32.load
                i32.const 0
                i32.const 1
                i32.const 72
                call $fd_write
                call $exit_if_errno

                i32.const 64
                i32.load
                i64.const 0
                i32.const 2
                i32.const 80
                call $fd_seek
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 16
                i32.const 1
                i32.const 72
                call $fd_write
                call $exit_if_errno

                i32.const 68
                i32.load
                call $fd_close
                call $exit_if_errno
                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_append_tell_after_second_fd_pwrite_module(
    path: &str,
    pwrite_offset: u64,
    expected_tell: u64,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_pwrite"
                (func $fd_pwrite (param i32 i32 i32 i64 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_tell"
                (func $fd_tell (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; ciovec 0: data ptr 128, len 1; ciovec 16: data ptr 144, len 1
              (data (i32.const 0) "\80\00\00\00\01\00\00\00")
              (data (i32.const 16) "\90\00\00\00\01\00\00\00")
              (data (i32.const 96) "{escaped_path}")
              (data (i32.const 128) "A")
              (data (i32.const 144) "Z")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                ;; fd1: append + tell
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const -1
                i64.const 0
                i32.const 1
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                ;; fd2: positional write that extends the file after fd1 was opened.
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const -1
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 68
                call $path_open2
                call $exit_if_errno

                i32.const 68
                i32.load
                i32.const 16
                i32.const 1
                i64.const {pwrite_offset}
                i32.const 76
                call $fd_pwrite
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 0
                i32.const 1
                i32.const 80
                call $fd_write
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 88
                call $fd_tell
                call $exit_if_errno

                i32.const 68
                i32.load
                call $fd_close
                call $exit_if_errno
                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno

                i32.const 88
                i64.load
                i64.const {expected_tell}
                i64.eq
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

pub(in crate::tests) fn wasix_open_append_then_chmod_then_write_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasix_32v1" "path_chmod"
                (func $path_chmod (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; ciovec at 0: data ptr 128, data len 1
              (data (i32.const 0) "\80\00\00\00\01\00\00\00")
              (data (i32.const 96) "{escaped_path}")
              (data (i32.const 128) "X")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const 64
                i64.const 0
                i32.const 1
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 96
                i32.const {path_len}
                i32.const 0
                call $path_chmod
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

pub(in crate::tests) fn wasix_open_read_then_chmod_then_read_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasix_32v1" "path_chmod"
                (func $path_chmod (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_read"
                (func $fd_read (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; iovec at 0: data ptr 128, data len 8
              (data (i32.const 0) "\80\00\00\00\08\00\00\00")
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const 2
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 96
                i32.const {path_len}
                i32.const 0
                call $path_chmod
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 0
                i32.const 1
                i32.const 72
                call $fd_read
                call $exit_if_errno

                i32.const 72
                i32.load
                i32.const 8
                i32.ne
                if
                  i32.const 78
                  call $proc_exit
                end

                i32.const 1
                i32.const 0
                i32.const 1
                i32.const 76
                call $fd_write
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_set_absolute_path_metadata_module(
    path: &str,
    mode: u32,
    uid: u32,
    gid: u32,
    atime: u64,
    mtime: u64,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_chmod"
                (func $path_chmod (param i32 i32 i32) (result i32)))
              (import "wasix_32v1" "path_chown"
                (func $path_chown (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_filestat_set_times"
                (func $path_filestat_set_times (param i32 i32 i32 i32 i64 i64 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 96
                i32.const {path_len}
                i32.const {mode}
                call $path_chmod
                call $exit_if_errno

                i32.const 96
                i32.const {path_len}
                i32.const {uid}
                i32.const {gid}
                call $path_chown
                call $exit_if_errno

                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i64.const {atime}
                i64.const {mtime}
                i32.const 5
                call $path_filestat_set_times
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_chmod_absolute_path_module(path: &str, mode: u32) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_chmod"
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

pub(in crate::tests) fn wasix_chown_absolute_path_module(path: &str, uid: u32, gid: u32) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_chown"
                (func $path_chown (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_path}")
              (func (export "_start")
                i32.const 96
                i32.const {path_len}
                i32.const {uid}
                i32.const {gid}
                call $path_chown
                call $proc_exit))
            "#
    )
}

pub(in crate::tests) fn wasix_set_absolute_path_times_module(
    path: &str,
    atime: u64,
    mtime: u64,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_filestat_set_times"
                (func $path_filestat_set_times (param i32 i32 i32 i32 i64 i64 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i64.const {atime}
                i64.const {mtime}
                i32.const 5
                call $path_filestat_set_times
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_set_absolute_path_times_with_flags_module(
    path: &str,
    atime: u64,
    mtime: u64,
    flags: u32,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_filestat_set_times"
                (func $path_filestat_set_times (param i32 i32 i32 i32 i64 i64 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i64.const {atime}
                i64.const {mtime}
                i32.const {flags}
                call $path_filestat_set_times
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_touch_absolute_path_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_touch_at"
                (func $path_touch_at (param i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 3
                i32.const 96
                i32.const {path_len}
                i32.const 1
                i32.const 0
                call $path_touch_at
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_set_fd_times_module(path: &str, atime: u64, mtime: u64) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_filestat_set_times"
                (func $fd_filestat_set_times (param i32 i64 i64 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 64
                i32.load
                i64.const {atime}
                i64.const {mtime}
                i32.const 5
                call $fd_filestat_set_times
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_set_fd_times_with_flags_module(
    path: &str,
    atime: u64,
    mtime: u64,
    flags: u32,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_filestat_set_times"
                (func $fd_filestat_set_times (param i32 i64 i64 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 64
                i32.load
                i64.const {atime}
                i64.const {mtime}
                i32.const {flags}
                call $fd_filestat_set_times
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_read_absolute_file_to_stdout_module(
    path: &str,
    read_len: usize,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const 2
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
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

pub(in crate::tests) fn wasix_open_unlink_then_read_to_stdout_module(
    path: &str,
    read_len: usize,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_unlink_file"
                (func $path_unlink_file (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_read"
                (func $fd_read (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; iovec/ciovec at 0: data ptr 192, len read_len. nread/nwritten at 20.
              (data (i32.const 0) "\c0\00\00\00")
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const 2
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 96
                i32.const {path_len}
                call $path_unlink_file
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

pub(in crate::tests) fn wasix_open_unlink_then_fd_filestat_nlink_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_unlink_file"
                (func $path_unlink_file (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_filestat_get"
                (func $fd_filestat_get (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 96
                i32.const {path_len}
                call $path_unlink_file
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 384
                call $fd_filestat_get
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno

                i32.const 408
                i64.load
                i64.eqz
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

pub(in crate::tests) fn wasix_open_unlink_recreate_then_read_to_stdout_module(
    path: &str,
    read_len: usize,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_unlink_file"
                (func $path_unlink_file (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_read"
                (func $fd_read (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; iovec/ciovec at 0. old fd out at 64, new fd out at 68, counts at 24/28.
              (data (i32.const 96) "{escaped_path}")
              (data (i32.const 224) "new")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const 2
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 96
                i32.const {path_len}
                call $path_unlink_file
                call $exit_if_errno

                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 9
                i64.const 64
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 68
                call $path_open2
                call $exit_if_errno

                i32.const 0
                i32.const 224
                i32.store
                i32.const 4
                i32.const 3
                i32.store
                i32.const 68
                i32.load
                i32.const 0
                i32.const 1
                i32.const 28
                call $fd_write
                call $exit_if_errno

                i32.const 68
                i32.load
                call $fd_close
                call $exit_if_errno

                i32.const 0
                i32.const 192
                i32.store
                i32.const 4
                i32.const {read_len}
                i32.store
                i32.const 64
                i32.load
                i32.const 0
                i32.const 1
                i32.const 24
                call $fd_read
                call $exit_if_errno

                i32.const 4
                i32.const 24
                i32.load
                i32.store
                i32.const 1
                i32.const 0
                i32.const 1
                i32.const 28
                call $fd_write
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_link_absolute_paths_module(old_path: &str, new_path: &str) -> String {
    wasix_link_absolute_paths_with_flags_module(old_path, new_path, 0)
}

pub(in crate::tests) fn wasix_link_absolute_paths_with_flags_module(
    old_path: &str,
    new_path: &str,
    old_flags: u32,
) -> String {
    let escaped_old_path = wat_bytes(old_path.as_bytes());
    let escaped_new_path = wat_bytes(new_path.as_bytes());
    let old_path_len = old_path.len();
    let new_path_len = new_path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_link"
                (func $path_link (param i32 i32 i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_old_path}")
              (data (i32.const 160) "{escaped_new_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const {old_flags}
                i32.const 96
                i32.const {old_path_len}
                i32.const 3
                i32.const 160
                i32.const {new_path_len}
                call $path_link
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_open_link_then_fd_filestat_nlink_module(
    old_path: &str,
    new_path: &str,
    expected_nlink: u64,
) -> String {
    let escaped_old_path = wat_bytes(old_path.as_bytes());
    let escaped_new_path = wat_bytes(new_path.as_bytes());
    let old_path_len = old_path.len();
    let new_path_len = new_path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_link"
                (func $path_link (param i32 i32 i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_filestat_get"
                (func $fd_filestat_get (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_old_path}")
              (data (i32.const 160) "{escaped_new_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {old_path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 320
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {old_path_len}
                i32.const 3
                i32.const 160
                i32.const {new_path_len}
                call $path_link
                call $exit_if_errno

                i32.const 320
                i32.load
                i32.const 384
                call $fd_filestat_get
                call $exit_if_errno

                i32.const 320
                i32.load
                call $fd_close
                call $exit_if_errno

                i32.const 408
                i64.load
                i64.const {expected_nlink}
                i64.eq
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

pub(in crate::tests) fn wasix_link_relative_between_directories_module(
    source_directory: &str,
    source_name: &str,
    target_directory: &str,
    target_name: &str,
) -> String {
    let escaped_source_directory = wat_bytes(source_directory.as_bytes());
    let escaped_source_name = wat_bytes(source_name.as_bytes());
    let escaped_target_directory = wat_bytes(target_directory.as_bytes());
    let escaped_target_name = wat_bytes(target_name.as_bytes());
    let source_directory_len = source_directory.len();
    let source_name_len = source_name.len();
    let target_directory_len = target_directory.len();
    let target_name_len = target_name.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_link"
                (func $path_link (param i32 i32 i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_source_directory}")
              (data (i32.const 192) "{escaped_source_name}")
              (data (i32.const 288) "{escaped_target_directory}")
              (data (i32.const 384) "{escaped_target_name}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {source_directory_len}
                i32.const 16
                i64.const 288768
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 0
                i32.const 288
                i32.const {target_directory_len}
                i32.const 16
                i64.const 290816
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 68
                call $path_open2
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 0
                i32.const 192
                i32.const {source_name_len}
                i32.const 68
                i32.load
                i32.const 384
                i32.const {target_name_len}
                call $path_link
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno
                i32.const 68
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_unlink_absolute_path_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_unlink_file"
                (func $path_unlink_file (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 3
                i32.const 96
                i32.const {path_len}
                call $path_unlink_file
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_rename_absolute_paths_module(
    old_path: &str,
    new_path: &str,
) -> String {
    let escaped_old_path = wat_bytes(old_path.as_bytes());
    let escaped_new_path = wat_bytes(new_path.as_bytes());
    let old_path_len = old_path.len();
    let new_path_len = new_path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_rename"
                (func $path_rename (param i32 i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_old_path}")
              (data (i32.const 160) "{escaped_new_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 96
                i32.const {old_path_len}
                i32.const 3
                i32.const 160
                i32.const {new_path_len}
                call $path_rename
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_open_rename_then_write_module(
    old_path: &str,
    new_path: &str,
) -> String {
    let escaped_old_path = wat_bytes(old_path.as_bytes());
    let escaped_new_path = wat_bytes(new_path.as_bytes());
    let old_path_len = old_path.len();
    let new_path_len = new_path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_rename"
                (func $path_rename (param i32 i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; ciovec at 0: data ptr 320, data len 8. fd out at 64.
              (data (i32.const 0) "\40\01\00\00\08\00\00\00")
              (data (i32.const 96) "{escaped_old_path}")
              (data (i32.const 192) "{escaped_new_path}")
              (data (i32.const 320) "-renamed")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {old_path_len}
                i32.const 0
                i64.const 64
                i64.const 0
                i32.const 1
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 96
                i32.const {old_path_len}
                i32.const 3
                i32.const 192
                i32.const {new_path_len}
                call $path_rename
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

pub(in crate::tests) fn wasix_open_target_rename_source_over_then_read_module(
    source_path: &str,
    target_path: &str,
    read_len: usize,
) -> String {
    let escaped_source_path = wat_bytes(source_path.as_bytes());
    let escaped_target_path = wat_bytes(target_path.as_bytes());
    let source_path_len = source_path.len();
    let target_path_len = target_path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_rename"
                (func $path_rename (param i32 i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_read"
                (func $fd_read (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; iovec/ciovec at 0: data ptr 320. fd out at 64, counts at 72/76.
              (data (i32.const 96) "{escaped_source_path}")
              (data (i32.const 192) "{escaped_target_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 192
                i32.const {target_path_len}
                i32.const 0
                i64.const 2
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 96
                i32.const {source_path_len}
                i32.const 3
                i32.const 192
                i32.const {target_path_len}
                call $path_rename
                call $exit_if_errno

                i32.const 0
                i32.const 320
                i32.store
                i32.const 4
                i32.const {read_len}
                i32.store
                i32.const 64
                i32.load
                i32.const 0
                i32.const 1
                i32.const 72
                call $fd_read
                call $exit_if_errno

                i32.const 4
                i32.const 72
                i32.load
                i32.store
                i32.const 1
                i32.const 0
                i32.const 1
                i32.const 76
                call $fd_write
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_create_directory_absolute_path_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_create_directory"
                (func $path_create_directory (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 3
                i32.const 96
                i32.const {path_len}
                call $path_create_directory
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_relative_create_remove_directory_module(
    directory: &str,
    child_name: &str,
) -> String {
    let escaped_directory = wat_bytes(directory.as_bytes());
    let escaped_child_name = wat_bytes(child_name.as_bytes());
    let directory_len = directory.len();
    let child_name_len = child_name.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_create_directory"
                (func $path_create_directory (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_remove_directory"
                (func $path_remove_directory (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_directory}")
              (data (i32.const 192) "{escaped_child_name}")
              (func $exit_if_errno (param $errno i32) (param $code i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $code
                  call $proc_exit
                end)
              (func $close_then_exit (param $fd i32) (param $step i32)
                local.get $fd
                call $fd_close
                drop
                local.get $step
                call $proc_exit)
              (func (export "_start")
                (local $errno i32)
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {directory_len}
                i32.const 16
                ;; Directory fd base rights: path_open, fd_readdir,
                ;; create-directory, and remove-directory. Do not request
                ;; fd_write for the directory itself.
                i64.const 33563136
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                i32.const 1
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 192
                i32.const {child_name_len}
                call $path_create_directory
                local.set $errno
                local.get $errno
                i32.eqz
                if
                else
	                  i32.const 64
	                  i32.load
	                  i32.const 2
	                  call $close_then_exit
	                end

                i32.const 64
                i32.load
                i32.const 192
                i32.const {child_name_len}
                call $path_remove_directory
                local.set $errno
                local.get $errno
                i32.eqz
                if
                else
	                  i32.const 64
	                  i32.load
	                  i32.const 3
	                  call $close_then_exit
	                end

	                i32.const 64
	                i32.load
	                call $fd_close
	                i32.const 4
	                call $exit_if_errno
	                i32.const 0
	                call $proc_exit))
	            "#
    )
}

pub(in crate::tests) fn wasix_relative_create_rename_unlink_file_module(
    directory: &str,
    created_name: &str,
    renamed_name: &str,
) -> String {
    let escaped_directory = wat_bytes(directory.as_bytes());
    let escaped_created_name = wat_bytes(created_name.as_bytes());
    let escaped_renamed_name = wat_bytes(renamed_name.as_bytes());
    let directory_len = directory.len();
    let created_name_len = created_name.len();
    let renamed_name_len = renamed_name.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_rename"
                (func $path_rename (param i32 i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_unlink_file"
                (func $path_unlink_file (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_directory}")
              (data (i32.const 192) "{escaped_created_name}")
              (data (i32.const 288) "{escaped_renamed_name}")
              (func $exit_if_errno (param $errno i32) (param $code i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $code
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {directory_len}
                i32.const 16
                ;; Directory fd base rights: path_open, fd_readdir, create-file,
                ;; rename-source/target, and unlink-file. Do not request
                ;; fd_write for the directory itself; writable child fd rights
                ;; come from the inheriting-rights argument below.
                i64.const 67331072
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                i32.const 1
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 0
                i32.const 192
                i32.const {created_name_len}
                i32.const 9
                i64.const 64
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 68
                call $path_open2
                i32.const 2
                call $exit_if_errno

                i32.const 68
                i32.load
                call $fd_close
                i32.const 3
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 192
                i32.const {created_name_len}
                i32.const 64
                i32.load
                i32.const 288
                i32.const {renamed_name_len}
                call $path_rename
                i32.const 4
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 288
                i32.const {renamed_name_len}
                call $path_unlink_file
                i32.const 5
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                i32.const 6
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_remove_directory_absolute_path_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_remove_directory"
                (func $path_remove_directory (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 3
                i32.const 96
                i32.const {path_len}
                call $path_remove_directory
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_readdir_absolute_path_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_readdir"
                (func $fd_readdir (param i32 i32 i32 i64 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 256)
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 2
                i64.const 16384
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 128
                i32.const 128
                i64.const 0
                i32.const 72
                call $fd_readdir
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_readdir_absolute_path_to_stdout_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_readdir"
                (func $fd_readdir (param i32 i32 i32 i64 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 256)
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 2
                i64.const 16384
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 256
                i32.const 4096
                i64.const 0
                i32.const 72
                call $fd_readdir
                call $exit_if_errno

                i32.const 0
                i32.const 256
                i32.store
                i32.const 4
                i32.const 72
                i32.load
                i32.store
                i32.const 1
                i32.const 0
                i32.const 1
                i32.const 80
                call $fd_write
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_open_child_unlink_then_readdir_to_stdout_module(
    directory: &str,
    child_path: &str,
) -> String {
    let escaped_directory = wat_bytes(directory.as_bytes());
    let escaped_child_path = wat_bytes(child_path.as_bytes());
    let directory_len = directory.len();
    let child_path_len = child_path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_unlink_file"
                (func $path_unlink_file (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_readdir"
                (func $fd_readdir (param i32 i32 i32 i64 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 256)
              (data (i32.const 96) "{escaped_directory}")
              (data (i32.const 192) "{escaped_child_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {directory_len}
                i32.const 2
                i64.const 16384
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 0
                i32.const 192
                i32.const {child_path_len}
                i32.const 0
                i64.const 2
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 68
                call $path_open2
                call $exit_if_errno

                i32.const 68
                i32.load
                call $fd_close
                call $exit_if_errno

                i32.const 3
                i32.const 192
                i32.const {child_path_len}
                call $path_unlink_file
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 256
                i32.const 4096
                i64.const 0
                i32.const 72
                call $fd_readdir
                call $exit_if_errno

                i32.const 0
                i32.const 256
                i32.store
                i32.const 4
                i32.const 72
                i32.load
                i32.store
                i32.const 1
                i32.const 0
                i32.const 1
                i32.const 80
                call $fd_write
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_open_dir_mkdir_then_fd_filestat_mtime_module(
    directory: &str,
    child_name: &str,
) -> String {
    let escaped_directory = wat_bytes(directory.as_bytes());
    let escaped_child_name = wat_bytes(child_name.as_bytes());
    let directory_len = directory.len();
    let child_name_len = child_name.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_create_directory"
                (func $path_create_directory (param i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_filestat_get"
                (func $fd_filestat_get (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_directory}")
              (data (i32.const 192) "{escaped_child_name}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {directory_len}
                i32.const 16
                ;; path_open + path_create_directory + fd_filestat_get.
                i64.const 2105856
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 320
                call $fd_filestat_get
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 192
                i32.const {child_name_len}
                call $path_create_directory
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 384
                call $fd_filestat_get
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno

                ;; WASI filestat layout: st_mtim is the u64 at byte offset 48.
                i32.const 432
                i64.load
                i32.const 368
                i64.load
                i64.gt_u
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

pub(in crate::tests) fn wasix_open_target_rename_source_over_then_set_fd_times_module(
    source_path: &str,
    target_path: &str,
    atime: u64,
    mtime: u64,
) -> String {
    let escaped_source_path = wat_bytes(source_path.as_bytes());
    let escaped_target_path = wat_bytes(target_path.as_bytes());
    let source_path_len = source_path.len();
    let target_path_len = target_path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_rename"
                (func $path_rename (param i32 i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_filestat_set_times"
                (func $fd_filestat_set_times (param i32 i64 i64 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_source_path}")
              (data (i32.const 192) "{escaped_target_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 192
                i32.const {target_path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 96
                i32.const {source_path_len}
                i32.const 3
                i32.const 192
                i32.const {target_path_len}
                call $path_rename
                call $exit_if_errno

                i32.const 64
                i32.load
                i64.const {atime}
                i64.const {mtime}
                i32.const 5
                call $fd_filestat_set_times
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_open_target_rename_source_over_then_fd_touch_module(
    source_path: &str,
    target_path: &str,
) -> String {
    let escaped_source_path = wat_bytes(source_path.as_bytes());
    let escaped_target_path = wat_bytes(target_path.as_bytes());
    let source_path_len = source_path.len();
    let target_path_len = target_path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_rename"
                (func $path_rename (param i32 i32 i32 i32 i32 i32) (result i32)))
              (import "wasix_32v1" "fd_touch"
                (func $fd_touch (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_source_path}")
              (data (i32.const 192) "{escaped_target_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 192
                i32.const {target_path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 96
                i32.const {source_path_len}
                i32.const 3
                i32.const 192
                i32.const {target_path_len}
                call $path_rename
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_touch
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_open_target_rename_source_over_then_fd_filestat_nlink_module(
    source_path: &str,
    target_path: &str,
) -> String {
    let escaped_source_path = wat_bytes(source_path.as_bytes());
    let escaped_target_path = wat_bytes(target_path.as_bytes());
    let source_path_len = source_path.len();
    let target_path_len = target_path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_rename"
                (func $path_rename (param i32 i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_filestat_get"
                (func $fd_filestat_get (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_source_path}")
              (data (i32.const 192) "{escaped_target_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 192
                i32.const {target_path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 96
                i32.const {source_path_len}
                i32.const 3
                i32.const 192
                i32.const {target_path_len}
                call $path_rename
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 384
                call $fd_filestat_get
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno

                i32.const 408
                i64.load
                i64.eqz
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

pub(in crate::tests) fn wasix_open_target_rename_source_over_then_fd_metadata_module(
    source_path: &str,
    target_path: &str,
    mode: u32,
    uid: u32,
    gid: u32,
) -> String {
    let escaped_source_path = wat_bytes(source_path.as_bytes());
    let escaped_target_path = wat_bytes(target_path.as_bytes());
    let source_path_len = source_path.len();
    let target_path_len = target_path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_rename"
                (func $path_rename (param i32 i32 i32 i32 i32 i32) (result i32)))
              (import "wasix_32v1" "fd_chmod"
                (func $fd_chmod (param i32 i32) (result i32)))
              (import "wasix_32v1" "fd_mode"
                (func $fd_mode (param i32 i32) (result i32)))
              (import "wasix_32v1" "path_mode"
                (func $path_mode (param i32 i32 i32) (result i32)))
              (import "wasix_32v1" "fd_chown"
                (func $fd_chown (param i32 i32 i32) (result i32)))
              (import "wasix_32v1" "fd_owner"
                (func $fd_owner (param i32 i32 i32) (result i32)))
              (import "wasix_32v1" "path_owner_at"
                (func $path_owner_at (param i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_source_path}")
              (data (i32.const 192) "{escaped_target_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 192
                i32.const {target_path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 96
                i32.const {source_path_len}
                i32.const 3
                i32.const 192
                i32.const {target_path_len}
                call $path_rename
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const {mode}
                call $fd_chmod
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 384
                call $fd_mode
                call $exit_if_errno

                i32.const 192
                i32.const {target_path_len}
                i32.const 388
                call $path_mode
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const {uid}
                i32.const {gid}
                call $fd_chown
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 392
                i32.const 396
                call $fd_owner
                call $exit_if_errno

                i32.const 3
                i32.const 192
                i32.const {target_path_len}
                i32.const 400
                i32.const 404
                call $path_owner_at
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno

                i32.const 384
                i32.load
                i32.const {mode}
                i32.eq
                i32.const 388
                i32.load
                i32.const {mode}
                i32.ne
                i32.and
                i32.const 392
                i32.load
                i32.const {uid}
                i32.eq
                i32.and
                i32.const 396
                i32.load
                i32.const {gid}
                i32.eq
                i32.and
                i32.const 400
                i32.load
                i32.const {uid}
                i32.ne
                i32.and
                i32.const 404
                i32.load
                i32.const {gid}
                i32.ne
                i32.and
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

pub(in crate::tests) fn wasix_filestat_same_inode_and_nlink_module(
    first_path: &str,
    second_path: &str,
    expected_nlink: u64,
) -> String {
    let escaped_first_path = wat_bytes(first_path.as_bytes());
    let escaped_second_path = wat_bytes(second_path.as_bytes());
    let first_path_len = first_path.len();
    let second_path_len = second_path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_filestat_get"
                (func $path_filestat_get (param i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_first_path}")
              (data (i32.const 192) "{escaped_second_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {first_path_len}
                i32.const 320
                call $path_filestat_get
                call $exit_if_errno

                i32.const 3
                i32.const 0
                i32.const 192
                i32.const {second_path_len}
                i32.const 384
                call $path_filestat_get
                call $exit_if_errno

                i32.const 328
                i64.load
                i32.const 392
                i64.load
                i64.eq
                i32.const 344
                i64.load
                i64.const {expected_nlink}
                i64.eq
                i32.and
                i32.const 408
                i64.load
                i64.const {expected_nlink}
                i64.eq
                i32.and
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

pub(in crate::tests) fn wasix_filestat_follow_symlink_matches_target_module(
    link_path: &str,
    target_path: &str,
) -> String {
    let escaped_link_path = wat_bytes(link_path.as_bytes());
    let escaped_target_path = wat_bytes(target_path.as_bytes());
    let link_path_len = link_path.len();
    let target_path_len = target_path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_filestat_get"
                (func $path_filestat_get (param i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_link_path}")
              (data (i32.const 192) "{escaped_target_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 1
                i32.const 96
                i32.const {link_path_len}
                i32.const 320
                call $path_filestat_get
                call $exit_if_errno

                i32.const 3
                i32.const 0
                i32.const 192
                i32.const {target_path_len}
                i32.const 384
                call $path_filestat_get
                call $exit_if_errno

                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {link_path_len}
                i32.const 448
                call $path_filestat_get
                call $exit_if_errno

                i32.const 328
                i64.load
                i32.const 392
                i64.load
                i64.ne
                if
                  i32.const 1
                  call $proc_exit
                end

                i32.const 456
                i64.load
                i32.const 392
                i64.load
                i64.eq
                if
                  i32.const 2
                  call $proc_exit
                end))
            "#
    )
}

pub(in crate::tests) fn wasix_fd_filestat_same_inode_and_nlink_module(
    first_path: &str,
    second_path: &str,
    expected_nlink: u64,
) -> String {
    let escaped_first_path = wat_bytes(first_path.as_bytes());
    let escaped_second_path = wat_bytes(second_path.as_bytes());
    let first_path_len = first_path.len();
    let second_path_len = second_path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_filestat_get"
                (func $fd_filestat_get (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_first_path}")
              (data (i32.const 192) "{escaped_second_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {first_path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 320
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 0
                i32.const 192
                i32.const {second_path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 324
                call $path_open2
                call $exit_if_errno

                i32.const 320
                i32.load
                i32.const 384
                call $fd_filestat_get
                call $exit_if_errno

                i32.const 324
                i32.load
                i32.const 448
                call $fd_filestat_get
                call $exit_if_errno

                i32.const 320
                i32.load
                call $fd_close
                call $exit_if_errno
                i32.const 324
                i32.load
                call $fd_close
                call $exit_if_errno

                i32.const 392
                i64.load
                i32.const 456
                i64.load
                i64.eq
                i32.const 408
                i64.load
                i64.const {expected_nlink}
                i64.eq
                i32.and
                i32.const 472
                i64.load
                i64.const {expected_nlink}
                i64.eq
                i32.and
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

pub(in crate::tests) fn wasix_open_then_write_alias_then_fd_filestat_size_module(
    open_path: &str,
    write_path: &str,
    contents: &[u8],
    expected_size: u64,
) -> String {
    let escaped_open_path = wat_bytes(open_path.as_bytes());
    let escaped_write_path = wat_bytes(write_path.as_bytes());
    let escaped_contents = wat_bytes(contents);
    let open_path_len = open_path.len();
    let write_path_len = write_path.len();
    let contents_len = contents.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_filestat_get"
                (func $fd_filestat_get (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_open_path}")
              (data (i32.const 192) "{escaped_write_path}")
              (data (i32.const 320) "{escaped_contents}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {open_path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 3
                i32.const 0
                i32.const 192
                i32.const {write_path_len}
                i32.const 0
                i64.const 64
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 68
                call $path_open2
                call $exit_if_errno

                i32.const 0
                i32.const 320
                i32.store
                i32.const 4
                i32.const {contents_len}
                i32.store
                i32.const 68
                i32.load
                i32.const 0
                i32.const 1
                i32.const 72
                call $fd_write
                call $exit_if_errno

                i32.const 68
                i32.load
                call $fd_close
                call $exit_if_errno

                i32.const 64
                i32.load
                i32.const 384
                call $fd_filestat_get
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno

                ;; WASI filestat layout: st_size is the u64 at byte offset 32.
                i32.const 416
                i64.load
                i64.const {expected_size}
                i64.eq
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

pub(in crate::tests) fn wasix_fd_filestat_ctime_equals_module(
    path: &str,
    expected_ctime: u64,
) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_filestat_get"
                (func $fd_filestat_get (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 0
                i64.const -1
                i64.const -1
                i32.const 0
                i32.const 0
                i32.const 192
                call $path_open2
                call $exit_if_errno

                i32.const 192
                i32.load
                i32.const 256
                call $fd_filestat_get
                call $exit_if_errno

                i32.const 192
                i32.load
                call $fd_close
                call $exit_if_errno

                ;; WASI filestat layout: st_ctim is the u64 at byte offset 56.
                i32.const 312
                i64.load
                i64.const {expected_ctime}
                i64.eq
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

pub(in crate::tests) fn wasix_readlink_absolute_path_to_stdout_module(path: &str) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_readlink"
                (func $path_readlink (param i32 i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              ;; path at 96, output buffer at 192, buf_used at 64, ciovec at 0.
              (data (i32.const 0) "\c0\00\00\00\00\00\00\00")
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
                i32.const 3
                i32.const 96
                i32.const {path_len}
                i32.const 192
                i32.const 128
                i32.const 64
                call $path_readlink
                call $exit_if_errno

                i32.const 4
                i32.const 64
                i32.load
                i32.store
                i32.const 1
                i32.const 0
                i32.const 1
                i32.const 72
                call $fd_write
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_symlink_absolute_path_module(
    target: &str,
    link_path: &str,
) -> String {
    let escaped_target = wat_bytes(target.as_bytes());
    let escaped_link_path = wat_bytes(link_path.as_bytes());
    let target_len = target.len();
    let link_path_len = link_path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_symlink"
                (func $path_symlink (param i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_target}")
              (data (i32.const 192) "{escaped_link_path}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 96
                i32.const {target_len}
                i32.const 3
                i32.const 192
                i32.const {link_path_len}
                call $path_symlink
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_symlink_relative_to_directory_module(
    directory: &str,
    target: &str,
    link_name: &str,
) -> String {
    let escaped_directory = wat_bytes(directory.as_bytes());
    let escaped_target = wat_bytes(target.as_bytes());
    let escaped_link_name = wat_bytes(link_name.as_bytes());
    let directory_len = directory.len();
    let target_len = target.len();
    let link_name_len = link_name.len();
    format!(
        r#"
            (module
              (import "wasix_32v1" "path_open2"
                (func $path_open2 (param i32 i32 i32 i32 i32 i64 i64 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "path_symlink"
                (func $path_symlink (param i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_close"
                (func $fd_close (param i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
              (data (i32.const 96) "{escaped_directory}")
              (data (i32.const 192) "{escaped_target}")
              (data (i32.const 256) "{escaped_link_name}")
              (func $exit_if_errno (param $errno i32)
                local.get $errno
                i32.eqz
                if
                else
                  local.get $errno
                  call $proc_exit
                end)
              (func (export "_start")
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {directory_len}
                i32.const 16
                i64.const 16777216
                i64.const 0
                i32.const 0
                i32.const 0
                i32.const 64
                call $path_open2
                call $exit_if_errno

                i32.const 192
                i32.const {target_len}
                i32.const 64
                i32.load
                i32.const 256
                i32.const {link_name_len}
                call $path_symlink
                call $exit_if_errno

                i32.const 64
                i32.load
                call $fd_close
                call $exit_if_errno))
            "#
    )
}

pub(in crate::tests) fn wasix_lstat_filetype_equals_module(path: &str, expected: u32) -> String {
    let escaped_path = wat_bytes(path.as_bytes());
    let path_len = path.len();
    format!(
        r#"
            (module
              (import "wasi_snapshot_preview1" "path_filestat_get"
                (func $path_filestat_get (param i32 i32 i32 i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "proc_exit"
                (func $proc_exit (param i32)))
              (memory (export "memory") 1 128)
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
                i32.const 3
                i32.const 0
                i32.const 96
                i32.const {path_len}
                i32.const 192
                call $path_filestat_get
                call $exit_if_errno

                i32.const 208
                i32.load8_u
                i32.const {expected}
                i32.eq
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

pub(in crate::tests) fn wat_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}
