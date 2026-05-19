import os
import sys

for _path in (
    "/cross-build/wasm32-wasip1/build/lib.wasi-wasm32-3.13",
    "/Lib/site-packages",
):
    if _path not in sys.path:
        sys.path.insert(0, _path)

if not hasattr(os, "umask"):
    _sinomo_umask_value = 0o022

    def _sinomo_umask(mask):
        global _sinomo_umask_value
        previous = _sinomo_umask_value
        _sinomo_umask_value = int(mask) & 0o777
        return previous

    os.umask = _sinomo_umask

if sys.platform == "wasi":
    try:
        import _sinomo_subprocess
        import subprocess as _subprocess

        os.umask = _sinomo_subprocess.umask
        os.chmod = _sinomo_subprocess.chmod
        os.fchmod = _sinomo_subprocess.fchmod
        os.chown = _sinomo_subprocess.chown
        os.fchown = _sinomo_subprocess.fchown
        os.lchown = _sinomo_subprocess.chown
        os.getuid = _sinomo_subprocess.getuid
        os.geteuid = _sinomo_subprocess.getuid
        os.getgid = _sinomo_subprocess.getgid
        os.getegid = _sinomo_subprocess.getgid
        os.getgroups = _sinomo_subprocess.getgroups
        os.getpid = _sinomo_subprocess.getpid
        os.getppid = _sinomo_subprocess.getppid
        os.kill = _sinomo_subprocess.kill
        os.waitpid = _sinomo_subprocess.waitpid
        os.wait = _sinomo_subprocess.wait
        os.WNOHANG = getattr(os, "WNOHANG", 1)
        os.WUNTRACED = getattr(os, "WUNTRACED", 2)
        os.WCONTINUED = getattr(os, "WCONTINUED", 8)
        os.WSTOPPED = getattr(os, "WSTOPPED", os.WUNTRACED)
        os.getpgid = _sinomo_subprocess.getpgid
        os.getpgrp = _sinomo_subprocess.getpgrp
        os.getsid = _sinomo_subprocess.getsid
        os.setpgid = _sinomo_subprocess.setpgid
        os.setpgrp = _sinomo_subprocess.setpgrp
        os.setsid = _sinomo_subprocess.setsid
        os.tcgetpgrp = _sinomo_subprocess.tcgetpgrp
        os.tcgetsid = _sinomo_subprocess.tcgetsid
        os.tcsetpgrp = _sinomo_subprocess.tcsetpgrp

        _original_subprocess_run = _subprocess.run
        _original_subprocess_popen = _subprocess.Popen
        _sinomo_popen_counter = 0

        def _sinomo_env_list(env):
            if env is None:
                merged = dict(os.environ)
            else:
                merged = dict(env)
            merged.setdefault("HOME", "/home")
            merged.setdefault("PIP_DISABLE_PIP_VERSION_CHECK", "1")
            merged.setdefault("PIP_FIND_LINKS", "/Lib/sinomo-wheels")
            if not merged.get("PYTHONHOME"):
                merged["PYTHONHOME"] = "/"
            _required_python_paths = [
                "/Lib",
                "/cross-build/wasm32-wasip1/build/lib.wasi-wasm32-3.13",
                "/Lib/site-packages",
            ]
            _python_path = [part for part in merged.get("PYTHONPATH", "").split(":") if part]
            for _path in reversed(_required_python_paths):
                if _path not in _python_path:
                    _python_path.insert(0, _path)
            merged["PYTHONPATH"] = ":".join(_python_path)
            return [f"{key}={value}" for key, value in merged.items()]

        class _SinomoPipeWriter:
            def __init__(self, path):
                self.name = path
                self.closed = False

            def close(self):
                self.closed = True

            def write(self, data):
                raise OSError("Sinomo WASI subprocess stdin pipes are file-backed and must be written before spawn")

            def flush(self):
                pass

        class _SinomoPipeReader:
            def __init__(self, path, *, encoding=None, errors=None, text=False):
                mode = "r" if text else "rb"
                self._file = open(path, mode, encoding=encoding if text else None, errors=errors if text else None)
                self.name = path

            def close(self):
                return self._file.close()

            def read(self, *args):
                return self._file.read(*args)

            def readline(self, *args):
                return self._file.readline(*args)

            def readlines(self, *args):
                return self._file.readlines(*args)

            def __iter__(self):
                return iter(self._file)

            @property
            def closed(self):
                return self._file.closed

        def _sinomo_next_pipe_path(kind):
            global _sinomo_popen_counter
            _sinomo_popen_counter += 1
            return f"/tmp/sinomo-subprocess-{_sinomo_popen_counter}-{kind}"

        def _sinomo_normalize_argv(argv):
            if isinstance(argv, (str, bytes, os.PathLike)):
                raise TypeError("string commands require shell=True, which is unsupported in the Sinomo WASI subprocess bridge")
            normalized = [os.fsdecode(value) for value in argv]
            if normalized:
                executable_name = os.path.basename(normalized[0])
                if normalized[0] == sys.executable or executable_name in ("python", "python.wasm"):
                    normalized[0] = "python"
            return normalized

        def _sinomo_shell_argv(args, executable=None):
            shell = os.fsdecode(executable) if executable is not None else "sh"
            if isinstance(args, (str, bytes, os.PathLike)):
                return [shell, "-c", os.fsdecode(args)]
            normalized = [os.fsdecode(value) for value in args]
            command = normalized[0] if normalized else ""
            return [shell, "-c", command, *normalized[1:]]

        class _SinomoPopen:
            def __init__(
                self,
                args,
                bufsize=-1,
                executable=None,
                stdin=None,
                stdout=None,
                stderr=None,
                preexec_fn=None,
                close_fds=True,
                shell=False,
                cwd=None,
                env=None,
                universal_newlines=None,
                startupinfo=None,
                creationflags=0,
                restore_signals=True,
                start_new_session=False,
                pass_fds=(),
                *,
                user=None,
                group=None,
                extra_groups=None,
                encoding=None,
                errors=None,
                text=None,
                umask=-1,
                pipesize=-1,
                process_group=None,
            ):
                unsupported = (
                    preexec_fn is not None
                    or not close_fds
                    or startupinfo is not None
                    or creationflags
                    or start_new_session
                    or pass_fds
                    or user is not None
                    or group is not None
                    or extra_groups is not None
                    or umask != -1
                    or pipesize != -1
                    or process_group is not None
                )
                if unsupported:
                    self._fallback = _original_subprocess_popen(
                        args,
                        bufsize=bufsize,
                        executable=executable,
                        stdin=stdin,
                        stdout=stdout,
                        stderr=stderr,
                        preexec_fn=preexec_fn,
                        close_fds=close_fds,
                        shell=shell,
                        cwd=cwd,
                        env=env,
                        universal_newlines=universal_newlines,
                        startupinfo=startupinfo,
                        creationflags=creationflags,
                        restore_signals=restore_signals,
                        start_new_session=start_new_session,
                        pass_fds=pass_fds,
                        user=user,
                        group=group,
                        extra_groups=extra_groups,
                        encoding=encoding,
                        errors=errors,
                        text=text,
                        umask=umask,
                        pipesize=pipesize,
                        process_group=process_group,
                    )
                    self.__dict__.update(self._fallback.__dict__)
                    return

                if shell:
                    self.args = _sinomo_shell_argv(args, executable=executable)
                else:
                    self.args = _sinomo_normalize_argv(args)
                if executable is not None and self.args and not shell:
                    self.args[0] = os.fsdecode(executable)
                    self.args = _sinomo_normalize_argv(self.args)
                self.stdin = None
                self.stdout = None
                self.stderr = None
                self.returncode = None
                self._text = bool(text or universal_newlines or encoding or errors)
                self._encoding = encoding or "utf-8"
                self._errors = errors or "strict"
                cwd = os.fsdecode(cwd if cwd is not None else os.environ.get("PWD") or os.getcwd())
                env_list = _sinomo_env_list(env)

                stdin_path = None
                if stdin == _subprocess.PIPE:
                    stdin_path = _sinomo_next_pipe_path("stdin")
                    open(stdin_path, "wb").close()
                    self.stdin = _SinomoPipeWriter(stdin_path)
                elif stdin is not None:
                    raise OSError("unsupported stdin configuration for Sinomo WASI subprocess")

                stdout_path = None
                if stdout == _subprocess.PIPE:
                    stdout_path = _sinomo_next_pipe_path("stdout")
                elif stdout is not None:
                    raise OSError("unsupported stdout configuration for Sinomo WASI subprocess")

                stderr_path = None
                stderr_to_stdout = stderr == _subprocess.STDOUT
                if stderr == _subprocess.PIPE:
                    stderr_path = _sinomo_next_pipe_path("stderr")
                elif stderr not in (None, _subprocess.STDOUT):
                    raise OSError("unsupported stderr configuration for Sinomo WASI subprocess")

                self.returncode = _sinomo_subprocess.spawn_wait_io(
                    self.args,
                    env_list,
                    cwd,
                    stdin_path,
                    stdout_path,
                    False,
                    stderr_path,
                    False,
                    stderr_to_stdout,
                )
                if stdout_path is not None:
                    if not os.path.exists(stdout_path):
                        open(stdout_path, "wb").close()
                    self.stdout = _SinomoPipeReader(
                        stdout_path,
                        encoding=self._encoding,
                        errors=self._errors,
                        text=self._text,
                    )
                if stderr_path is not None:
                    if not os.path.exists(stderr_path):
                        open(stderr_path, "wb").close()
                    self.stderr = _SinomoPipeReader(
                        stderr_path,
                        encoding=self._encoding,
                        errors=self._errors,
                        text=self._text,
                    )

            def __enter__(self):
                return self

            def __exit__(self, exc_type, value, traceback):
                for stream in (self.stdin, self.stdout, self.stderr):
                    if stream is not None:
                        stream.close()
                self.wait()
                return False

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                if timeout is not None:
                    raise OSError("Sinomo WASI subprocesses are waited synchronously at spawn time")
                return self.returncode

            def communicate(self, input=None, timeout=None):
                if input is not None or timeout is not None:
                    raise OSError("unsupported communicate configuration for Sinomo WASI subprocess")
                stdout = self.stdout.read() if self.stdout is not None else None
                stderr = self.stderr.read() if self.stderr is not None else None
                return stdout, stderr

            def kill(self):
                return None

            terminate = kill

        def _sinomo_subprocess_run(*popenargs, input=None, capture_output=False, timeout=None, check=False, **kwargs):
            if input is not None or timeout is not None:
                return _original_subprocess_run(
                    *popenargs,
                    input=input,
                    capture_output=capture_output,
                    timeout=timeout,
                    check=check,
                    **kwargs,
                )
            if capture_output:
                kwargs["stdout"] = _subprocess.PIPE
                kwargs["stderr"] = _subprocess.PIPE
            with _SinomoPopen(*popenargs, **kwargs) as process:
                stdout, stderr = process.communicate()
                returncode = process.poll()
            completed = _subprocess.CompletedProcess(popenargs[0], returncode, stdout, stderr)
            if check and returncode:
                raise _subprocess.CalledProcessError(returncode, popenargs[0], output=stdout, stderr=stderr)
            return completed

        def _sinomo_subprocess_call(*popenargs, timeout=None, **kwargs):
            return _sinomo_subprocess_run(*popenargs, timeout=timeout, **kwargs).returncode

        def _sinomo_subprocess_check_call(*popenargs, **kwargs):
            returncode = _sinomo_subprocess_call(*popenargs, **kwargs)
            if returncode:
                raise _subprocess.CalledProcessError(returncode, popenargs[0])
            return 0

        _subprocess.run = _sinomo_subprocess_run
        _subprocess.call = _sinomo_subprocess_call
        _subprocess.check_call = _sinomo_subprocess_check_call
        _subprocess.Popen = _SinomoPopen
    except Exception:
        pass
