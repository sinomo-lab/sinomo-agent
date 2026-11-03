#!/bin/sh
marker="${0%/*}/container-cancelled"
if [ "$1" = "rm" ]; then
  touch "$marker"
  exit 0
fi
while [ "$#" -gt 0 ]; do
  case "$1" in
    fake-image) shift; break ;;
    --name|--network|--cap-drop|--security-opt|--memory|--stop-timeout|--volume|--workdir|--env) shift 2 ;;
    --rm|--pull=never|--read-only|-i) shift ;;
    *) shift ;;
  esac
done
while [ ! -f "$marker" ]; do
  sleep 0.05
done
exit 0
