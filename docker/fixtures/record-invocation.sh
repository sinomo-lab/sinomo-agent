#!/bin/sh
echo invoked >> "${0%/*}/invoked"
exit 99
