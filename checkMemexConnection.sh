#! /bin/bash

# If this is sourced, then the exit, below, will terminate the parent script.
# This is a feature, not a bug.

FOUND=`fgrep tun0: /proc/net/dev`
if  [ -n "$FOUND" ] ; then
  echo "A tunnel is present, assuming it leads to the Memex cluster."
else
  echo "No tunnel found, exiting"
  exit 1
fi
