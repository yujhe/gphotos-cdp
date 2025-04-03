#!/bin/bash

# Read profile from .profile and save to PROFILE variable
PROFILE_ARG=""
PROFILE_DIR=""
if [ -f .profile ]; then
    PROFILE_ARG="-profile"
    PROFILE_DIR="$(cat .profile)"
    kill $(ps aux | grep $PROFILE_DIR | grep "Google Chrome.app" | awk '{print $2}') > /dev/null 2>&1
fi

set -e
go run . -workers 2 -loglevel ${LOGLEVEL:-info} $PROFILE_ARG $PROFILE_DIR -dldir photos -headless | tee >(sed $'s/\033[[][^A-Za-z]*[A-Za-z]//g' > sync.log)