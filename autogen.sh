#!/usr/bin/env bash
( cd resources
protoc --python_out=../converters teryt.proto
)
