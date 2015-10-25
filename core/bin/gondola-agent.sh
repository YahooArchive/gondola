#!/bin/bash

export CLASSPATH_PREFIX=target/classes
sh target/appassembler/bin/gondola-agent.sh $*
