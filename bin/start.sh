#!/bin/sh
export SERVER_JVMFLAGS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
./zkServer.sh start-foreground
