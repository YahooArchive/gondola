#!/bin/sh
if [ "x$1" = "xstart" ] ; then
  jps | grep GondolaCommand | sort -n | cut -d" " -f 1 | xargs -n 1 -I % jcmd % JFR.start
elif [ "x$1" = "xstop" -a "x$2" != "x" ] ; then
  echo "Stop recording $2 "
  jps | grep GondolaCommand | sort -n | cut -d" " -f 1 | xargs -n 1 -I % jcmd % JFR.stop recording=$2 filename=gondola.recording.%.$2.jfr
else
  echo "Usage: $0 start"
  echo "Usage: $0 stop recordingNumber filename"
fi

