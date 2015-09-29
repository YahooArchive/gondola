#!/bin/sh
PWD=`pwd`
JMETER_SRC=http://apache.cs.utah.edu//jmeter/binaries/apache-jmeter-2.13.tgz
JMETER_PLUGIN_SRC=http://jmeter-plugins.org/downloads/file/JMeterPlugins-Standard-1.2.1.zip
JMETER_FILE=$(basename $JMETER_SRC)
JMETER_PLUGIN_FILE=$(basename $JMETER_PLUGIN_SRC)
TMP_DIR=tmp
TARGET_DIR=${JMETER_FILE%.*}

# configurations
sizes="128 256 512 1024 4096 8092 16384 32768"
threads="1 2 4 8 16 32 64"
default_thread=8
default_size=128
if [ "x$target_host" = "x" ]; then
  target_host=localhost
fi

if [ "x$target_port" = "x" ]; then
  target_port=1099
fi

if [ "x$duration" = "x" ]; then
  duration=30
fi
data_file=/tmp/data
######################################################################################
# generate data pack
for size in $sizes; do
  awk -v size=$size 'BEGIN{printf "c ";while(size--)printf "a"}' > $data_file.$size
done
####################


PARAM=""
PERF_TEST=n
for i in $*; do
  if [ $i = "-n" ]; then
    PARAM="$PARAM -n"
  elif [ $i = "perf" ]; then
    PERF_TEST=y
  fi
done

if [ ! -d $TMP_DIR/$TARGET_DIR ]; then
  echo "fetch jmeter binaries";
  wget -P $TMP_DIR $JMETER_SRC
  wget -P $TMP_DIR $JMETER_PLUGIN_SRC
  cd $TMP_DIR
  tar zxf $JMETER_FILE
  unzip -qo $JMETER_PLUGIN_FILE -d $TARGET_DIR
  cd ..
fi

if [ $PERF_TEST = "y" ]; then
  for thread in $threads; do
    bin/gondola-test.sh start
    echo "Run Jmeter test for $thread threads";
    $TMP_DIR/$TARGET_DIR/bin/jmeter -JTargetHost=$target_host -JTargetPort=$target_port -Jthreads=$thread -Jduration=$duration -JCsvData=$data_file.$default_size -n -t src/test/jmeter/gondola_performance.jmx -l artifacts/jmeter/gondola-t${thread}-s${default_size}.jtl
    bin/gondola-test.sh stop
  done

  for size in $sizes; do
    echo "Run Jmeter test for $size size with $default_thread ";
    bin/gondola-test.sh start
    $TMP_DIR/$TARGET_DIR/bin/jmeter -JTargetHost=$target_host -JTargetPort=$target_port -Jthreads=$default_thread -Jduration=$duration -JCsvData=$data_file.$size -n -t src/test/jmeter/gondola_performance.jmx -l artifacts/jmeter/gondola-t${default_thread}-s${size}.jtl
    bin/gondola-test.sh stop
  done
else
  echo "Waiting for the member to become the leader...."
  echo "F" | nc $target_host $target_port
  echo "Ready"

  $TMP_DIR/$TARGET_DIR/bin/jmeter -JTargetHost=$target_host -JTargetPort=$target_port -Jthreads=$default_thread -Jduration=600 -JCsvData=/tmp/data.128  -t src/test/jmeter/gondola_performance.jmx -l gondola.jtl $PARAM
fi
