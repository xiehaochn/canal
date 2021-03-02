#!/bin/bash

current_path=`pwd`
case "`uname`" in
    Linux)
		bin_abs_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_abs_path=`cd $(dirname $0); pwd`
		;;
esac
base=${bin_abs_path}/..
atlas_conf=$base/conf/atlas_mysql_import.properties
logback_configurationFile=$base/conf/logback.xml
export LANG=en_US.UTF-8
export BASE=$base
if [ -f $base/bin/atlas.pid ] ; then
	echo "found atlas.pid , Please run stop.sh first ,then startup.sh" 2>&2
    exit 1
fi

if [ ! -d $base/logs/atlas ] ; then
	mkdir -p $base/logs/atlas
fi

## set java path
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi

str=`file -L $JAVA | grep 64-bit`
if [ -n "$str" ]; then
	JAVA_OPTS="-server -Xms2048m -Xmx3072m -Xmn1024m -XX:SurvivorRatio=2 -XX:PermSize=96m -XX:MaxPermSize=256m -Xss256k -XX:-UseAdaptiveSizePolicy -XX:MaxTenuringThreshold=15 -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:+UseFastAccessorMethods -XX:+UseCMSInitiatingOccupancyOnly -XX:+HeapDumpOnOutOfMemoryError"
else
	JAVA_OPTS="-server -Xms1024m -Xmx1024m -XX:NewSize=256m -XX:MaxNewSize=256m -XX:MaxPermSize=128m "
fi

JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8"
ATLAS_OPTS="-DappName=atlas-import-mysql -Dlogback.configurationFile=$logback_configurationFile -Datlas.conf=$atlas_conf"

if [ -e $atlas_conf -a -e $logback_configurationFile ]
then

	for i in $base/lib/*;
		do CLASSPATH=$i:"$CLASSPATH";
	done
 	CLASSPATH="$base/conf:$CLASSPATH";

 	echo "cd to $bin_abs_path for workaround relative path"
  	cd $bin_abs_path

  echo LOG CONFIGURATION : $logback_configurationFile
	echo atlas conf : $atlas_conf
	echo CLASSPATH :$CLASSPATH
	$JAVA $JAVA_OPTS $ATLAS_OPTS -classpath .:$CLASSPATH com.atlas.mysql.bridge.launcher.AtlasImportMysql 1>>$base/logs/atlas/atlas_stdout.log 2>&1 &
  echo $! > $base/bin/atlas.pid

	echo "cd to $current_path for continue"
  	cd $current_path
else
	echo "atlas conf("$atlas_conf") OR log configration file($logback_configurationFile) is not exist,please create then first!"
fi
