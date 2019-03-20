#!/bin/sh

# Corgi Home
case "`uname`" in
    Linux)
		bin_absolute_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_absolute_path=`cd $(dirname $0); pwd`
		;;
esac
corgi_home=${bin_absolute_path}/..

export CORGI_HOME=$corgi_home

# Enviroment Var
source $corgi_home/conf/corgi-env.sh

# Java
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi

# 检查 pid 文件
if [ -f $corgi_home/bin/corgi.pid ] ; then
	echo "错误：服务已启动，请先停止之前的服务" 2>&2
    exit 1
fi

# 启动服务
mkdir -p $corgi_home/logs
$JAVA -jar corgi-web.jar --spring.profiles.active=prod --spring.config.location=file:$corgi_home/conf/application.yml 1>>$corgi_home/logs/corgi.log 2>&1 &
echo $! > $corgi_home/bin/corgi.pid