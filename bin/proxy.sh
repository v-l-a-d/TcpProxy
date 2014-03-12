#/bin/bash
nohup java -cp .:out/production/TcpProxy:lib/* -server -Xmx512m  -Xms512m -XX:NewSize=256m -XX:MaxNewSize=256m -XX:+UseConcMarkSweepGC com.causata.volta.proxy.TcpProxy > proxy.log 2>&1 &
