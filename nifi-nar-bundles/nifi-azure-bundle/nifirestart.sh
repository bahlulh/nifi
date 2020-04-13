set -e

export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/
~/Downloads/nifi-1.11.4/bin/nifi.sh stop
rm ~/Downloads/nifi-1.11.4/logs/*.log
sudo cp nifi-azure-nar/target/nifi-azure-nar*.nar ~/Downloads/nifi-1.11.4/lib/
~/Downloads/nifi-1.11.4/bin/nifi.sh start