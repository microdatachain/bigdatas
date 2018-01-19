#!/bin/sh
java -Xms2048m -Xmx2048m -Xmn1024m -XX:PermSize=128M -XX:MaxPermSize=256M -Djava.net.preferIPv4Stack=true  -jar microdatachain-rt-${microdatachain_version}.jar \$1


