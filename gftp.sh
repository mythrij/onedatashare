#!/bin/sh
#java -Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog -Dorg.apache.commons.logging.simplelog.defaultlog=debug -cp stork.jar stork.module.StorkGridFTPModule $@
java -Xmx2m -Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog -Dorg.apache.commons.logging.simplelog.defaultlog=error -cp stork.jar stork.module.gridftp.GridFTPSession $@
