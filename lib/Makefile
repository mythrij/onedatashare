GET=curl -OJs

define g
	@echo Fetching $@...
	@$(GET) 
endef

JGLOBUS=myproxy-2.0.6.jar gss-2.0.6.jar ssl-proxies-2.0.6.jar

.PHONY: all clean

all: netty-all-5.0.0.Alpha1.jar jargon-core-3.3.1.1.jar \
	junit-4.12-beta-1.jar $(JGLOBUS) jsch-0.1.51.jar \
	jsoup-1.8.1.jar
netty-all-5.0.0.Alpha1.jar: commons-logging-1.2.jar log4j-1.2.17.jar
	$g https://search.maven.org/remotecontent?filepath=io/netty/netty-all/5.0.0.Alpha1/netty-all-5.0.0.Alpha1.jar
commons-logging-1.2.jar:
	$g https://search.maven.org/remotecontent?filepath=commons-logging/commons-logging/1.2/commons-logging-1.2.jar
jargon-core-3.3.1.1.jar:
	$g https://ci-dev.renci.org/nexus/content/groups/public/org/irods/jargon/jargon-core/3.3.1.1/jargon-core-3.3.1.1.jar
junit-4.12-beta-1.jar: hamcrest-core-1.3.jar
	$g https://search.maven.org/remotecontent?filepath=junit/junit/4.12-beta-1/junit-4.12-beta-1.jar
hamcrest-core-1.3.jar:
	$g https://search.maven.org/remotecontent?filepath=org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar
myproxy-2.0.6.jar: bouncycastle-jce-jdk13-112.jar
	$g https://search.maven.org/remotecontent?filepath=org/jglobus/myproxy/2.0.6/myproxy-2.0.6.jar
log4j-1.2.17.jar:
	$g https://search.maven.org/remotecontent?filepath=log4j/log4j/1.2.17/log4j-1.2.17.jar
jsch-0.1.51.jar:
	$g https://search.maven.org/remotecontent?filepath=com/jcraft/jsch/0.1.51/jsch-0.1.51.jar
bouncycastle-jce-jdk13-112.jar:
	$g https://search.maven.org/remotecontent?filepath=bouncycastle/bouncycastle-jce-jdk13/112/bouncycastle-jce-jdk13-112.jar
gss-2.0.6.jar:
	$g https://search.maven.org/remotecontent?filepath=org/jglobus/gss/2.0.6/gss-2.0.6.jar
ssl-proxies-2.0.6.jar:
	$g https://search.maven.org/remotecontent?filepath=org/jglobus/ssl-proxies/2.0.6/ssl-proxies-2.0.6.jar
jsoup-1.8.1.jar:
	$g https://search.maven.org/remotecontent?filepath=org/jsoup/jsoup/1.8.1/jsoup-1.8.1.jar
clean:
	@echo Do not use target clean, use distclean instead.
	@exit 1
distclean:
	@echo Cleaning dependencies...
	@$(RM) *.jar
