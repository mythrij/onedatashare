CLASSPATH = '.:lib/*:lib/cog/*:build'
JFLAGS = -g -cp $(CLASSPATH) -verbose
JC = javac
JAVA = java
JAR = jar

PACKAGES = stork stork/util stork/module
JARNAME = stork.jar

.PHONY: all install clean init
.SUFFIXES: .java .class

JAVASRCS = $(wildcard $(PACKAGES:%=%/*.java))
CLASSES = $(JAVASRCS:%.java=build/%.class)

all: init $(CLASSES) $(JARNAME)

build:
	mkdir -p build

$(JARNAME): $(CLASSES)
	$(JAR) cf $(JARNAME) -C build . -C lib/EXTRACTED .
	cp stork.jar bin/

build/%.class: %.java | build
	$(JC) $(JFLAGS) -d build $<

init: | build

clean:
	$(RM) -rf build $(JARNAME)
