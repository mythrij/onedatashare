PROJECT = stork
PACKAGES = stork stork/util stork/module

CLASSPATH = '.:lib/*:lib/cog/*:build'
JFLAGS = -g -cp $(CLASSPATH) -verbose
JC = javac
JAVA = java
JAR = jar

.PHONY: all install clean init release
.SUFFIXES: .java .class

JAVASRCS = $(wildcard $(PACKAGES:%=%/*.java))
CLASSES = $(JAVASRCS:%.java=build/%.class)

all: init $(CLASSES) $(JARNAME)

build:
	mkdir -p build

$(JARNAME): $(CLASSES)
	$(JAR) cf $(PROJECT).jar -C build . -C lib/EXTRACTED .
	cp $(PROJECT).jar bin/

build/%.class: %.java | build
	$(JC) $(JFLAGS) -d build $<

init: | build

release: $(PROJECT).tar.gz

$(PROJECT).tar.gz: all
	tar czf $(PROJECT).tar.gz bin libexec --exclude='*/CVS'

clean:
	$(RM) -rf build $(PROJECT).jar $(PROJECT).tar.gz
