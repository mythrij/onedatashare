PROJECT = stork
PACKAGES = stork stork/util stork/module

CLASSPATH = '.:lib/EXTRACTED/:build'
JFLAGS = -J-Xms512m -J-Xmx512m -g -cp $(CLASSPATH) -verbose -Xlint:unchecked
JARFLAGS = -J-Xms512m -J-Xmx512m
JC = javac
JAVA = java
JAR = jar $(JARFLAGS)

.PHONY: all install clean init release
.SUFFIXES: .java .class

JAVASRCS = $(wildcard $(PACKAGES:%=%/*.java))
CLASSES = $(JAVASRCS:%.java=build/%.class)

all: init lib/EXTRACTED $(CLASSES) $(PROJECT).jar

build:
	mkdir -p build

$(PROJECT).jar: $(CLASSES)
	$(JAR) cf $(PROJECT).jar -C build . -C lib/EXTRACTED .
	cp $(PROJECT).jar bin/

build/%.class: %.java | build
	$(JC) $(JFLAGS) -d build $<

init: | build

release: $(PROJECT).tar.gz

$(PROJECT).tar.gz: $(PROJECT).jar
	cp $(PROJECT).jar bin/
	tar czf $(PROJECT).tar.gz bin libexec --exclude='*/CVS' \
		--transform 's,^,$(PROJECT)/,'

# FIXME: This is a bad hack.
lib/EXTRACTED:
	cd lib && ./extract.sh

clean:
	$(RM) -rf build $(PROJECT).jar $(PROJECT).tar.gz
