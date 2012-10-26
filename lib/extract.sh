#/bin/sh

mkdir -p EXTRACTED
cd EXTRACTED

for i in ../*.jar; do
  jar xvf $i
done

for i in ../cog/*.jar; do
  jar xvf $i
done

rm -rf META-INF/ stylesheet.css log4j.properties
