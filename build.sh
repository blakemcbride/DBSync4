#
rm -rf build
mkdir build
find src -name '*.java' |xargs -t javac -d build
cd build
jar cfm DBSync4.jar ../MANIFEST.MF dbsync


