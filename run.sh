
USAGE="usage: ./run.sh [class name]"

MVN="/home/$USER/.m2/repository"

# LOG4J="${MVN}/log4j/log4j/1.2.12/log4j-1.2.12.jar"

JETTYSERVER="${MVN}/org/eclipse/jetty/jetty-server/9.0.0.v20130308/jetty-server-9.0.0.v20130308.jar"
JETTYUTIL="${MVN}/org/eclipse/jetty/jetty-util/9.0.0.v20130308/jetty-util-9.0.0.v20130308.jar"
JETTYHTTP="${MVN}/org/eclipse/jetty/jetty-http/9.0.0.v20130308/jetty-http-9.0.0.v20130308.jar"
JETTYIO="${MVN}/org/eclipse/jetty/jetty-io/9.0.0.v20130308/jetty-io-9.0.0.v20130308.jar"

JACKSONCORE="${MVN}/com/fasterxml/jackson/core/jackson-core/2.1.4/jackson-core-2.1.4.jar"
JACKSONBIND="${MVN}/com/fasterxml/jackson/core/jackson-databind/2.1.4/jackson-databind-2.1.4.jar"
JACKSONANNOTATIONS="${MVN}/com/fasterxml/jackson/core/jackson-annotations/2.1.4/jackson-annotations-2.1.4.jar"

JAVAX="${MVN}/javax/servlet/javax.servlet-api/3.1.0/javax.servlet-api-3.1.0.jar"

GUAVA="${MVN}/com/google/guava/guava/20.0/guava-20.0.jar"

TESTS="target/test-classes"

if [ ! -f "target/saber-0.0.1-SNAPSHOT.jar" ]; then
        echo "error: target/saber-0.0.1-SNAPSHOT.jar not found. Try 'build.sh' first"
        exit 1
fi

if [ ! -f ${LOG4J} ]; then
        echo "error: ${LOG4J} not found"
        exit 1
fi

if [ ! -d ${TESTS} ]; then
        echo "error: ${TESTS} not found"
        exit 1
fi

# Set classpath
JCP="."
JCP="${JCP}:target/saber-0.0.1-SNAPSHOT.jar"
# JCP="${JCP}:${LOG4J}"
JCP="${JCP}:${JETTYSERVER}:${JETTYUTIL}:${JETTYHTTP}:${JETTYIO}"
JCP="${JCP}:${JACKSONCORE}:${JACKSONBIND}:${JACKSONANNOTATIONS}"
JCP="${JCP}:${JAVAX}"
JCP="${JCP}:${GUAVA}"
JCP="${JCP}:${TESTS}"

# OPTS="-Xloggc:test-gc.out"
OPTS="-server -XX:+UseConcMarkSweepGC -XX:NewRatio=2 -XX:SurvivorRatio=16 -Xms52g -Xmx52g"

if [ $# -lt 1 ]; then
        echo "error: unspecified application class"
else
        CLASS=$1
        shift 1
fi

CLASSFILE="${TESTS}/`echo ${CLASS} | tr '.' '/'`.class"

if [ ! -f ${CLASSFILE} ]; then
        echo "error: ${CLASSFILE} not found"
        exit 1
fi


java $OPTS -cp $JCP $CLASS $@ 


echo "Done."
echo "Bye."

exit 0

