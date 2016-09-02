#!/bin/bash

# Check if SABER_HOME is set
if [ -z $SABER_HOME ]; then
    echo "error: \$SABER_HOME is not set"
    exit 1
fi

# Source configuration parameters
. $SABER_HOME/scripts/saber.conf

saberProcessStart () {
	saberDirectoryExistsOrExit "$SABER_LOGDIR"
	saberDirectoryExistsOrExit "$SABER_PIDDIR"
	#
	name=$1
	saberProcessClear "$@"
	saberProcessIsRunning "$@"
	[ $? -eq 0 ] && return 1
	shift 1
	(
		# Redirect standard file descriptors to log files
		[[ -t 0 ]] && exec 0</dev/null
		[[ -t 1 ]] && exec 1>"$SABER_LOGDIR/${name}.out"
		[[ -t 2 ]] && exec 2>"$SABER_LOGDIR/${name}.err"
		# Close non-standard file descriptors
		eval exec {3..255}\>\&-
		trap '' 1 2 # Ignore HUP INT in child process
		exec "$@"
	) &
	pid=$!
	disown -h $pid
	$SABER_VERBOSE && echo "$name's pid is $pid"
	echo $pid > "$SABER_PIDDIR/$name.pid"
	return 0
}

saberProcessStop () {
	name=$1
	[ -s "$SABER_PIDDIR/$name.pid" ] && (
		pid=`cat "$SABER_PIDDIR/$name.pid"`
		kill -15 $pid >/dev/null 2>&1
		rm "$SABER_PIDDIR/$name.pid"
		return 0
	) || (
		echo "error: $SABER_PIDDIR/$name.pid not found"
		return 1
	)
}

saberProcessIsRunning () {
	name=$1
	# Check if process $name is running
	[ -s "$SABER_PIDDIR/$name.pid" ] && (
		# $SABER_VERBOSE && echo "$name.pid found"
		pid=`cat "$SABER_PIDDIR/$name.pid"`
		ps -p $pid >/dev/null 2>&1
		return $?
	) || ( 
		# unlikely
		shift 1
		t=\""$@"\"
		pgrep -lf "$t" >/dev/null 2>&1
		[ $? -eq 1 ] && return 1 || (
			echo "warning: $name is beyond our control"
			return 0
		)
	)
}

saberProcessClear () {
	name=$1
	shift 1
	# Check if $name.pid exists but process is not running
	t=\""$@"\"
	pgrep -lf "$t" >/dev/null 2>&1
	if [ \( $? -eq 1 \) -a \( -f "$PIDDIR/$name.pid" \) ]; then
		rm "$PIDDIR/$name.pid"
	fi
	return 0
}

saberProcessDone () {
	# 
	# Wait up to X seconds for measurements to be flushed in output file.
	#
	# The line to look for, printed by the PerformanceMonitor class, is:
	# 
	# [MON] Done.
	#
	name=$1
	
	F="$SABER_LOGDIR/${name}.out"
	
	[ ! -f "$F" ] && {
		echo "warning: \"$F\" not found"
		return 1
	}
	
	found=1 # Not found
	
	attempts=$SABER_WAITTIME
	length=${#attempts} # Length to control printf length
	while [ $attempts -gt 0 ]; do
		printf "\rWaiting up to %${length}ds for measurements to be flushed " $attempts
		let attempts--
		sleep 1
		cat "$F" | grep "\[MON\] Done." >/dev/null 2>&1
		if [ $? -eq 0 ]; then
			found=0
			break
		fi
	done
	echo "" # Line break
	return $found
}

saberSignalTrapped () {
	if [ -f "$SABER_TRAP" ]; then
		return 0
	else
		return 1
	fi
}

saberProcessTrap () {
	trap "saberProcessSignal" 1 2 3 8 11 16 17
}

saberProcessClearTrap () {
	trap - 1 2 3 8 11 16 17
	# Delete trap file, in exists
	rm -f "$SABER_TRAP"
}

saberProcessSignal () {
	echo "Signal received: shutting down..."
	saberShutdown
	# Delete trap file
	rm -f "$SABER_TRAP"
	exit 0
}

saberShutdown () {
	# Shutdown all running processes
	error=0
	for f in $(ls "$SABER_PIDDIR"/*.pid); do
		t=${f%.*}
		n=${t##*/}
		saberProcessStop $n
		if [ $? -ne 0 ]; then
			let error++
		fi
	done
	# Sanitise stdin
	stty sane
	return $error
}

saberProgramExists () {
	program=$1
	which $program >/dev/null 2>&1
	return $?
}

saberProgramExistsOrExit () {
	saberProgramExists "$1"
	if [ $? -ne 0 ]; then
		echo "error: $1: command not found"
		exit 1
	fi
}

saberProgramVersion () {
	version=""
	case "$1" in
	javac)
	version=`javac -version 2>&1 | awk '{ print $2 }'`
	;;
	java)
	version=`java -version 2>&1 | head -n 1 | awk -F '"' '{ print $2 }'`
	;;
	mvn)
	version=`mvn --version 2>&1 | head -n 1 | awk '{ print $3 }'`
	;;
	python)
	version=`python --version 2>&1 | awk '{ print $2 }'`
	;;
	make)
	version=`make --version 2>&1 | head -n 1 | awk '{ print $3 }'`
	;;
	gcc)
	version=`gcc -dumpversion 2>&1`
	;;
	perl)
	version=`perl -e 'print $^V;' 2>&1`
	;;
	latex)
	version=`latex -v 2>&1 | head -n 1`
	;;
	pdflatex)
	version=`pdflatex -v 2>&1 | head -n 1`
	;;
	bibtex)
	version=`bibtex -v 2>&1 | head -n 1 | awk '{ print $2 }'`
	;;
	epstopdf)
	version=`epstopdf -v 2>&1 | head -n 1 | awk '{ print $NF }'`
	;;
	gs)
	version=`gs -v 2>&1 | head -n 1`
	;;
	gnuplot)
	version=`gnuplot --version | awk '{ print $2 }'`
	;;
	*)
	version="(undefined version)"
	;;
	esac
	echo $version
}

saberSuperUser () {
	user=`id -u`
	[ $user != "0" ] && return 1
	return 0
}

saberAskToInstall () {
	P="$1"
	F="$2"
	
	OPT=$3 # Skip  installation of optional packages
	REQ=$4 # Force installation of required packages
	
	[ "$F" = "y" ] && [ $REQ -ne 0 ] && return 0 # User said  'yes'
	[ "$F" = "n" ] && [ $OPT -ne 0 ] && return 2 # User said 'skip'
	
	Q="Install package $1"
	[ "$F" = "y" ] && Q="$Q (yes/no)? " || Q="$Q (yes/no/skip)? "
	
	result=0
	echo -n "$Q"
	while true; do
		read a
		case "$a" in
		y|yes)
		result=0
		break
		;;
		n|no)
		result=1
		break
		;;
		s|skip)
		[ $F = "n" ] && {
		result=2
		break
		} || {
		echo -n "Invalid option: \"$a\". Choose 'yes' or 'no': "
		}
		;;
		*)
		[ $F = "y" ] && {
		echo -n "Invalid option: \"$a\". Choose 'yes' or 'no': "
		} || {
		echo -n "Invalid option: \"$a\". Choose 'yes', 'no', or 'skip': "
		}
		;;
		esac
	done
	
	return $result
}

saberRotateAptLog () {
	next=0
	# Are there any rotated logs?
	ls "$SABER_HOME"/apt.log.* >/dev/null 2>&1
	if [ $? -eq 0 ]; then
		files=`ls "$SABER_HOME"/apt.log.*`
		# Find latest version of apt.log.*
		for file in $files; do
			curr=`echo $file | awk '{ n = split($0, t, "."); print t[n] }'`
			[ $curr -gt $next ] && next=$curr
		done
	fi
	# Is there an apt.log file? If yes, rotate it
	if [ -f "$SABER_HOME/apt.log" ]; then
		let next++
		mv "$SABER_HOME/apt.log" "$SABER_HOME/apt.log.$next"
	fi
}

saberInstallPackage () {
	
	saberProgramExists "apt-get" || return 1
	saberProgramExists    "dpkg" || return 1
	
	package="$1"
	logfile="$2"
	
	dpkg -s "$package" >/dev/null 2>&1
	# Is package already installed?
	[ $? -eq 0 ] && return 0
	
	if [ -f "$logfile" ]; then
	# Redirect output to apt.log (here, $logfile)
	sudo apt-get -y -q --allow-unauthenticated install "$package" >>"$logfile" 2>&1
	else
	sudo apt-get -y -q --allow-unauthenticated install "$package"
	fi
	
	# Is package installed?
	dpkg -s "$package" >/dev/null 2>&1
	if [ $? -eq 1 ]; then
		if [ -f "$logfile" ]; then
		echo "error: failed to install $package (transcript written on $logfile)"
		else
		echo "error: failed to install $package"
		fi
		return 1
	fi
	
	return 0
}

saberDirectoryExistsOrExit () {
	# Check if $1 is a directory
	if [ ! -d "$1" ]; then
		echo "error: $1: directory not found"
		exit 1
	fi
	return 0
}

saberFileExistsOrExit () {
	if [ ! -f "$1" ]; then
		echo "error: $1: file not found"
		exit 1
	fi
	return 0
}

saberFileExists () {
	V="$2"
	if [ ! -f "$1" ]; then
		[ "$V" = "-v" ] && echo "warning: $1: file not found"
		return 1
	fi
	return 0
}

saberFindJar () {
	find $1 -name *.jar
}

saberOptionInSet () {
	OPT="$1"
	ARG="$2"
	shift 2
	for VAL in $@
	do
		[ "$ARG" = "$VAL" ] && return 0
	done
	echo "error: invalid option: $OPT $ARG"
	return 1
}

saberOptionIsDuplicateFigureId () {
	OPT="$1"
	ARG="$2"
	FIG=`echo "$ARG" | tr -d [a-z,A-Z]`
	shift 2
	for VAL in $@
	do
		[ "$FIG" = "$VAL" ]	&& {
		echo "error: duplicate option: $OPT $ARG"
		return 1
		}
	done
	return 0
}

saberOptionIsPositiveInteger () {
	OPT="$1"
	ARG="$2"
	# Check that opt is a number
	NUMERIC='^[0-9]+$'
	if [[ ! "$ARG" =~ $NUMERIC ]]; then
		echo "error: invalid option: $OPT must be integer"
		return 1
	fi
	# Also check that it is > 0
	if [ $ARG -le 0 ]; then
		echo "error: invalid option: $OPT must be greater than 0"
		return 1
	fi
	return 0
}

saberOptionIsInteger () {
	OPT="$1"
	ARG="$2"
	# Check that opt is a number
	NUMERIC='^[0-9]+$'
	if [[ ! "$ARG" =~ $NUMERIC ]]; then
		echo "error: invalid option: $OPT must be integer"
		return 1
	fi
	# Also check that it is > 0
	if [ $ARG -lt 0 ]; then
		echo "error: invalid option: $OPT must be greater or equal to 0"
		return 1
	fi
	return 0
}

saberOptionIsIntegerWithinRange () {
	OPT="$1"
	ARG="$2"
	MIN="$3"
	MAX="$4"
	# Check that opt is a number
	NUMERIC='^[0-9]+$'
	if [[ ! "$ARG" =~ $NUMERIC ]]; then
		echo "error: invalid option: $OPT must be integer"
		return 1
	fi
	# Also check that it is within range
	if [ \( $ARG -lt $MIN \) -o \( $ARG -gt $MAX \) ]; then
		echo "error: invalid option: $OPT must be between $MIN and $MAX"
		return 1
	fi
	return 0
}

saberOptionIsBoolean () {
	OPT="$1"
	ARG="$2"
	if [ \( "$ARG" != "true" \) -a \( "$ARG" != "false" \) ]; then
		echo "error: invalid option: $OPT must be \"true\" or \"false\""
		return 1
	fi
	return 0
}

saberOptionIsAlpha () {
	OPT="$1"
	ARG="$2"
	ALPHA='^[a-zA-Z]+$'
	if [[ ! "$ARG" =~ $ALPHA ]]; then
		echo "error: invalid option: $OPT must contains only alphabetical chars"
		return 1
	fi
	return 0
}

saberOptionIsValidFigureId () {
	OPT="$1"
	ARG="$2"
	# Check that opt is a number
	NUMERIC='^[0-9]+$'
	if [[ "$ARG" =~ $NUMERIC ]]; then
		# Check that opt is a valid figure id
		FIG=`printf "%02d" $ARG`
		for VAL in "01" "07" "08" "09" "10" "11" "12" "13" "14" "15" "16"; do
			[ "$FIG" = "$VAL" ] && return 0
		done
	else 
		# Opt is not numeric; is it a sub-figure?
		for XPR in '10[a|b]' '11[a|b]' '12[a|b|c]' '13[a|b|c]'; do
			[[ "$ARG" =~ $XPR ]] && return 0
		done
	fi
	echo "error: invalid option: $OPT $ARG"
	return 1
}

saberParseSysConfArg () {
	
	result=0
	
	case "$1" in
	--execution-mode)
	saberOptionInSet "$1" "$2" "cpu" "gpu" "hybrid" || result=1
	SABER_CONF_EXECUTIONMODE="$2"
	;;
	--number-of-worker-threads)
	saberOptionIsPositiveInteger "$1" "$2" || result=1
	SABER_CONF_NUMWORKERTHREADS="$2"
	;;
	--number-of-result-slots)
	saberOptionIsPositiveInteger "$1" "$2" || result=1
	SABER_CONF_NUMRESULTSLOTS="$2"
	;;
	--scheduling-policy)
	saberOptionInSet "$1" "$2" "hls" "fcfs" "static" || result=1
	SABER_CONF_SCHEDULINGPOLICY="$2"
	;;
	--switch-threshold)
	saberOptionIsPositiveInteger "$1" "$2" || result=1
	SABER_CONF_SWITCHTHRESHOLD="$2"
	;;
	--number-of-partial-windows)
	saberOptionIsPositiveInteger "$1" "$2" || result=1
	SABER_CONF_NUMPARTIALWINDOWS="$2"
	;;
	--circular-buffer-size)
	saberOptionIsPositiveInteger "$1" "$2" || result=1
	SABER_CONF_CIRCULARBUFFERSIZE="$2"
	;;
	--intermediate-buffer-size)
	saberOptionIsPositiveInteger "$1" "$2" || result=1
	SABER_CONF_INTERMEDIATEBUFFERSIZE="$2"
	;;
	--hash-table-size)
	saberOptionIsPositiveInteger "$1" "$2" || result=1
	SABER_CONF_HASHTABLESIZE="$2"
	;;
	--throughput-monitor-interval)
	saberOptionIsPositiveInteger "$1" "$2" || result=1
	SABER_CONF_THROUGHPUTMONITORINTERVAL="$2"
	;;
	--performance-monitor-interval)
	saberOptionIsPositiveInteger "$1" "$2" || result=1
	SABER_CONF_PERFORMANCEMONITORINTERVAL="$2"
	;;
	--number-of-upstream-queries)
	saberOptionIsPositiveInteger "$1" "$2" || result=1
	SABER_CONF_UPSTREAMQUERIES="$2"
	;;
	--pipeline-depth)
	saberOptionIsPositiveInteger "$1" "$2" || result=1
	SABER_CONF_PIPELINEDEPTH="$2"
	;;
	--enable-latency-measurements)
	saberOptionIsBoolean "$1" "$2" || result=1
	SABER_CONF_MEASURELATENCY="$2"
	;;
	--launch-web-server)
	saberOptionIsBoolean "$1" "$2" || result=1
	SABER_CONF_LAUNCHWEBSERVER="$2"
	;;
	--experiment-duration)
	saberOptionIsPositiveInteger "$1" "$2" || result=1
	SABER_CONF_EXPERIMENTDURATION="$2"
	;;
	*)
	# Unknown option
	result=2
	;;
	esac
	
	return $result
}

saberSetSysArgs () {
	
	[ -n "$SABER_CONF_EXECUTIONMODE" ] && \
	SYSARGS="$SYSARGS --execution-mode $SABER_CONF_EXECUTIONMODE"
	
	[ -n "$SABER_CONF_NUMWORKERTHREADS" ] && \
	SYSARGS="$SYSARGS --number-of-worker-threads $SABER_CONF_NUMWORKERTHREADS"
	
	[ -n "$SABER_CONF_NUMRESULTSLOTS" ] && \
	SYSARGS="$SYSARGS --number-of-result-slots $SABER_CONF_NUMRESULTSLOTS"
	
	[ -n "$SABER_CONF_SCHEDULINGPOLICY" ] && \
	SYSARGS="$SYSARGS --scheduling-policy $SABER_CONF_SCHEDULINGPOLICY"
	
	[ -n "$SABER_CONF_SWITCHTHRESHOLD" ] && \
	SYSARGS="$SYSARGS --switch-threshold $SABER_CONF_SWITCHTHRESHOLD"
	
	[ -n "$SABER_CONF_NUMPARTIALWINDOWS" ] && \
	SYSARGS="$SYSARGS --number-of-partial-windows $SABER_CONF_NUMPARTIALWINDOWS"
	
	[ -n "$SABER_CONF_CIRCULARBUFFERSIZE" ] && \
	SYSARGS="$SYSARGS --circular-buffer-size $SABER_CONF_CIRCULARBUFFERSIZE"
	
	[ -n "$SABER_CONF_INTERMEDIATEBUFFERSIZE" ] && \
	SYSARGS="$SYSARGS --intermediate-buffer-size $SABER_CONF_INTERMEDIATEBUFFERSIZE"
	
	[ -n "$SABER_CONF_HASHTABLESIZE" ] && \
	SYSARGS="$SYSARGS --hash-table-size $SABER_CONF_HASHTABLESIZE"
	
	[ -n "$SABER_CONF_THROUGHPUTMONITORINTERVAL" ] && \
	SYSARGS="$SYSARGS --throughput-monitor-interval $SABER_CONF_THROUGHPUTMONITORINTERVAL"
	
	[ -n "$SABER_CONF_PERFORMANCEMONITORINTERVAL" ] && \
	SYSARGS="$SYSARGS --performance-monitor-interval $SABER_CONF_PERFORMANCEMONITORINTERVAL"
	
	[ -n "$SABER_CONF_UPSTREAMQUERIES" ] && \
	SYSARGS="$SYSARGS --number-of-upstream-queries $SABER_CONF_UPSTREAMQUERIES"
	
	[ -n "$SABER_CONF_PIPELINEDEPTH" ] && \
	SYSARGS="$SYSARGS --pipeline-depth $SABER_CONF_PIPELINEDEPTH"
	
	[ -n "$SABER_CONF_MEASURELATENCY" ] && \
	SYSARGS="$SYSARGS --enable-latency-measurements $SABER_CONF_MEASURELATENCY"
	
	[ -n "$SABER_CONF_LAUNCHWEBSERVER" ] && \
	SYSARGS="$SYSARGS --launch-web-server $SABER_CONF_LAUNCHWEBSERVER"
	
	[ -n "$SABER_CONF_EXPERIMENTDURATION" ] && \
	SYSARGS="$SYSARGS --experiment-duration $SABER_CONF_EXPERIMENTDURATION"
	
	return 0
}

saberNextMajorVersion () {
	#
	# Iterate over experiments/figure-*/* directories and 
	# find the largest major version.
	#
	# TODO
	# 
	# The function is tied to the current directory layout, 
	# assuming there are up to 3 sub-directories with names 
	# "/a", "/b" and "/c".
	#
	result=0
	
	for N in "01" "07" "08" "09" "10" "11" "12" "13" "14" "15" "16"; do
		
		# Search Figure-* directories
		F="$SABER_HOME/scripts/experiments/figure-$N"
		
		if [ -d "$F" ]; then # very likely
			
			# Get current major version
			m=$(saberCurrentFigureVersion "$F" 0)
			
			# Find max
			[ $result -lt $m ] && result=$m
			
			# Repeat process for sub-figure directories, if any
			for sub in "a" "b" "c"; do
			 
				m=$(saberCurrentFigureVersion "$F/$sub" 0)
				[ $result -lt $m ] && result=$m
			
			done
		fi
	done
	# Bump version
	let result++
	echo $result
}

saberCurrentFigureVersion () {
	# Find the largest major (or minor) version, 
	# indexed by $2, in directory $1
	
	F="$1"
	V="$2" # Index, 0 or 1
	
	result=0
	
	# Check if results/ directory exists
	if [ -d "$F/results" ]; then
		
		# List results
		children=`ls "$F/results"`
		
		major=0
		minor=0
		
		if [ ! -z "$children" ]; then
			
			for child in $children; do
				# Extract major.minor from child directory
				# a=( ${last//./ } )
				# result=${a[$V]}
				M=`echo $child | awk '{ split($0, t, "."); print t[1] }'`
				m=`echo $child | awk '{ split($0, t, "."); print t[2] }'`
				
				[ $major -lt $M ] && major=$M
				[ $minor -lt $m ] && minor=$m
			done
			# Which one to return?
			[ $V -eq 0 ] && result=$major || result=$minor
		fi
	fi
	echo $result
}

saberExperimentStoreResults () {
	
	ALIAS="$1"
	DESTINATION="$2" # Result directory
	KEY="$3"
	
	OUT="$SABER_LOGDIR/$ALIAS.out"
	ERR="$SABER_LOGDIR/$ALIAS.err"
	
	saberFileExists "$OUT" "-v" && cp "$OUT" "$DESTINATION/$KEY.out"
	saberFileExists "$ERR" "-v" && cp "$ERR" "$DESTINATION/$KEY.err" && cat "$ERR" >>"$DESTINATION/error.log"
	
	return 0
}

saberExperimentLogIsValid () {
	LOG="$1"
	saberFileExists "$LOG" || return 1
	cat "$LOG" | grep "\[MON\] Done." >/dev/null 2>&1
	return $?
}

saberLogRunCommand () {
	
	echo "# " >>"$SABER_HOME/run.log"
	
	echo "# Java command invoked on `uname -n` running `uname -s` on `date`" >>"$SABER_HOME/run.log"
	
	echo "# " >>"$SABER_HOME/run.log"
	echo "$1" >>"$SABER_HOME/run.log"
	
	return 0
}
