#!/bin/bash
usage="Usage: ./$(basename "$0") [-options]
		(to execute xAdDemo.jar)
where options include:
		-h  show this help text 
		-i  set input directory	(for example- /home/spark/Desktop/xad/in/) 
		-o  set output directory (for example- /home/spark/Desktop/xad/out/)
		-l  set log file (for example- /home/spark/Desktop/xad/in/logs/etl.log)
		-p  set Parallelism (default: 5)"

MYDIR="$(dirname "$(readlink -f "$0")")"
parallel=5

while getopts hi:o:p:l: option; do
 	case "$option" in

		i)input=$OPTARG;;
		o)output=$OPTARG;;
		p)parallel=$OPTARG;;
		l)log=$OPTARG;;
		:) printf "missing argument for -%s\n" "$OPTARG" >&2
			echo "$usage" >&2
			exit 1;;
		h)echo "$usage" >&2
			exit 1;;
	esac
done
java -jar $MYDIR/xAdDemo.jar $input $output $parallel $log
shift $((OPTIND - 1))
