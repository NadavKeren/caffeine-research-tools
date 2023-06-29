traces=("http://www.wikibench.eu/wiki/2007-09/wiki.1190153705.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190157306.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190160907.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190164508.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190168109.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190171710.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190175311.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190178912.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190182513.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190186114.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190189715.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190193316.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190196917.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190200518.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190204119.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190207720.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190211321.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190214922.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190218523.gz" "http://www.wikibench.eu/wiki/2007-09/wiki.1190222124.gz")

Reset_all='\033[0m'
BYellow='\033[1;33m'

i=0

for trace in "${traces[@]}";
do
    x=$i
    while [ ${#x} -ne 4 ]; 
    do    
        x="0"$x; 
    done
    echo -e "${BYellow}Downloading file ${i}: ${trace}${Reset_all}"
    wget -O "wiki.${x}.gz" ${trace}
    i=$((i+1))
done;