#!/bin/bash

#curdate=`date +"%d/%b/%Y:%H:%M:%S" -d -$@min`
fixdate=`date +%s -d -5min`

# Top 10 сайтов по количеству запросов
awk -F\" '{print $2}' /var/log/httpd/access_log | awk -F / '{print $2}' | grep -E "^\w{2}" | sort -r | uniq -c | sort -hr | head

# Top 10 ip адресов для каждого из сайтов за последние N-минут
for i in $(date --date="$(awk '{print $4}' /var/log/httpd/access_log | awk -F "[" '{print $2}')" +"%s")
    do
    echo $fixdate
    echo $i
        if [[ "$i" -le "$fixdate" ]]
        then
            echo true
            for n in $(awk -F\" '{print $2}' /var/log/httpd/access_log | awk -F / '{print $2}' | grep -E "^\w{2}" | sort -r | uniq | sort -hr | head)
                do
                    echo -n ${n}': ' && grep ${n} /var/log/httpd/access_log | awk -F "-" '{print $1}' | grep -E "^\w{2}" | sort -n | uniq | sort -hr | head
                done
        fi
   done