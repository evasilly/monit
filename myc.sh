#!/bin/bash

echo -n mysql -udbuser -pdbpassword -e "show full processlist\G" """
        """| grep -E "Command:" | grep -iv "sleep\|daemon" | wc -l