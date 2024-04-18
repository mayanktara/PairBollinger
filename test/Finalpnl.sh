cat trade_completed.csv | awk -F',' '{sum=sum+(($5-$3)*$9)+(($4-$6)*$10);} END {print sum;}'
