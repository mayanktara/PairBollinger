d=2011-02-01
while [ "$d" != 2019-12-31 ]; do
  echo $d
  d=$(date -I -d "$d + 1 day")
  python3 test_dailyPnl_setwise.py $d
  cp traded_pairs.csv marchIteration/traded_pairs_$d.csv
  mv traded_pairs.csv traded_pairs_last.csv
  mv traded_symbols.txt traded_symbols_last.txt
  rm final_symbols.txt
done
