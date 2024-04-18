#export my_dataframe="$(python3 load_dataframe.py)"
#echo $my_dataframe
for lookup in 5 10 20 40
do
	for moving in 6 12 24 48
	do
		for Bentry in 2.5
		do
			for Bexit in 0.5 1
			do
				for odays in 1 3
				do
					for threshold in 0.4
					do
						echo $lookup $moving $Bentry $Bexit $odays $threshold
						d=2011-02-01
						while [ "$d" != 2019-12-31 ]; do
  							echo $d
  							d=$(date -I -d "$d + 1 day")
							python3 test_dailyPnl_setwise_argument.py $d $lookup $moving $Bentry $Bexit $odays $threshold
  							cp traded_pairs.csv marchIteration/traded_pairs_$d.csv
  							mv traded_pairs.csv traded_pairs_last.csv
  							mv traded_symbols.txt traded_symbols_last.txt
  							rm final_symbols.txt
						done
						mv pnlDaily.csv pnl/pnlDaily_$lookup"_"$moving"_"$Bentry"_"$Bexit"_"$odays"_"$threshold.csv
						mv trade_completed.csv pnl/trade_completed_$lookup"_"$moving"_"$Bentry"_"$Bexit"_"$odays"_"$threshold.csv
						rm traded_symbols_last.txt
						rm pairs.csv
						rm final_symbols.txt
						cp ../trade_completed.csv ./
						cp ../traded_pairs_last.csv ./

					done
				done
			done
		done
	done
done

#d=2011-02-01
#while [ "$d" != 2019-12-31 ]; do
  #echo $d
  #d=$(date -I -d "$d + 1 day")
  #python3 test_dailyPnl_setwise.py $d
  #cp traded_pairs.csv marchIteration/traded_pairs_$d.csv
  #mv traded_pairs.csv traded_pairs_last.csv
  #mv traded_symbols.txt traded_symbols_last.txt
  #rm final_symbols.txt
#done

