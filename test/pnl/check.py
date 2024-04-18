import sys
import os

arg1=sys.argv[1]
arg2=sys.argv[2]
#print(arg)

string1=str(arg1)
string2=str(arg2)


minpnl=0
maxpnl=0
cummPnl=0

with open(string1, "r") as filestream:
    pnl=[0]
    finPnl=[0]
    i=0
    #minpnl=0
    #maxpnl=0
    #cummPnl=0
    for line in filestream:
        cummPnl =int(line.split(",")[1])
        ele=cummPnl-int(finPnl[i])
        minpnl=min(ele,minpnl)
        maxpnl=max(ele,maxpnl)
        print(ele)
        pnl.append(ele)
        finPnl.append(cummPnl)
        i=i+1
    #print("minpnl:",minpnl,"maxpnl:",maxpnl)
    n = len(finPnl)
    drawdown = 0
    for i in range(n-1):
        for j in range(i+1,n):
            diff=finPnl[i]-finPnl[j]
            if diff > 0:
                drawdown =max(drawdown,diff)

    print("minpnl:",minpnl,"maxpnl:",maxpnl,"drawdown:",drawdown,"totalPnl:",cummPnl)

minTradePnl=0
maxTradePnl=0
positiveTrades=0
negativeTrades=0

with open(string2, "r") as filestream2:
    k=0
    #minTradePnl=0
    #maxTradePnl=0
    #positiveTrades=0
    #negativeTrades=0
    for line in filestream2:
        if k!=0:
            symbol1entryprice=int(line.split(",")[4])
            symbol1exitprice=int(line.split(",")[2])
            symbol2entryprice=int(line.split(",")[5])
            symbol2exitprice=int(line.split(",")[3])
            symbol1quantity=int(line.split(",")[8])
            symbol2quantity=int(line.split(",")[9])
            tradepnl=((symbol1entryprice-symbol1exitprice)*symbol1quantity)+((symbol2exitprice-symbol2entryprice)*symbol2quantity)
            maxTradePnl=max(maxTradePnl,tradepnl)
            minTradePnl=min(minTradePnl,tradepnl)
            if tradepnl >= 0:
                positiveTrades=positiveTrades+1
            else:
                negativeTrades=negativeTrades+1
        k=k+1
    print("maxTradePnl:",maxTradePnl,"minTradePnl:",minTradePnl,"positiveTrades:",positiveTrades,"negativeTrades:",negativeTrades)

print(minpnl,",",maxpnl,",",drawdown,",",cummPnl,",",minTradePnl,",",maxTradePnl,",",positiveTrades,",",negativeTrades)




