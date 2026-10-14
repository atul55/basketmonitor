Create a new python program which will mimic the logic present in program delta_per_expiry_hybrid_stop_fixed.py. In the new program we need to monitor the open positions based on the user program Take Profit (TP) amount in USD and Stop Loss (SL) amount in USD. Note that this TP and SL should be on all the open positions on Delta Exchange. 


Review the program and make sure that this requirement is accurately implemented. Validate all the changes and make sure tha positions are exited only when the specified TP or SL is hit. If none of the TP and SL are hit, then let the position expire. 
- monitor the open positions based on the user provided Take Profit (TP) amount in USD and Stop Loss (SL) amount in USD from .env file. Consider only unrealized PnL on all the open positions in Delta Exchange.


First disable the close position code. There is a bug in the code. See the below output - 
[2026-10-14 19:42:34] Total PnL: -252.60 USD across 2 positions

On the delta exchange portal, Unrealized PnL is showing -4USD. Lets first fix the program so that it reflects the corrent PnL. We will enable the position closing code later. 


Logic is still wrong. I see 2USD on the delta exchange as the unrealized PnL but the below program is showing a different output - 
[2026-10-14 19:54:40] Unrealized PnL by settlement asset: USD 248.80 across 2 positions
  P-BTC-82800-151026: 193.20 USD
  C-BTC-82800-151026: 55.60 USD


Output is still incorrect. Add a debug statement to output the actual API call response. 
[2026-10-14 20:03:24] Unrealized PnL by settlement asset: USD 257.00 across 2 positions
  P-BTC-82800-151026: 205.60 USD
  C-BTC-82800-151026: 51.40 USD
[2026-10-14 20:03:40] Unrealized PnL by settlement asset: USD 258.80 across 2 positions
  P-BTC-82800-151026: 210.00 USD
  C-BTC-82800-151026: 48.80 USD
[2026-10-14 20:03:55] Unrealized PnL by settlement asset: USD 257.60 across 2 positions
  P-BTC-82800-151026: 207.00 USD
  C-BTC-82800-151026: 50.60 USD


Check this program delta_per_expiry_hybrid_stop_fixed.py which checks the unrealized PnL correctly. Update the program delta_account_pnl_tp_sl_guard.py accordingly. 


Now the Unrealized looks correct. Update the program to do a dry run closing the position. Do not close the positions yet. Compare how program delta_per_expiry_hybrid_stop_fixed.py is closing the positions and use the same logic in delta_account_pnl_tp_sl_guard.py


Format the output so that the unrealized PnL is shown and highlighted clearly. 

Formatting can be improvied further. 

Remove the dry run code now. Make changes to close the positions when TP or SL is hit. 

Enable Color highlighting of the key details in the output. 
Also check if the program will exit when there are no open positions or when the open positions are closed. 

1. Mimic the behaviour of Telegram alerting from the code delta_per_expiry_hybrid_stop_fixed.py to delta_account_pnl_tp_sl_guard.py
2. Create a database in which we can store all the realized and unrealized PnL values. 
3. Every time a poll is done for the checking the PnL, those values should be updated in the database. 
4. Create a chart in the same folder of the program,  which will reflect the unrealized PnL over time. 
5. Update the chart on every poll. 


Update the Telegram Alert that is sent every 5 mins as below - 
1. Remove the Account PnL Status Update line
2. First line of the alert should be Combined PnL so that $ value is seen on the message notification


Update the console output to show the TP and SL value on the each poll output. 

1. Generated chart doesn't show the correct unrealized PnL. Currently the Unrealized PnL is +18.4 but the chart shows the latest value as negative. 
2. Give me commands to access the data and query the data in it. 

1. Now the data on the chart looks correct. Document the database querying commands in the Readme file. 
2. Change the timezone on the console output and the chart to reflect the current timezone on which the program runs. 


1. Make the chart interactive so that I hover over and see the value of each point on the chart. 
2. Make the cursor as cross hair so that it will show time and the PnL value for each point. 
3. Chart should show the new poll values on the existing chart opened. 
4. Improve the formatting of the chart and make it advanced. 
5. Spin up a local server with a web page to depict the chart. Chart should be accessible using a url and should be automatically updated for new poll values. 


Format the readme file and organize it better. 
Program delta_per_expiry_hybrid_stop_fixed.py should be used for monitoring the positions taken with "Ratio Spread Strategy"
Program delta_account_pnl_tp_sl_guard.py should be used for monitoring the positions taken with "1 DTE Short Straddle" Strategy. 