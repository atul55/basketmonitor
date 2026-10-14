# basketmonitor
Monitor the Order Basket in Delta Exchange

# PreRequisite -
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

old commands-
py -m pip install -r requirements.txt
py -m pip install aiohttp

# Latest command to run 
python3 delta_per_expiry_hybrid_stop_fixed.py