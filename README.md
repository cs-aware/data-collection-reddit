# data-collection-reddit

The script monitors a set of Reddit accounts and collects the latest posts. The posts are consolidated in a CSV file and stored within AWS S3 storage.
Currently, for the CS-AWARE project, we started monitoring only the accounts listed in users.json. Please note that this solution requires Reddit API credentials (config.py).
Finally, the code is written for Python3, anyhow it could be easily adapted for Python2.

### How to install dependencies and run the script

```console
aws configure
git clone https://github.com/cs-aware/data-collection-reddit.git
sudo python3 -m pip install -r requirements.txt 
python3 main.py
```

`This project has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No 740723. This communication reflects only the author's view and the Commission is not responsible for any use that may be made of the information it contains.
`
