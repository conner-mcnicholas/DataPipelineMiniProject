# DataPipelineMiniProject

beginning with folder only containing initcsv.sh and third_party_sales.csv...
run:
```
pip3 install -r requirements.txt
./initcsv.sh
python pipeline.py
```

Program should output (also found in output.txt):
```
Creating table sales: OK
Record inserted
Record inserted
Record inserted
Record inserted
Record inserted
Record inserted
Here are the top three events by number of tickets sold:
    -Christmas Spectacular(5 tix sold)
    -Washington Spirits vs Sky Blue FC(5 tix sold)
    -The North American International Auto Show(4 tix sold)
```
