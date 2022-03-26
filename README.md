# DataPipelineMiniProject

beginning with folder only containing:
- `requirements.txt`
- `initcsv.sh`
- `third_party_sales.csv`
- `pipeline.py`...

running the following:
```
pip3 install -r requirements.txt
./initcsv.sh
python pipeline.py > output.txt
```
will create several more files, most importanly the program log `output.txt`.

To examining contents of that log, execute command:
`cat output.txt`

Successful program operation will yield the following log output:
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
