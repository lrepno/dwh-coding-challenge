# DWH Coding Challenge Solution

## Solution structure

Each solution is located in appropriate folder `taskN` inside `src` directory.
Inside directory following files included:
* python solution file
* requirements.txt
* Dockerfile - I have included default environment variables for data path and logging level
* run.sh bash script for automated run

## Task 1

Task1 consist of several functions that return pandas dataframe allowing to reuse this code in other tasks.

All `.json` files are considered inside the directory, collected to list of dictionaries and then transformed to pandas dataframe.
This is not the best solution for production, especially when dealing with large amount of data and small files.

The processing is inside `create_table_from_events` function that iterates over sorted dataframe and applies logic based on `c` or `u` operations.
Once the data is being changed, historical table is populated with old version of record. Same happens if the new record has been created.

At first, my thinking was to get a current state of table (e.g. make all of the creates/updates on each row), 
but once I started working on the Task 2, I realized that we need a historical view so I print both current state and historical view. 
This is easy to remove/comment so I have kept both of them for flexibility.

## Task 2

Task 2 is a tricky one, in order to get a historical denormalized view I have collected all unique timestamps from dataframes
and merged (made an outer join) with each of the historical dataframes from task1.
This also requires to forward-fill data so on each timestamp we know what is the state of all tables. 
Otherwise, we would have NaN and this solution wouldn't work (not all joins would happen properly).


## Task 3

This task is considering the output table from Task 2 and calculates what are the balance/credit changes were made, 
filters the rows with changes and split them to credit/balance dataframes, allowing to have 1 line per 1 transaction.

In order to discuss how many transactions, when and what is the size of them, we would need to print all of them and analyze.
No specific directions are given so I printed how much transactions occured and table with them. 


## How to run

Run `run.sh` script that builds docker container and runs it locally.

In order to run a solution, you will need to run a bash script for it.
`chmod +x run.sh` command may be required to run to add executable permissions.

Example is below:


```bash
rleonid@rleonid:~$ cd PycharmProjects/workdir/dwh-coding-challenge/solution/src/task1
rleonid@rleonid:~/PycharmProjects/workdir/dwh-coding-challenge/solution/src/task1$ chmod +x run.sh 
rleonid@rleonid:~/PycharmProjects/workdir/dwh-coding-challenge/solution/src/task1$ ./run.sh 
Sending build context to Docker daemon  382.5kB
Step 1/8 : from python:3.8-buster
 ---> 07330382c4de
Step 2/8 : COPY . .
 ---> Using cache
 ---> 699f81d160f7
Step 3/8 : RUN pip3 install -r solution/src/task1/requirements.txt
 ---> Using cache
 ---> 35e42d95c71a
Step 4/8 : WORKDIR solution/src
 ---> Using cache
 ---> 777dfde9bb54
Step 5/8 : ENV DATA_PATH ../../data
 ---> Using cache
 ---> f7f4006af965
Step 6/8 : ENV LOG_LEVEL INFO
 ---> Using cache
 ---> 1085aedcc7c8
Step 7/8 : ENV PYTHONPATH "${PYTHONPATH}:."
 ---> Using cache
 ---> 868223c1aba4
Step 8/8 : CMD [ "python3", "task1/task1.py"]
 ---> Using cache
 ---> 2e1dc7c8b034
Successfully built 2e1dc7c8b034
Successfully tagged task1:latest
2021-08-26 11:57:19,020 - root - INFO - historical accounts table:
           id             ts account_id     name   address phone_number                    email savings_account_id card_id
0  a1globalid  1577863800000         a1  Anthony  New York     12345678     anthony@somebank.com                NaN     NaN
1  a1globalid  1577865600000         a1  Anthony  New York     87654321     anthony@somebank.com                NaN     NaN
2  a1globalid  1577890800000         a1  Anthony  New York     87654321     anthony@somebank.com                sa1     NaN
3  a1globalid  1577894400000         a1  Anthony   Jakarta     87654321  anthony@anotherbank.com                sa1     NaN
4  a1globalid  1577926800000         a1  Anthony   Jakarta     87654321  anthony@anotherbank.com                sa1      c1
5  a1globalid  1579078860000         a1  Anthony   Jakarta     87654321  anthony@anotherbank.com                sa1        
6  a1globalid  1579163400000         a1  Anthony   Jakarta     87654321  anthony@anotherbank.com                sa1      c2

2021-08-26 11:57:19,022 - root - INFO - accounts table:
           account_id     name  address phone_number                    email             ts savings_account_id card_id
a1globalid         a1  Anthony  Jakarta     87654321  anthony@anotherbank.com  1579163400000                sa1      c2

2021-08-26 11:57:19,030 - root - INFO - historical cards table:
           id             ts card_id card_number  credit_used  monthly_limit   status
0  c1globalid  1577926800000      c1    11112222            0          30000  PENDING
1  c1globalid  1578159000000      c1    11112222            0          30000   ACTIVE
2  c1globalid  1578313800000      c1    11112222        12000          30000   ACTIVE
3  c1globalid  1578420000000      c1    11112222        19000          30000   ACTIVE
4  c1globalid  1578654000000      c1    11112222            0          30000   ACTIVE
5  c1globalid  1579078800000      c1    11112222            0          30000   CLOSED
6  c2globalid  1579163400000      c2    12123434            0          70000  PENDING
7  c2globalid  1579298400000      c2    12123434            0          70000   ACTIVE
8  c2globalid  1579361400000      c2    12123434        37000          70000   ACTIVE

2021-08-26 11:57:19,031 - root - INFO - cards table:
           card_id card_number  credit_used  monthly_limit  status             ts
c1globalid      c1    11112222            0          30000  CLOSED  1579078800000
c2globalid      c2    12123434        37000          70000  ACTIVE  1579361400000

2021-08-26 11:57:19,039 - root - INFO - historical savings_account table:
            id             ts savings_account_id  balance  interest_rate_percent  status
0  sa1globalid  1577890800000                sa1        0                    1.5  ACTIVE
1  sa1globalid  1577955600000                sa1    15000                    1.5  ACTIVE
2  sa1globalid  1578159060000                sa1    15000                    3.0  ACTIVE
3  sa1globalid  1578648600000                sa1    40000                    3.0  ACTIVE
4  sa1globalid  1578654000000                sa1    21000                    3.0  ACTIVE
5  sa1globalid  1579078860000                sa1    21000                    1.5  ACTIVE
6  sa1globalid  1579298460000                sa1    21000                    4.0  ACTIVE
7  sa1globalid  1579505400000                sa1    33000                    4.0  ACTIVE

2021-08-26 11:57:19,040 - root - INFO - savings_accounts table:
            savings_account_id  balance  interest_rate_percent  status             ts
sa1globalid                sa1    33000                    4.0  ACTIVE  1579505400000
```

