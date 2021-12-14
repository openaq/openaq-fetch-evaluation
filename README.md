<h1 align="center">OpenAQ fetch evaluation</h1>

This script checks the openaq-api in order to get the latest updates for [openaq-fetch](https://github.com/openaq/openaq-fetch) adapters, this will help to check which adapters have been still working correctly in which adapters may faling or needs updates in the node.js code.


# Step 1:  clone the repo

```sh
git clone https://github.com/openaq/openaq-fetch-evaluation

```

# Step 2: Get location ID fron openaq-api DB

```sql
    SELECT sensor_nodes_id,site_name,source_name,city FROM sensor_nodes WHERE ismobile=False and source_name !='PurpleAir';
```

Save results as: `data/adapters_id.csv`

# Step 3: Build docker image

```sh
cd openaq-fetch-evaluation/
docker-compose build
```


# Step 4:Get latest update for adapters


```sh
    docker-compose up
```

Output: The script will generates two files `data/adapters_update.csv` and `data/adapters_outdate.csv` which can be used to the the evaluations.



