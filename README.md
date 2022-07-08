# CS422 Project 2-Data Processing Pipelines Over Apache Spark-Report

## Task 1: Bulk-load data

In this task, we need to implement two data loading classes (**TitlesLoader**, **RatingsLoader**).

We use `sc.textFile` to load data as RDD[String]. Then we use two `map` functions consecutively to handle each records:
* The first one splits string into List[String] row by row based on delimeter "|".
* The second one just creates RDD format accoridng to interface row by row.
* We need to persist the loaded data so as to avoid re-reading data.

## Task 2: Average rating pipeline

### Task 2.1: Rating aggregation
#### Init()
In the `init()` function, we need to carefully decide what kind of data we would need later, and then we should prepare it at just one time, finally persisting it! 
#### Join
When joining title information with rating information, since some titles may not be rated yet, we need to use `leftOuterJoin`(titles are at left, while ratings are at right) so as to not miss unrated titles in the joined result.
#### Rated Flag
We use a boolean flag to indicate whether a title is rated or not(Its importance will be demonstrated in next subtasks).
#### Stored Data
Having the jointed result, we first use `countByKey` to count the the number of ratings for each title; then we use `reduceByKey` to get the sum of ratings for each title. Then we can get `ratings_avg` by caculating the average rating, the number of ratings, rated flag for each title!
We decided to store the following data(other data will be omited):
* **title_map:** It is a read-only map. Its key is title's id, and its value is a tuple that contains title's name and keywords(keywords are as Set instead of List, since we need to efficiently figure out if the keywords of a specific title would contains all required keywords in Task 2.2). So in the future, we can get information efficiently whenever we have a title id.
* **ratings_avg:** Its format is `RDD[(Int, (Double, Double, Boolean))]`. Its key is title's id and its value is a tuple that contains the average rating, the number of ratings, and rated flag.
#### getResult()
Since we already have `ratings_avg` and `title_map` we can get results by simply applying `map`.

### Task 2.2: Ad-hoc keyword rollup
#### getKeywordQueryResult()
This function requires that the result would exclude unrated titles, so here `Rated Flags` plays its role! We first filter out unrated titles. Then we apply `filter` on the result by setting the condition that the valid title should contain required keywords(Time complexity is O(n), and n is the number of required keywords)! 
#### Aggregate
Then, it is similar with task 2.1, but when we want to use `aggregate`, we need to define the aggregate function for partitions and another one for merging results from different partitions, and finnaly we can get the sum. Then we just divide the count of valid titles, we can get the average needed. The reason to do the above calculation is that project description mentions that "all titles contribute equally."

### Task 2.3: Incremental maintenance
The key idea to simplify this problem is to first handle the input `delta_`. To be more specific, for a rating record inputed: 
* **old_rating == None:** It is a new incoming rating, we count it as (new_rating, 1). The second element of the tuple is its "count" contribution.
* **old_rating != None:** It is a modified rating, we count it as (new_rating - old_rating, 0). The second element of tuple is 0 since we have counted its contribution before.
After handling `delta_`, we can map our preserved `ratings_avg` to reconstruct the average(calculate new sum and divide it with new count) for each title if applicable.

## Task 3: Similarity-search pipeline
### Task 3.1: Indexing the dataset
In this task, we mainly need to complete function `getBuckets()` of class `LSHIndex`. We first iterate the input data row by row to change a record to a tuple(key, value). Key is hashing result of corresponding keywords, and value is original record. Then we call `groupByKey()` to group the final result needed. We need to store the result as a read-only map as `bucket` which can facilitate future lookup operations.

### Task 3.2: Near-neighbor lookups
In this task, when we have an incoming quiry that contains a list of keywords, we can compute its hashing result and then get results from `bucket`. If there is no mathced results, we just return empty list.

### Task 3.3: Near-neighbor cache
* **cacheLookup():** This function's return value is (hit_quiries_results, miss_quiries). We simply use `filter`, and then we can have hit_quiries and miss_quiries. For hit_quiries, we retrieve hit_quiries_results from `cache`.
* **lookup():** This function would first call `cacheLookup()` to get (hit_quiries_results, miss_quiries). For miss_quiries, we would call `lshIndex.lookup(miss)` to get results. At the end, we just return union of the above two results.
* **buildExternal():** We simply use `cache = ext.value` to save the broadcast message.

### Task 3.4: Cache policy
#### histogram in cacheLookup()
For the incoming quiries in `cacheLookup()`, we first hash them, and then apply `countByKey()` to calculate the total number for every type of quieries. Then we just filter out number that is <= 1% of total amount.
#### Cache creation:build() 
Having the results of `histogram` in `acheLookup()`, we broadcast `histogram` to get the return value, which is the comman cache on each machine. Then we perform `lshIndex.lookup()` to get quiry results, which would be saved as local cache.