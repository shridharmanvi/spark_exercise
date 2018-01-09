from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col, max as max_, udf
from pyspark.sql.types import StringType
import urlparse as u

conf = SparkConf().setMaster("local").setAppName("sparkExercise")
sc = SparkContext(conf = conf)

# Extracts biz_id from url
def extract_biz(url):
    parsed = u.urlparse(url)
    try:
        return u.parse_qs(parsed.query)['__biz'][0]
    except Exception as e:
        return None

# Reads local file into a data frame(RDD) 
def read_file_to_df(path):
    df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "\t").load(path)
    return df

def clean_clicks(clicks):
    # Rename columns
    sq = clicks.selectExpr("_c0 as id", "_c1 as url", "_c2 as title", "_c3 as read_number", "_c4 as like_number", "_c5 as timestamp", "_c6 as emp")
    # Get a df which consists of lines with max read_number for each url
    max_items = sq.groupBy("url").agg(max_("read_number").alias('read_number'))
    # Join the above two data sets to get clean non duplicate click data
    clicks = sq.alias('a').join(max_items.alias('b'), col('a.read_number') == col('b.read_number')).select([col('a.'+xy) for xy in sq.columns])
    # Define a UDF to extract biz_id from url
    url_parser=udf(extract_biz, StringType())
    clicks_withbiz = clicks_df.withColumn("biz_id", url_parser("url"))
    return clicks_withbiz


def clean_biz(biz):
    # Rename columns
    old_columns=['_c0', '_c1', '_c2', '_c3', '_c4', '_c5', '_c6', '_c7']
    new_columns=['id', 'biz_id', 'biz_name', 'biz_code', 'biz_desc', 'qr_code', 'timestamp', 'emp']
    df = reduce(lambda biz, idx: biz.withColumnRenamed(old_columns[idx], new_columns[idx]), xrange(len(old_columns)), biz)
    return df

if __name__ == '__main__':
    
    # Handle clicks data
    clicks_file_path = 'file:///Users/shridhar.manvi/Downloads/wechat_data_medium/weixin_click'
    clicks = read_file_to_df(clicks_file_path)
    clicks_df = clean_clicks(clicks)
    
    # Handle biz data
    biz_file_path = 'file:///Users/shridhar.manvi/Downloads/wechat_data_medium/weixin_biz'
    biz = read_file_to_df(biz_file_path)
    biz_df = clean_biz(biz)
    
