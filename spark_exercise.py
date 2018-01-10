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
    clicks_withbiz = clicks.withColumn("biz_id", url_parser("url"))
    return clicks_withbiz


def clean_biz(biz):
    # Rename columns
    old_columns=['_c0', '_c1', '_c2', '_c3', '_c4', '_c5', '_c6', '_c7']
    new_columns=['id', 'biz_id', 'biz_name', 'biz_code', 'biz_desc', 'qr_code', 'timestamp', 'emp']
    df = reduce(lambda biz, idx: biz.withColumnRenamed(old_columns[idx], new_columns[idx]), xrange(len(old_columns)), biz)
    return df

# IMPROVEMENT: To fasten this process, I would split the html file into multiple smaller files and handle them parallely using thread pool
# Extract html data from file
# Since this method streams the file line by line, there is no risk of running into memory issues due to file size
def read_html_pages(input_file_path, outfile_path):
    with open(input_file_path) as infile, open(outfile_path, 'w') as outfile:
        copy = False
        data={}
        for line in infile:
            if line.strip() == "<BODY><!DOCTYPE html>":
                copy = True
            elif line.strip() == "</BODY>":
                copy = False
                try:
                    outfile.write(data['biz_id'] + '\t' + data['create_time'] + '\n')
                except Exception as e:
                    pass
                data = {'create_time':'', 'biz_id':''} # Reset
            elif copy:
                data = parse_line(line, data)


def topic_of_interest(line):
    line = line.strip()
    ret_value = ''
    if line.startswith('var ct = '):
        ret_value = line.split('"')[1]
    elif line.startswith('var appuin ='):
        ret_value = line.split('"')[1]
    return ret_value


def parse_line(line, data):
    interest = topic_of_interest(line)
    if(interest != ''):
        if(is_number(interest)):
            data['create_time'] = interest
        else:
            data['biz_id'] = interest
    return data

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

def clean_html(html_data):
    old_columns=['_c0', '_c1']
    new_columns=['biz_id', 'create_time']
    df = reduce(lambda html_data, idx: html_data.withColumnRenamed(old_columns[idx], new_columns[idx]), xrange(len(old_columns)), html_data)
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
    
    # Handle html data
    html_data_path = '/Users/shridhar.manvi/Downloads/wechat_data_medium/weixin_page_test'
    html_outfile_path = '/Users/shridhar.manvi/Downloads/wechat_data_medium/outfile'
    # This method streams the file and saves the required columns to a different file to be read as rdd in the next line
    read_html_pages(html_data_path, html_outfile_path)
    html_data = read_file_to_df(html_outfile_path)
    
    # Interim join
    interim = ht.alias('a').join(biz_df.alias('b'), col('a.biz_id') == col('b.biz_id')).\
    select(col('a.biz_id'), col('a.create_time'), col('b.biz_name'), col('b.biz_name'), col('b.biz_desc'))

    # Join with third dataframe
    final = interim.alias('a').join(clicks_df.alias('b'), col('a.biz_id') == col('b.biz_id')).\
    select(col('a.biz_id'), col('a.create_time'), col('a.biz_name'), col('a.biz_name'), col('a.biz_desc'), col('b.title'), \
    col('b.url'), col('b.read_number'), col('like_number'))
