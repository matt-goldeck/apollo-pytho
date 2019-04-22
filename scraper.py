import MySQLdb
from secrets import corpora # Improve this later
from weibo import get_weibo_results

# Overall Plan
# Get all unique topics from database
# Pull hash set of (text + topic) from weibo_oracle
    # For each topic scrape associated Weibo posts
    # If hash already in table, ignore, otherwise store it in weibo_oracle
    # Create entries in the xref table to link oracle content to the -
    # - source topic (which may have many, many appearances in freeweibo_topics)

def scrape_weibo():
    # ==Connect to corpora==
    db = MySQLdb.connect(host=corpora['host'], user=corpora['username'], passwd=corpora['password'],
     db=corpora['database'], use_unicode=True)
    cursor = db.cursor()

    # == Return all distinct free_weibo_topics ==
    topic_list = pull_freeweibo_topics(cursor)


    # == Process topics ==
    saved_topics = 0
    for topic in topic_list:
        process_topic(db, cursor, topic)
        saved_topics += 1

    # == Save, close connection ==
    db.commit()
    db.close()

    return saved_topics;

def pull_freeweibo_topics(cursor, topic=None):
    # pull_freeweibo_topics()
    # Grabs all topics we have from freeweibo that match a topic; distinct if topic is None

    #TODO: Generalize this method better

    if topic:
        sql = "SELECT kp FROM freeweibo_topics WHERE topic = '{0}'".format(topic)
    else:
        sql = "SELECT DISTINCT topic FROM freeweibo_topics"

    cursor.execute(sql)
    topics = cursor.fetchall()

    topic_list = [topic[0] for topic in topics] # Convert to a nicely formatted list

    return topic_list

def process_topic(db, cursor, topic):
    # process_topic()
    # Scrapes a list of posts relating to the topic, stores them, and makes entries in xref table

    post_list = get_weibo_results(topic)
    existing_hash_set = get_hash_set(cursor)
    new_hash_list = []

    # Iterate through backwards to avoid index deletion mishaps
    for post in reversed(post_list):
        # Hash text and topic to determine if we've already stored this post for this topic
        if post['hash'] not in existing_hash_set and post['hash'] not in new_hash_list:
            new_hash_list = new_hash_list + [post['hash']] # Add new post to hash set
            post['topic'] = topic
        else:
            post_list.remove(post)

    if post_list:
        store_posts(db, cursor, post_list)

def store_posts(db, cursor, post_list):
    # store_posts()
    # Accepts cursor, list of discovered posts
    # Stores the posts and creates an entry in the xref table

    # == Store each post in weibo_oracle ==
    oracle_sql = build_oracle_query(db, post_list)
    cursor.execute(oracle_sql)

    # == Update post_list with new kps ==
    post_list = augment_post_list_with_kp(cursor, post_list)

    # == Manage cross reference table entries ==
    crossref_sql = build_crossref_query(db, post_list)
    cursor.execute(crossref_sql)

def build_oracle_query(db, post_list):
    # build_oracle_query()
    # Accepts post list, formats and returns a constructed SQL query to store each post

    # Base statement
    oracle_sql = "INSERT INTO weibo_oracle (hash, content, pub_date, ret_date, likes, shares, comments, nickname, processed, marked_by_proc) VALUES {0}"

    # Create a list of format strings and values for each post in post list
    post_strings = run_values = []
    for post in post_list:
        post_strings = post_strings + ["({}, '{}', '{}', '{}', {}, {}, {}, '{}', {}, {})"]
        # TODO: Plug  in word bagger from Apollo
        run_values = run_values + [post['hash'], post['text'], post['pub_date'], post['ret_date'], post['likes'], post['shares'], post['comments'], post['nickname'], 0, 0]

    # Join together for final combination
    sql = oracle_sql.format(", ".join(post_strings))
    sql = sql.format(*[db.escape_string(str(value)) for value in run_values])

    return sql

def build_crossref_query(db, post_list):
    # build_crossref_query()
    # Accepts post list, prepares and returns sql to create cross_ref entry for each topic

    xref_sql = "INSERT INTO weibo_oracle_xref (oracle_kp, topic) VALUES {0}"

    # Create an entry in the for each post
    post_strings = run_values = []
    for post in post_list:
        post_strings = post_strings + ["({}, '{}')"]
        run_values = run_values + [post['kp'], post['topic']]

    # Join together for final insertion
    sql = xref_sql.format(", ".join(post_strings))
    sql = sql.format(*[db.escape_string(str(value)) for value in run_values])

    return sql

def get_hash_set(cursor):
    # Return the set of all hashes in weibo_oracle
    sql = "SELECT hash FROM weibo_oracle;"
    cursor.execute(sql)
    hashed_set = {hm[0] for hm in cursor.fetchall()}

    return hashed_set

def augment_post_list_with_kp(cursor, post_list):
    # augment_post_list_with_kp()
    # Modifies and returns a post list augmented w/ the matching KPs associated with that hash
    # Used by store_posts() after weibo_oracle posts have been newly inserted

    hash_list = ", ".join([str(post['hash']) for post in post_list])
    hash_sql = "SELECT hash, kp FROM weibo_oracle WHERE hash in ({0})".format(hash_list)

    cursor.execute(hash_sql)
    results = cursor.fetchall()

    # Create hash-kp mapping for rapid lookup
    hash_dict = {hash:kp for hash, kp in results}
    # Augment post_list
    for post in post_list:
        post['kp'] = hash_dict[post['hash']]

    return post_list
