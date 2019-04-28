import MySQLdb, time
from secrets import corpora # Improve this later
from weibo import get_weibo_results

def scrape_weibo(debug_flag=False):
    # scrape_weibo()
    # Driver class; creates DB connection, processes posts, stores, and prints out debug info

    begin_time = time.time()

    # ==Connect to corpora==
    db = MySQLdb.connect(host=corpora['host'], user=corpora['username'], passwd=corpora['password'],
     db=corpora['database'], use_unicode=True)
    cursor = db.cursor()

    # == Return all distinct free_weibo_topics ==
    topic_list = pull_freeweibo_topics(cursor)

    # == Process topics ==
    saved_posts = []

    for topic in topic_list:
        saved_posts += [process_topic(db, cursor, topic)]
        db.commit()

    # == Close connection ==
    db.close()

    # == Log info ==
    if(debug_flag):
        print("===============Weibo Scraper===================")
        print("*** Succesfully scraped {0} freeweibo topics! ***".format(len(topic_list)))
        print("** Elapsed time: {0} seconds **)".format(time.time() - begin_time))
        for num_saved, topic in zip(saved_posts, topic_list):
            print ("Topic: [{0}] | Saved: [{1}]".format(topic, num_saved))
        print("===============================================")

        
def pull_freeweibo_topics(cursor):
    # pull_freeweibo_topics()
    # Grabs all distinct topics we have from freewebo_topics
    sql = "SELECT DISTINCT topic FROM freeweibo_topics"

    cursor.execute(sql)
    topics = cursor.fetchall()

    topic_list = [topic[0] for topic in topics] # Convert to a nicely formatted list

    return topic_list

def process_topic(db, cursor, topic):
    # process_topic()
    # Scrapes a list of posts relating to the topic, stores them, and makes entries in xref table
    # Returns number of posts processed

    post_list = get_weibo_results(topic)

    existing_hash_dict = get_hash_set(cursor) # Mapping of all hashes we have per topic

    # TODO: Probably a good way of doing this with a list comprehension but I give up
    all_hashes = []
    for topic in existing_hash_dict:
        run_list = existing_hash_dict[topic]
        all_hashes += run_list

    run_hash_list = [] # keep track of posts we've already processed in this run
    auxillary_post_list = [] # Keep track of posts we have, but need to associate topics to

    # Iterate through list backwards to avoid index deletion mishaps
    for post in reversed(post_list):
        phash = post['hash']
        ptopic = post['topic']

        if phash in run_hash_list: # We've already found this post for this topic in this run
            post_list.remove(post)
        else: # We haven't come across this post for this topic in this run
            if phash in all_hashes: # We have this post in the db, but maybe not for this topic
                post_list.remove(post)
                try:
                    if phash in existing_hash_dict[ptopic]:  # We already have it for this topic
                        pass
                    else: # Don't yet have this post for this topic; make association
                        auxillary_post_list.append(post)
                except Exception as e: # We don't have any posts yet for this topic
                    auxillary_post_list.append(post)

            else: # We don't have this post in the db; store it.
                run_hash_list.append(phash)

    if post_list or auxillary_post_list:
        store_posts(db, cursor, post_list, auxillary_post_list)

    return len(post_list) + len (auxillary_post_list)

def store_posts(db, cursor, post_list, aux_post_list):
    # store_posts()
    # Accepts cursor, list of discovered posts
    # Stores the posts and creates an entry in the xref table

    # == Store each post in weibo_oracle ==
    if post_list:
        oracle_sql = build_oracle_query(db, post_list)
        cursor.execute(oracle_sql)

        # = Update post_list with new KPs =
        post_list = augment_post_list_with_kp(cursor, post_list)

        # == Manage cross reference table entries ==
        crossref_sql = build_crossref_query(db, post_list)
        cursor.execute(crossref_sql)

    # = Manage auxillary cross reference entries =
    if aux_post_list:
        aux_post_list = augment_post_list_with_kp(cursor, aux_post_list)
        aux_sql = build_crossref_query(db, aux_post_list)
        cursor.execute(aux_sql)

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
    # get_hash_set():
    # Return a dict of topics mapped to lists of hashes we have for that topic
    # NOTE: Format -> {'Topic0':{hashes we have for this topic}, 'Topic1':{...}, ...}

    sql = "SELECT weibo_oracle.hash, weibo_oracle_xref.topic FROM weibo_oracle INNER JOIN weibo_oracle_xref ON weibo_oracle.kp = weibo_oracle_xref.oracle_kp;"

    cursor.execute(sql)
    results = cursor.fetchall()

    hashed_dict = {topic:{x[0] for x in results if x[1] == topic} for hash_val, topic in results}

    return hashed_dict

def augment_post_list_with_kp(cursor, post_list):
    # augment_post_list_with_kp()
    # Modifies and returns a post list augmented w/ the matching WO KPs associated with that hash
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
