import requests, datetime, re
from bs4 import BeautifulSoup

def get_weibo_results(topic):
    # get_weibo_results()
    # Returns a list of formatted dicts representing unique weibo posts relating to a
    # certain topic.

    # Url is for the 'Popular Posts' section for the given topic
    base_url = "https://s.weibo.com/weibo?q={0}&xsort=hot&Refer=hotmore"

    page = requests.get(base_url.format(topic))
    soup = BeautifulSoup(page.content, 'html.parser')

    # Grab all individual posts, each denoted by 'card-wrap'
    raw_post_list = soup.find_all('div', class_='card-wrap')
    processed_post_list = []

    # Process each post
    for raw_post in raw_post_list:
        # If post corrupted or gives us trouble - don't bother with it - log incident
        try:
            post_content = raw_post.find_all('p', class_='txt')

            # Some posts are just videos or pictures; we're only interested in text
            # so we want to ignore anything that doesn't have it
            if post_content:
                proc_post = {}

                # = Text =
                # Extract the fully expanded txt at [-1]
                proc_post['text'] = post_content[-1].get_text().encode('utf-8')
                # Get rid of new lines and execessive whitespace
                proc_post['text'] = proc_post['text'].replace('\n', '').strip()
                proc_post['text'] = proc_post['text'].replace('  ', '')

                # Used to distinguish posts from each other
                proc_post['hash'] = hash(proc_post['text'])
                proc_post['topic'] = topic

                # = Username =
                # Strange formatting on Weibo sometimes (rarely) obfuscates the nick-name
                try:
                    proc_post['nickname'] = post_content[-1]['nick-name'].encode('utf-8')
                except Exception:
                    proc_post['nickname'] = None

                # = Time and device (if it exists) =
                from_field = raw_post.find_all('p', class_='from')[0] # extract 'from' tag
                proc_post = extract_time_and_source(proc_post, from_field)

                # = Social data =
                social_data = raw_post.find('div', class_='card-act')
                proc_post = parse_social(social_data, proc_post)

                processed_post_list.append(proc_post)
        except Exception as e:
            print ("Error occured: ", e)

    return processed_post_list

def extract_time_and_source(post_dict, dirty_tag):
    # extract_time_and_source()
    # Parses the time and source tag from a scraped weibo post

    # Time/date and source separated by obscenely large blank space
    # TODO: Replace w/ strip or split
    tag_text = dirty_tag.get_text().split('                                         ')

    time_date = tag_text[0].split() # Clear all white space, seperate into -> [date,time]
    # = Parse date =
    # '2011(Chinese year)01(Chinese Month)11(Chinese Day)' - standard format (for historical post)

    # Case 1 : If a post was made today Weibo doesn't supply us with a date
    if len(time_date) == 1:
        month = int(datetime.datetime.now().month)
        day = int(datetime.datetime.now().day)
        year = int(datetime.datetime.now().year)

        # Use regex to wipe out non-latin characters
        time = re.sub(ur'[^\x00-\x7F\x80-\xFF\u0100-\u017F\u0180-\u024F\u1E00 -\u1EFF]', u'', time_date[0])

        hour = int(time.split(':')[0])
        minute = int(time.split(':')[1])

    # Case 2: Weibo has given us a date -> parse it
    else:
        # Use regular expression to wipe out anything that isn't a latin character
        date = re.sub(ur'[^\x00-\x7F\x80-\xFF\u0100-\u017F\u0180-\u024F\u1E00 -\u1EFF]', u'', time_date[0])

        month = int(date[-4:-2])
        day = int(date[-2:])

        # If a post is from the current year -> no year is specified
        if (len(str(date)) == 8):
            year = int(date[:4])
        else:
            year = int(datetime.datetime.now().year)

        hour = int(time_date[1].split(':')[0])
        minute = int(time_date[1].split(':')[1])

    # = Finalize =
    post_dict['pub_date'] = datetime.datetime(year, month, day, hour, minute)
    post_dict['ret_date'] = datetime.datetime.now()

    # If a source/device is specified, retrieve it
    if len(tag_text) == 2:
        post_dict['source'] = tag_text[1]
    elif len(tag_text) == 1:
        post_dict['source'] = None

    return post_dict

def parse_social(social_data, proc_post):
    # parse_social()
    # Accepts social data section from a weibo post; strips it for metrics, returns modified post
    soc_list = social_data.find_all('li')

    # Get the text of each list entry; if no value available will find chinese character
    shares = strip_non_latin(soc_list[1].get_text().split()[-1])
    if shares:
        proc_post['shares'] = int(shares)
    else:
        proc_post['shares'] = 0

    comments = strip_non_latin(soc_list[2].get_text().split()[-1])
    if comments:
        proc_post['comments'] = int(comments)
    else:
        proc_post['comments'] = 0

    likes = strip_non_latin(soc_list[3].get_text().split()[-1])
    if likes:
        proc_post['likes'] = int(likes)
    else:
        proc_post['likes'] = 0

    return proc_post

def strip_non_latin(text):
    # strip_non_latin
    # Uses a regex expression to wipe out all non-latin characters

    text = re.sub(ur'[^\x00-\x7F\x80-\xFF\u0100-\u017F\u0180-\u024F\u1E00 -\u1EFF]', u'', text)

    return text
