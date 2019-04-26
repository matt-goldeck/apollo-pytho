# apollo-pytho
A component of Project Apollo in the Network Science Lab at Montclair State University. Scrapes Weibo posts that correspond to topics the Apollo project has gathered in order to provide an up-to-date corpus of censored posts (publically available) content to compare against uncensored (removed; not public).

Retrieves a distinct list of FreeWeibo topics and uses them to probe Weibo. Scrapes the search results, processes, formats, and stores as a unique weibo_oracle post and as an entry in the weibo_oracle_xref table. 

## Tables 

### Apollo FreeWeibo Topics
Used by Apollo Core. Scraped from the censored topics section of FreeWeibo.com; reported by the project as one of the 'top 10' most censored topics on Weibo. Scraped at a revolving interval throughout the day to track the position of the topic in the list. Querying multiple rows containing a given topic over a period of time gives us a rough idea of how censored a topic has been, though its absence from this table isn't necessarily indicative of an absence of censorship.

| Column | Type | Explanation                 |
| -------|------|-----------------------------|
|**kp** | int  | Key Primeiro - Primary key  | 
|**date_sampled** | datetime | Time we gathered this topic |
|**topic** | tinytext | The topic in question |
|**link** | tinytext| A link to the post on freeweibo.com |
|**n** | tinytext | The position in the list the topic held at time of retrieval|

### Pytho Weibo_Oracle
Represents a Weibo post containing a keyword or topic from the Freeweibo_Topics table. Designed to mimic the Freeweibo table of scraped Freeweibo posts. Not necessarily uncensored, but represents a post that met the standards of Weibo such that it could be publically visible. Linked to the topics from which it was found via the Weibo_Oracle_Xref table.

| Column | Type | Explanation                 |
|--------|------|-----------------------------|
|**kp** | int  | Auto-incrementing primary key|
|**hash** | bigint | Hashed content of the post used to prevent duplicates|
|**content**| text | Content of the post |
|**pub_date**| datetime | Date and time the post was made |
|**ret_date** | datetime | Date and time the post was retrieved |
|**processed** | tinyint | Whether or not this post has been bagged yet |
|**marked_by_proc** | int | Whether or not this post is being processed by the bagger |
|**bag_of_words** | text | Feature of the Apollo project; escaped corpus of most important words |
|**likes** | smallint | Amount of likes the post had at time of scraping |
|**shares** | smallint | Amount of shares the post had at time of scraping |
|**comments** | smallint | Amount of comments the post had at time of scraping |

### Pytho Weibo_Oracle_Xref
Many-to-many relationship between Freeweibo_Topics and Weibo_Oracle. Links a source topic (eg 'Labor Shortages') to posts discovered with it in the weibo_oracle table. 

| Column | Type | Explanation                |
|--------|------|----------------------------|
|**oracle_kp**| int | The kp of the weibo_oracle post|
|**topic**| tinytext | The topic from Freeweibo_topics that was used to find this post|
|**created_at**| timestamp | Auto-generated timestamp at time of insertion |

