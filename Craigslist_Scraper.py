from bs4 import BeautifulSoup
from urllib2 import urlopen
import sys
import smtplib
import MySQLdb as mdb
import MySQLdb.cursors
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import configparser
import datetime
import os
import logging

def parse_results(search_term,min_price,max_price,must_have_image,url_prefix,title_only):
    # Craigslist search URL
    BASE_URL = ('http://{4}.craigslist.org/search/'
            'sss?sort=rel&postedToday=1&minAsk={1}&maxAsk={2}&query={0}{3}{5}')
    results = []
    if must_have_image == 1:
        must_have_image = '&hasPic=1'
    else:
        must_have_image = ''
    if title_only ==1:
        title_only='&srchType=T'
    else:
        title_only=''
    search_term = search_term.strip().replace(' ', '+')
    search_url = BASE_URL.format(search_term,min_price,max_price,must_have_image,url_prefix,title_only)
    #logger.info('search url: {0}'.format(search_url))
    #logger.info('')
    soup = BeautifulSoup(urlopen(search_url).read())
    rows = soup.find('div', 'content').find_all('p', 'row')
    for row in rows:
        if row.a['href'].startswith('http'):
            pass
        else:
            url = 'http://{0}.craigslist.org'.format(url_prefix) + row.a['href']
            #price = row.find('span', class_='price').get_text()
            price = ''
            create_date = row.find('time').get('datetime')
            title = row.find_all('a')[1].get_text()
            title = title.encode("utf-8")
            d = dict({'url': url, 'create_date': create_date,'title': title,'price': price})
            results.append(d)
    return results

def get_active_searches():
    try:
        con = mdb.connect(host=db_host, db=db_database, passwd=db_pass, user=db_user, port=db_port,charset='utf8', cursorclass=MySQLdb.cursors.DictCursor);

        sql = "SELECT `id`,`description`,`keywords`,`price_max`,`price_min`,`must_have_image`,`title_only`,`email` FROM v_searches WHERE `status` = 'active' AND `is_verified`=1"
        with con:
            cur = con.cursor()
            cur.execute(sql)
            active_searches = cur.fetchall()
    except Exception as ex:
        logger.error(str(ex))
    return active_searches

def interate_through_searches(active_searches):
    logger.info('  entering loop for each search')
    new_records = []
    for search in active_searches:
        try:
            search_id=search['id']
            description=search['description']
            keywords = search['keywords']
            min_price=search['price_min']
            max_price=search['price_max']
            must_have_image=search['must_have_image']
            title_only=search['title_only']
            email =search['email']

        except Exception as ex:
            logger.info("Unable to initialize search variables")
            sys.exit(1)
        locations = get_search_locations(search_id)
        #logger.info('locations:{0} '.format(locations))
        for location in locations:
            url_prefix = location['url_prefix']
            #logger.info("Scanning craigslist for search_id: {0}".format(search_id))
            print''
            results = parse_results(keywords,min_price,max_price,must_have_image,url_prefix,title_only)
            logger.info("  Getting results for search id: {0}, location: {1}".format(search_id,location['url_prefix']))
            print''
            new_records = get_new_records(results,search_id)
            try:
                if len(new_records) > 0:
                    #logger.info("Sending email")

                    send_email(email,description,new_records)

                    #logger.info('writing new records for search id: {0}'.format(search_id))

                    write_results(new_records,search_id)
                    logger.info('    records successfully saved')
                else:
                    pass
            except Exception as ex:
                logger.error(str(ex))
        logger.info('    {0} new records found for search id {1}'.format(len(new_records),search_id))
def get_search_locations(search_id):
    con = mdb.connect(host=db_host, db=db_database, passwd=db_pass, user=db_user, port=db_port,charset='utf8', cursorclass=MySQLdb.cursors.DictCursor);
    sql = "SELECT `url_prefix` FROM v_search_locations WHERE `search_id` ={0}".format(search_id)
    #logger.info('location sql: {0}'.format(sql))
    print''
    with con:
        cur = con.cursor()
        cur.execute(sql)
        search_locations = cur.fetchall()
    return search_locations
#save new postings
def write_results(new_records,search_id):
    #database connection
    con = mdb.connect(host=db_host, db=db_database, passwd=db_pass, user=db_user, port=db_port,charset='utf8', cursorclass=MySQLdb.cursors.DictCursor);
    sql = "INSERT INTO postings(`url`,`date_posted`,`title`,`price`,`search_id`)VALUES(%(url)s,%(create_date)s,%(title)s,%(price)s,{0})".format(search_id)
    with con:
        cur = con.cursor()
        cur.executemany(sql,new_records)

def get_new_records(results,search_id):
    new_records =[]

    #database connection
    con = mdb.connect(host=db_host, db=db_database, passwd=db_pass, user=db_user, port=db_port,charset='utf8', cursorclass=MySQLdb.cursors.DictCursor);

    sql = "SELECT `url` FROM postings where `search_id` = {0}".format(search_id)
    with con:
        cur=con.cursor()
        cur.execute(sql);
        seen_posts = cur.fetchall()
    is_new = False
    for post in results:
        # do any of the already seen posts match this post?
        if any(seen_post['url'] == post['url'] for seen_post in seen_posts):
            #logger.info("this post exists {0} ".format(post))
            pass
        else:
        #   logger.info("NEW POST: {0}".format(post))
            new_records.append(post)
        #logger.info('')
    return new_records


def send_email(email,description,new_records):
    # Not actually sure how this gets used or if it does
    me = "MuffinB0t"

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "New results for {0}".format(description)
    msg['From'] = me
    msg['To'] = email

    text = "Hi!Here is the list of new postings for: {0}".format(description)
    html = """\
    <html>
      <head></head>
      <body>
        <p>Here is a list of new postings for:
        """
    html=html+description+" <br><ul>"
           
    
    logger.info('    building email')
    
    for row in new_records:
                try:                    
                    text = text+row['title']+": "+row['url']+""
                    html = html+"<br><li><a href='"+row['url']+"'>"+row['title']+"</a></li>"
                    
                except Exception as ex:
                    logger.error(str(ex))
                    
    html = html+"""\
            </ul>
        </p>
      </body>
    </html>
    """
    logger.info('    done building email')
    
    # Create the body of the message (a plain-text and an HTML version).

    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')

    part2 = MIMEText(html, 'html')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part1)
    msg.attach(part2)

    # Send the message via local SMTP server.
    server = smtplib.SMTP(email_server)
    server.starttls()
    logger.info('    logging in to smtp server')
    
    server.login(email_email,email_pass)
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    logger.info('    sending email')
    
    try:
        server.sendmail(me, email, msg.as_string())
    except Exception as ex:
        logger.error(str(ex))
    logger.info('    email successfully sent')
    server.quit()
    logger.info('    smtp server successfully quit')
    

if __name__ == '__main__':
    start_time = time.time()
    global logger
    global local_path
    global log_file
    global config_file

    local_path = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(local_path, 'scraper.log')
    config_file = os.path.join(local_path, 'config.ini')

    logger = logging.getLogger('scraper')
    logger.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # create file handler which logs even debug messages
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)

    try:

        #get config settings
        config = configparser.ConfigParser()

        #database config
        global db_host
        global db_database
        global db_user
        global db_pass
        global db_port

        #smtp settings
        global email_server
        global email_email
        global email_pass



        config.read(config_file)
        db_host = config.get('databaseInfo','host')
        db_database = config.get('databaseInfo','database')
        db_user = config.get('databaseInfo','user')
        db_pass = config.get('databaseInfo','pass')
        db_port = config.getint('databaseInfo','port')

        email_server = config.get('smtpInfo','server')
        email_email = config.get('smtpInfo','email')
        email_pass = config.get('smtpInfo','pass')

    except Exception as ex:
        logger.info("Unable to get config values: {0}".format(ex))
        sys.exit()

    logger.info("||||||| Starting new search {0} |||||||".format(time.strftime("%c")))
    logger.info("  getting active searches")
    active_searches = get_active_searches()
    #logger.info(active_searches)
    interate_through_searches(active_searches)
    end_time = time.time()
    execute_duration = end_time - start_time
    logger.info("Execution duration: {0} seconds\n\n".format(int(execute_duration)))



