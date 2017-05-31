#!/usr/bin/env python

import psycopg2
import datetime as u_day


# This function deals with the queries for Error% > 1
def err():
    # This query creates a view for the desired result
    quiry = """CREATE or replace VIEW e_c AS
            SELECT time::timestamp::date AS date,
                COUNT(CASE WHEN status LIKE '%200%' THEN 1 END) AS success,
                COUNT(CASE WHEN status LIKE '%404%' THEN 1 END) AS fail,
                COUNT(method) AS total
            FROM log
            GROUP BY date
            """
    run_query(quiry, 0)  # This function executes the above query
    # This query finds the % of error
    quiry = """
            SELECT date,100.0* fail/total as f_p
            FROM e_c
            WHERE 100.0 * fail/total > 1
            ORDER BY f_p DESC
            """
    answ = run_query(quiry)  # This function executes the above query

    d = u_day.datetime.strptime(str(answ[0][0]),
                                '%Y-%m-%d').strftime("%B %d, %Y")
    print("{} - {}% errors".format(d, round(answ[0][1], 2)))
    return answ


# This function finds the top 3 popular articles of all time
def pop_article():
    # query to create view for finding articles
    quiry = """CREATE or replace VIEW pop_article AS
            SELECT articles.title,
                COUNT(CASE WHEN log.status LIKE '%200%' THEN 1 END) AS view
            FROM articles LEFT JOIN log
            ON log.path LIKE '%' || articles.slug
            GROUP BY articles.title;
            """
    run_query(quiry, 0)  # This function executes the above query
    # query to fet the result
    quiry = """
            SELECT title, view
            FROM pop_article
            ORDER BY view DESC
            LIMIT 3
            """
    answ = run_query(quiry)  # This function executes the above query

    for i in range(len(answ)):
        print("{} - {} views".format(answ[i][0], answ[i][1]))
    return answ


# function to execute the query
def run_query(quiry, temp=1):
    # connection to the db
    database, curs = connection()
    answ = None
    # execution of query
    curs.execute(quiry)
    if temp:
        answ = curs.fetchall()
    database.commit()
    database.close()
    return answ


# function to sort the authors acc. to popularity
def pop_author():
    # query to create view for popular authors
    quiry = """CREATE or replace VIEW pop_art AS
            SELECT SUBSTRING(path,10) AS path
            FROM log WHERE path LIKE '/arti%'
            """
    run_query(quiry, 0)  # This function executes the above query
    # query to create another view
    quiry = """CREATE or replace VIEW total_count AS
            SELECT articles.author
                ,COUNT(*)
            FROM articles JOIN pop_art ON articles.slug = pop_art.path
            GROUP BY articles.author ORDER BY COUNT(*) DESC
            """
    run_query(quiry, 0)  # This function executes the above query
    # query to create another view
    quiry = """CREATE or replace VIEW popular_author AS
            SELECT authors.name, authors.id
            FROM authors JOIN articles
            ON articles.author = authors.id
            GROUP BY authors.id
            """
    run_query(quiry, 0)  # This function executes the above query
    # query to get the result
    quiry = """SELECT name, count
            FROM popular_author,total_count
            where popular_author.id = total_count.author
            """
    answ = run_query(quiry)  # This function executes the above query

    for i in range(len(answ)):
        print("{} - {} views".format(answ[i][0], answ[i][1]))
    return answ


# function to create connection to the db
def connection(data_name="news"):
    database = psycopg2.connect("dbname = news")
    curs = database.cursor()
    return database, curs

if __name__ == '__main__':
    # array containing output messages
    mess = ["3 most popular articles of all time:",
            "\nmost popular article authors of all time:",
            "\nDays with more than 1% of requests leading to errors:"]
    # array containing names of functions
    func = [pop_article, pop_author, err]
    j = 0
    for i in mess:
        print(i)
        func[j]()
        j = j + 1
