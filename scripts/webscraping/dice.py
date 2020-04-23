import bs4
import requests
from selenium import webdriver
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile

from db.database_controller import Database
import os
import re


BASE_URL = "https://www.dice.com/jobs?q={}&countryCode=US&radius=30&radiusUnit=mi&page={}&pageSize=100&language=en"
MAX_TRIES_JOB_LISTING_URLS = 50
MAX_TRIES_JOB_DATA = 10
DATABASE_CONNECT_DATA = ("", "", "")


def get_html(url):
    html = requests.get(url)
    return html.text


def get_job_listing_urls(search_query, page_number, webdriver):
    url = BASE_URL.format(search_query, page_number)
    tries = 0

    while True:
        # Get url contents
        webdriver.get(url)
        html = webdriver.page_source
        soup = bs4.BeautifulSoup(html, "html.parser")
        content_search_results = soup.find_all(attrs={"class": "card-title-link bold"})

        # Create list of job listings from url contents
        job_listing_urls = []
        
        for job_listing in content_search_results:
            url = job_listing.get('href')
            url = url.split('?')[0]
            job_listing_urls.append(url)

        # Returns list if succeeded, otherwise retries for x times and returns None
        if job_listing_urls:
            print("[0] Retrieved job listing URLs for query '{}' on page '{}'".format(search_query, page_number))
            return job_listing_urls
        else:
            if tries < MAX_TRIES_JOB_LISTING_URLS:
                tries += 1
                print("[!] Could not retrieve job listing URLs for query '{}' on page '{}', trying again ({})".format(search_query, page_number, tries))
            else:
                return None


def get_job_id(job_id_soup):
    for content in job_id_soup:
        for descendant in content.descendants:
            if isinstance(descendant, bs4.element.NavigableString):
                if str(descendant.string[0:14]) == "Position Id : ":
                    try:
                        return int(descendant.string[14:])
                    except:
                        return 0
    return 0


def get_search_term():
    exists_sql = """SELECT EXISTS(SELECT * FROM searchterm WHERE checked = False)"""
    select_sql = """SELECT name FROM searchterm WHERE checked = False"""
    with Database("job_database", "postgres", "1234567890") as db:
        db.execute(exists_sql)
        term_exists = db.fetchone()[0]
        if term_exists == True:
            db.execute(select_sql)
            return db.fetchone()[0]
        else:
            return None


def get_job_data(url, try_count=0):
    soup = bs4.BeautifulSoup(get_html(url), "html.parser")

    try:
        if try_count > MAX_TRIES_JOB_DATA:
            return None
        else:
            # Job id
            job_id_soup = soup.find_all(attrs={"class": "col-md-12"})
            job_id = get_job_id(job_id_soup)
            
            # Position title class
            position_title_soup = soup.find_all(attrs={"class": "jobTitle"})[0]
            position_title = position_title_soup.contents[0]

            # Company title
            company_title_soup = soup.find_all(attrs={"id": "hiringOrganizationName"})[0]
            company_title = company_title_soup.contents[0]
            
            # Job description
            job_description_soup = soup.find_all(attrs={"id": "jobdescSec"})[0]
            job_description = "\n".join([descendant.string for descendant in job_description_soup.descendants if isinstance(descendant, bs4.element.NavigableString) and descendant.string not in ["", " ", "\n"]])
            for header in ['"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd"','html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"']:
                job_description = job_description.replace(header, '')
            job_description = re.sub("\s\s+", ' ', job_description)

            if job_description[:4].lower() == "html":
                job_description = job_description[4:]

            # Skills
            skills_soup = soup.find_all(attrs={"id": "estSkillText"})[0]
            skills = skills_soup.get("value")


            job_data = {"job_id": job_id,
                        "position_title": position_title,
                        "company_title": company_title,
                        "job_description": job_description,
                        "skills": skills}

            print("[0] Retrieved job data")

            return job_data
    except IndexError:
        print("[!] IndexError while retrieving job data, trying again ({})".format(try_count))
        get_job_data(url, try_count + 1)


def add_job_data(job_data):
    try:
        exists_sql = """SELECT EXISTS(SELECT 1 FROM job WHERE description = %s)"""
        add_sql = """INSERT INTO job (position_title, company_title, description, skills) VALUES (%s, %s, %s, %s)"""
        with Database(DATABASE_CONNECT_DATA[0], DATABASE_CONNECT_DATA[1], DATABASE_CONNECT_DATA[2]) as db:
            db.execute(exists_sql, (job_data["job_description"],))
            desc_exists = db.fetchone()[0]
            if desc_exists == False:
                values = (job_data["position_title"], job_data["company_title"], job_data["job_description"], job_data["skills"])
                db.execute(add_sql, values)
                db.commit()
        print("[0] Job data added to database successfully")
    except:
        print("[!] Failed to add job data to database")


def add_search_terms(position_title):
    try:
        terms = [term for term in position_title.split(" ") if term]
        exists_sql = """SELECT EXISTS(SELECT * FROM searchterm WHERE name = %s)"""
        add_sql = """INSERT INTO searchterm (name) VALUES (%s)"""

        for term in terms:
            with Database(DATABASE_CONNECT_DATA[0], DATABASE_CONNECT_DATA[1], DATABASE_CONNECT_DATA[2]) as db:
                db.execute(exists_sql, (term,))
                term_exists = db.fetchone()[0]
                if term_exists == False:
                    db.execute(add_sql, (term,))
                    db.commit()
        print("[0] Search data added to database successfully")
    except:
        print("[!] Failed to add search terms to database")


def search_terms_is_empty():
    sql = """SELECT EXISTS(SELECT * FROM searchterm)"""

    with Database(DATABASE_CONNECT_DATA[0], DATABASE_CONNECT_DATA[1], DATABASE_CONNECT_DATA[2]) as db:
        db.execute(sql)
        result = db.fetchone()[0]
    
    return not result


def set_search_term_checked(term, bool_value):
    sql = """UPDATE searchterm SET checked = %s WHERE name = %s"""

    with Database(DATABASE_CONNECT_DATA[0], DATABASE_CONNECT_DATA[1], DATABASE_CONNECT_DATA[2]) as db:
        db.execute(sql, (bool_value, term,))
        db.commit()


def main(webdriver):
    while True:
        # 1. Get search terms
        if search_terms_is_empty() == False:
            search_query = get_search_term()
            if search_query == None:
                print("[!] All search terms are checked, stopping program")
                break
        else:
            search_query = ""

        # 2. Get job listing urls
        for page_number in range(1, 101):
            job_listing_urls = get_job_listing_urls(search_query, page_number, webdriver)

            if job_listing_urls == None:
                print("[!] Error receiving URLs for query '{}' on page '{}', skipping this page".format(search_query, page_number))
                continue

            # 3. Get job data from job listing urls
            for url in job_listing_urls:
                job_data = get_job_data(url)

                if job_data == None:
                    print("[!] Error receiving job data for URL '{}' of '{}', skipping this URL".format(job_listing_urls.index(url), len(job_listing_urls)))
                    continue

                # 4. Save data
                add_job_data(job_data)
                add_search_terms(job_data["position_title"])

            if len(job_listing_urls) < 100:
                print("[0] Amount of results for query '{}' on page '{}' was below 100, skipping next pages".format(search_query, page_number))
                break
        
        set_search_term_checked(search_query, True)


if __name__ == "__main__":
    # Selenium requirements
    firefox_profile = FirefoxProfile()
    firefox_profile.set_preference("geo.enabled", False)

    os.environ['MOZ_HEADLESS'] = '1'

    firefox_options = FirefoxOptions()
    firefox_options.add_argument('--headless')

    webdriver = webdriver.Firefox(firefox_profile=firefox_profile, options=firefox_options)
    webdriver.implicitly_wait(4)

    # Start program
    main(webdriver)

    # Stop after program is done
    webdriver.stop_client()
    webdriver.quit()