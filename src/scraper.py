import time
import argparse
import requests
import json
import validators
import pprint
import re

from urllib.parse import urlparse, parse_qs, urlencode
from random import gauss

from bs4 import BeautifulSoup
from collections import defaultdict

SCRAPE_TIMEOUT_SECONDS = 3

SCHOLAR_BASE_URL = "https://scholar.google.com/"

MAKE_HEADERS = lambda: {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:131.0) Gecko/20100101 Firefox/131.0"
}

page_layout_tags = {
    "citation_table": "gsc_a_b",
    "work_info_row": "gsc_a_t",
    "work_info_table": "gsc_oci_table",
    "field_name": "gsc_oci_field",
    "field_value": "gsc_oci_value",
    "articles": "gsc_oci_merged_snippet",
    "title_link_section": "gsc_oci_title_gg",
    'title_links':'gsc_oci_title_ggi'
}

valid_work_fields = [
    "authors",
    "publication_date",
    "conference",
    "pages",
    "publisher",
    "description",
    "total_citations",
    "scholar_articles"
]


def default_processor(soup):
    return soup.text


def process_text_date(soup):
    import dateutil.parser as parser

    return parser.parse(soup.text, yearfirst=True, dayfirst=False).strftime("%Y-%m-%d")


def process_authors(soup):
    return [author.strip() for author in soup.text.split(",")]


def process_citations(soup):
    total_citations = int(soup.div.a.text.replace("Cited by ", ""))
    return total_citations

def process_scholar_articles(soup: BeautifulSoup):
    articles_soup = soup.findAll('div', class_=page_layout_tags["articles"], recursive=False)

    if articles_soup is None:
        return None
    
    article_data = []
    for article_soup in articles_soup:
        sections_soup = article_soup.findAll('div', recursive=False)
        if sections_soup is not None:
            final_section_links = sections_soup[-1].findAll('a')
            final_section_dict = {
                'related_articles': None,
                'citations': None,
                'versions': None
            }

            for final_section_link in final_section_links:
                if 'cited' in final_section_link.text.lower():
                    final_section_dict['citations'] = final_section_link
                elif 'versions' in final_section_link.text.lower():
                    final_section_dict['versions'] = final_section_link
                elif 'related' in final_section_link.text.lower():
                    final_section_dict['related_articles'] = final_section_link

            data = {
                'name': sections_soup[0].a.text,
                'link': sections_soup[0].a['href'],
                'author_blob': sections_soup[1].text
            }

            if final_section_dict['versions'] is not None:
                data['version_count'] = int(final_section_dict['versions'].text.split(' ')[1])
                data['versions_link'] = final_section_dict['versions']['href']
            
            if final_section_dict['citations'] is not None:
                data['citation_count'] = int(final_section_dict['citations'].text.replace('Cited by ', ''))
                data['cited_by'] = final_section_dict['citations']['href']
            
            if final_section_dict['related_articles'] is not None:
                data['related_articles'] = final_section_dict['related_articles']['href']

            article_data.append(data)
    return article_data


field_processors = defaultdict(lambda: default_processor)
field_processors["publication_date"] = process_text_date
field_processors["authors"] = process_authors
field_processors["total_citations"] = process_citations
field_processors["scholar_articles"] = process_scholar_articles

parser = argparse.ArgumentParser(
    prog="scholar_scraper",
    description="Scraper google scholar for given input",
    epilog="made by ed :3",
)

parser.add_argument(
    "-f",
    "--file",
    help="CSV file To use as input, use with -c option to specify the column name to use",
)
parser.add_argument(
    "-c", "--column", help="Column name with google scholar profile links"
)
parser.add_argument("-o", "--output", help="output json path")
parser.add_argument("-l", "--link")


def read_page_and_get_soup(url, session=None):
    if session is None:
        session = requests.Session()
    html_doc = session.get(url, headers=MAKE_HEADERS()).text
    return BeautifulSoup(html_doc, "html.parser"), session


def extract_links_from_csv(path, column):
    from pandas import read_csv

    return read_csv(path)[column].to_list()


def rand_sleep():
    time.sleep(SCRAPE_TIMEOUT_SECONDS + gauss(mu=0.0, sigma=0.5))


def get_all_works_from_person_page(link, page_size=100, session=None):
    works_list = []
    make_page_params = lambda start: {"cstart": start, "pagesize": page_size}

    if session is None:
        session = requests.Session()

    counter = 0
    show_more = True
    while show_more:
        if counter > 0:
            rand_sleep()
        print(f"STARTING ITER: {counter}")
        paged_url = f"{link}&{urlencode(make_page_params(start=counter*page_size))}"
        counter += 1

        print(f"{counter}: {paged_url}")

        response = session.post(paged_url, headers=MAKE_HEADERS())
        person_soup = BeautifulSoup(response.text, "html.parser")
        works = get_works_from_person_soup(person_soup)
        works_list.append(works)

        show_more = len(works) == page_size

    return [work for works in works_list for work in works], session


def get_works_from_person_soup(person_soup):
    works = []
    for table_row in person_soup.body.find(
        id=page_layout_tags["citation_table"]
    ).findAll("tr", recursive=False):
        work = {}
        for table_column in table_row.findAll("td", recursive=False):
            if page_layout_tags["work_info_row"] in table_column["class"]:
                work["title"] = table_column.a.text
                work["link"] = table_column.a["href"]
        works.append(work)
    return works


def scrape_work_data(work_soup: BeautifulSoup, work):
    work = work.copy()
    for work_field in work_soup.find(id=page_layout_tags["work_info_table"]).findAll(
        "div", recursive=False
    ):
        field_name = (
            work_field.find("div", {"class": page_layout_tags["field_name"]})
            .text.lower()
            .replace(" ", "_")
        )
        field_value = work_field.find("div", {"class": page_layout_tags["field_value"]})

        if field_name in valid_work_fields:
            field_value = field_processors[field_name](field_value)
            if field_value is not None:
                work[field_name] = field_value

    title_link_soup = work_soup.find()
    link_section_soup = title_link_soup.find('div', {'id': page_layout_tags["title_link_section"]})

    if link_section_soup is not None:
        links = []
        links_soup = link_section_soup.findAll('div', {'class': page_layout_tags['title_links']})
        for link_soup in links_soup:
            link_data = {}
            if ' from ' in link_soup.text.lower():
                link_data['type'] = link_soup.text.replace('[', '').replace(']', '').lower().split(' from ')[0]
                link_data['location'] = link_soup.text.split(' from ')[-1]
                link_data['link'] = link_soup.a['href']
            elif 'get it at' in link_soup.text.lower():
                link_data['type'] = 'page'
                link_data['location'] = link_soup.text.split(' at ')[-1]
                link_data['link'] = link_soup.a['href']
            elif link_soup.text.lower() == 'full view':
                pass
            else:
                print("doesnt follow pattern!!!!!", link_soup.prettify())
            links.append(link_data) 
        work['title_links'] = links

    return work


def get_name_from_person(person_soup):
    return person_soup.find(id="gsc_prf_in").text


def get_scholar_id_from_url(url):
    # Parse the URL
    parsed_url = urlparse(url)

    # Get the query parameters as a dictionary
    query_params = parse_qs(parsed_url.query)
    return query_params["user"][0]


if __name__ == "__main__":
    args = parser.parse_args()
    url_list = []
    if args.file is not None and args.column is not None:
        url_list = extract_links_from_csv(args.file, args.column)
    elif args.link:
        url_list = [args.link]

    all_data = []
    for url in url_list:
        if validators.url(url) and "semanticscholar" not in url:
            print(f"Starting to Parse: {url}")

            person_soup, session = read_page_and_get_soup(url)
            name = get_name_from_person(person_soup)
            scholar_id = get_scholar_id_from_url(url)

            rand_sleep()
            print(f"Getting works for: {name} at {scholar_id}")
            works, session = get_all_works_from_person_page(url, session=session)
            for i, work in enumerate(works):
                print(f"Scraping work: {i}: {work['title']}")
                work_page, session = read_page_and_get_soup(
                    SCHOLAR_BASE_URL + work["link"], session=session
                )
                work = scrape_work_data(work_page, work)
                works[i] = work
                rand_sleep()

            all_data.append(
                {"name": name, "scholar_id": scholar_id, "publications": works}
            )
        else:
            print(f"Skipping: {url}")
    if args.output is not None:
        with open(args.output, "w") as outfile:
            json.dump(all_data, outfile, indent=2, sort_keys=False)
    else:
        pprint.pprint(all_data)
