import os
import re
import requests
import markdownify
import validators
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
from typing import List, Dict, Optional, Set
from pathlib import Path
from dags.util.file_manager import FilePathManager


def is_valid_url(url: str) -> bool:
    validation = validators.url(url)
    return validation


def get_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    """
    Extracts relevant internal links from a BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML content.
        base_url (str): The base URL of the page.

    Returns:
        List[str]: A list of absolute URLs on the same domain, filtered for relevance.
    """
    CONTENT_CONTAINERS = ['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 
                         'li', 'td', 'th', 'blockquote', 'div.content']
    
    links = []
    parsed_base = urlparse(base_url)
    
    # Find main content containers
    for container in CONTENT_CONTAINERS:
        for element in soup.find_all(container):
            # Skip elements in navigation sections that might have slipped through
            if element.find_parent(['nav', 'header', 'footer']):
                continue
                
            for link in element.find_all('a', href=True):
                href = link.get('href')
                absolute_url = urljoin(base_url, href)
                
                # Normalize URL
                parsed_link = urlparse(absolute_url)
                clean_url = parsed_link._replace(
                    query="", 
                    fragment="",
                    path=parsed_link.path.rstrip('/') 
                ).geturl()
                
                if parsed_link.netloc == parsed_base.netloc:
                    links.append(clean_url)
    
    return list(set(links)) 

def scrape_page(url: str) -> Optional[Dict]:
    """
    Scrapes the content of a single web page, extracts the main content, title, and links.

    Args:
        url (str): The URL of the page to scrape.

    Returns:
        Optional[Dict[str, str]]: A dictionary containing the URL, title, content, and links,
                        or None if the scraping fails.
    """
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; AirflowCrawler/1.0)'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        main_content = soup.find('main') or soup.find('article') or soup.find('body')
        if not main_content:
            logging.warning(f"No main content found on {url}")
            return None
            
        for elem in main_content(['nav', 'footer', 'script', 'style']):
            elem.decompose()
        
        title_tag = soup.find('title')
        title = title_tag.text.strip() if title_tag else ''

        return {
            'url': url,
            'title': soup.find('title').text.strip() if soup.find('title') else '',
            'content': markdownify.markdownify(str(main_content)),
            'links': [urljoin(url, l['href']) for l in soup.find_all('a', href=True)]
        }
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to scrape {url}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while scraping {url}: {e}")
        return None     


def save_as_markdown(content: Dict[str, str], project_name: str) -> Optional[Path]:
    """
    Saves the scraped content as a Markdown file.

    Args:
        content (Dict[str, str]): A dictionary containing the page's URL, title, and content.
        project_name (str): The name of the project.

    Returns:
        Optional[Path]: The path to the saved Markdown file, or None on error.
    """
    try:
        if not content or not content.get('content'):
            logging.warning(f"No content to save for {content.get('url')}")
            return None

        title = re.sub(r'[^\w\-_]', '_', content['title'])[:50]
        filename = f"{title}.md"

        # Get versioned path
        file_path = FilePathManager.get_project_filepath(
            project_name=project_name,
            file_name=filename,
            dataset_path=Path("dataset")
        )

        if not file_path:
            return None

        # Create markdown content
        md_content = f"# {content['title']}\n\n"
        md_content += f"**Source**: [{content['url']}]({content['url']})\n\n"
        md_content += content['content']

        # Write to file
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(md_content, encoding='utf-8')
        logging.info(f"Saved content from {content['url']} to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Failed to save markdown for {content.get('url')}: {e}")
        return None

def process_website(**kwargs) -> List[str]:
    """
    Crawls a website, scrapes content, and saves it as Markdown files.

    Args:
        base_url (str): The starting URL for the crawl.
        project_name (str): The name of the project.
        max_depth (int): The maximum depth to crawl.
        max_queue_size (int): The maximum number of URLs to keep in the queue.

    Returns:
       List[str]: A list of filepaths that were saved.
    """
    base_url = kwargs['base_url']
    project_name = kwargs['project_name']
    max_depth = kwargs['max_depth']
    max_queue_size = kwargs['max_queue_size']


    if not is_valid_url(base_url):
        raise ValueError(f"Invalid base URL: {base_url}")

    parsed_base = urlparse(base_url)
    visited: Set[str] = set()
    seen: Set[str] = set()  
    queue = deque([(base_url, 0)])
    saved_files: List[str] = []

    seen.add(base_url)
    
    while queue:
        if not queue:
            logging.info("Queue is empty, crawl complete.")
            break
            
        current_url, depth = queue.popleft()

        if depth > max_depth or current_url in visited:
            continue

        try:
            visited.add(current_url)
            logging.info(f"Processing {current_url} (depth {depth})")

            # Scrape page content
            content = scrape_page(current_url)
            if content:
                file_path = save_as_markdown(content, project_name)
                if file_path:
                    saved_files.append(str(file_path))

                soup = BeautifulSoup(requests.get(current_url).content, 'html.parser')
                new_links = get_links(soup, current_url)
                
                for link in new_links:
                    # Check if we've already seen or processed this URL
                    if link not in seen and link not in visited:
                        if len(queue) < max_queue_size:
                            queue.append((link, depth + 1))
                            seen.add(link)  # Mark as seen
                            logging.debug(f"Added to queue: {link}")
                        else:
                            logging.info(f"Queue full, skipping: {link}")
                    else:
                        logging.debug(f"Skipping duplicate: {link}")

        except Exception as e:
            logging.error(f"Error processing {current_url}: {e}")


    kwargs['ti'].xcom_push(key='saved_files', value=saved_files)

"""
def process_website(base_url: str, project_name: str, max_depth: int = 1, max_queue_size: int = 10) -> List[str]:
    if not is_valid_url(base_url):
        raise ValueError(f"Invalid base URL: {base_url}")

    parsed_base = urlparse(base_url)
    visited: Set[str] = set()
    queue = deque([(base_url, 0)])
    saved_files = []

    while queue:
        if not queue:
            break
            
        current_url, depth = queue.popleft()

        if depth > max_depth or current_url in visited:
            continue

        try:
            visited.add(current_url)
            logging.info(f"Processing {current_url} (depth {depth})")

            # Scrape page content
            content = scrape_page(current_url)
            if content:
                # Save markdown
                file_path = save_as_markdown(content, project_name)
                if file_path:
                    saved_files.append(str(file_path))

                asset_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.css', '.js', '.ico', '.xml', '.pdf', '.svg'}
                for link in content['links']:
                    parsed_link = urlparse(link)
                    if parsed_link.netloc == parsed_base.netloc and link not in visited:
                        if not any(link.lower().endswith(ext) for ext in asset_extensions):
                            if len(queue) < max_queue_size:
                                queue.append((link, depth + 1))
                            else:
                                logging.info(f"Queue full, not adding (potential content link): {link}")
                        else:
                            logging.info(f"Skipping asset link: {link}")

        except Exception as e:
            logging.error(f"Error processing {current_url}: {e}")

    return saved_files
"""

# --------------------------
# DAG Configuration
# --------------------------

default_args = {
    'owner': 'airflow',
    'depands_on_past': False,
    'email_on_failure': True,
    'email': ['alimurad7777@gmail.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'website_docs_crawler',
    default_args=default_args,
    description='A website documentation crawler that saves content as markdown',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 26),
    tags=['web_scraping']
)


crawl_website_task = PythonOperator(
    task_id='crawl_website',
    python_callable=process_website,
    op_kwargs={
        'base_url': 'https://en.wikipedia.org/wiki/Aristotle',
        'project_name': 'docs_crawler',
        'max_depth': 3,
        'max_queue_size': 20
    },
    dag=dag,
    provide_context=True,
)

crawl_website_task



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    base_url = 'https://en.wikipedia.org/wiki/Aristotle'
    #base_url = 'https://medium.com/@prithvijit.guha245/hello-world-airflow-docker-9102f4c5305b'
    project_name = "docs_crawler"
    max_depth = 2 

    print(f"Starting the direct processing for: {base_url}")
    saved_files = process_website(base_url, project_name, max_depth)

    if saved_files:
        print("\nSuccessfully processed and saved the following files:")
        for file in saved_files:
            print(f"- {file}")
    else:
        print("\nNo files were processed or saved.")

    print("\nCheck the 'markdown_docs/aristotle_docs' directory for the saved Markdown file.")