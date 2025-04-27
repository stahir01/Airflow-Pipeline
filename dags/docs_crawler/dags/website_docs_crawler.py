from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from typing import List, Dict, Optional, Set
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import markdownify
import validators
import logging
import re
from collections import deque
from dags.helper_functions import FilePathManager


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

def is_valid_url(self, url: str) -> bool:
    validation = validators.url(url)
    return validation


def get_links(self, soup: BeautifulSoup) -> List[str]:
        links = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if validators.url(href):
                links.append(href)
        
        return links

def scrape_page(url: str) -> Optional[Dict]:
    """Task 3: Scrape individual page content"""
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        main_content = soup.find('main') or soup.find('article') or soup.find('body')
        if not main_content:
            return None
            
        for elem in main_content(['nav', 'footer', 'script', 'style']):
            elem.decompose()
        
        return {
            'url': url,
            'title': soup.find('title').text.strip() if soup.find('title') else '',
            'content': markdownify.markdownify(str(main_content)),
            'links': [urljoin(url, l['href']) for l in soup.find_all('a', href=True)]
        }
        
    except Exception as e:
        print(f"Failed to scrape {url}: {e}")
        return None     

def generate_filepath(project_name: str, page_title: str) -> Path:
    file_path = FilePathManager.get_project_filepath(project_name, page_title)
    
    if not file_path:
        logging.error(f"Failed to get file path for {page_title}")
        return
    else:
        logging.info(f"File path for {page_title} is {file_path}")

    
def save_as_markdown(content: Dict, project_name: str) -> Optional[Path]:
    """Save content to versioned markdown file"""
    try:
        if not content or not content.get('content'):
            return None

        # Generate filename
        title = re.sub(r'[^\w\-_]', '_', content['title'])[:50]
        filename = f"{title}.md"

        # Get versioned path
        file_path = FilePathManager.get_project_filepath(
            project_name=project_name,
            file_name=filename,
            dataset_path=Path("markdown_docs")
        )

        # Create markdown content
        md_content = f"# {content['title']}\n\n"
        md_content += f"**Source**: [{content['url']}]({content['url']})\n\n"
        md_content += content['content']

        # Write to file
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(md_content, encoding='utf-8')
        return file_path
    except Exception as e:
        logging.error(f"Failed to save markdown: {e}")
        return None

def process_website(**kwargs):
    base_url = kwargs['params']['base_url']
    project_name = kwargs['params']['project_name']
    max_depth = kwargs['params']['max_depth']
    
    if not is_valid_url(base_url):
        raise ValueError(f"Invalid base URL: {base_url}")

    visited: Set[str] = set()
    queue = deque([(base_url, 0)])
    saved_files = []

    while queue:
        current_url, depth = queue.popleft()

        if depth > max_depth or current_url in visited:
            continue

        try:
            visited.add(current_url)
            logging.info(f"Processing {current_url} (depth {depth})")

            # Scrape page content
            content = scrape_page(current_url)
            if not content:
                continue

            # Save markdown
            file_path = save_as_markdown(content, project_name)
            if file_path:
                saved_files.append(str(file_path))
                logging.info(f"Saved {current_url} to {file_path}")

            # Add discovered links to queue
            for link in content['links']:
                if link not in visited:
                    queue.append((link, depth + 1))

        except Exception as e:
            logging.error(f"Error processing {current_url}: {e}")

    kwargs['ti'].xcom_push(key='saved_files', value=saved_files)

# --------------------------
# DAG Structure
# --------------------------


process_op = PythonOperator(
    task_id='process_website',
    python_callable=process_website,
    op_kwargs={
        'base_url': 'https://en.wikipedia.org/wiki/Aristotle',
        'project_name': 'philosophy_docs',
        'max_depth': 2
    },
    dag=dag
)


# Define workflow
process_op