import os 
import sys
from dags.zhelper_functions.web_utils import FilePathManager
from bs4 import BeautifulSoup
from pathlib import Path
from typing import List, Dict, Optional
import logging
import requests
import json
import validators

# TODO: Go to each website take all the links 
#       Each website, store its content in json format
#       Convert the json file to markdown format
#       Save the markdown file in a directory


class PageScraper:
    """Handles scraping of individual page elements"""
    
    @staticmethod
    def get_title(soup: BeautifulSoup) -> str:
        title = soup.find('title')
        return title.text.strip() if title else "No Title Found"
    
    @staticmethod
    def get_metadata(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
        meta_tags = soup.find_all('meta')
        return {
            meta.get('name', meta.get('property', '')).lower(): meta.get('content')
            for meta in meta_tags
            if meta.get('name') or meta.get('property')
        }
    
    @staticmethod
    def get_paragraphs(soup: BeautifulSoup) -> List[str]:
        paragraphs = soup.find_all(['p', 'div'])
        return [p.get_text().strip() for p in paragraphs if p.get_text().strip()]
    
    @staticmethod
    def get_headings(soup: BeautifulSoup) -> Dict[str, List[str]]:
        headings: Dict[str, List[str]] = {}
        for level in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            elements = soup.find_all(level)
            headings[level] = [h.get_text().strip() for h in elements]
        return headings
    
    @staticmethod
    def get_links(self, soup: BeautifulSoup) -> List[str]:
        links = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if validators.url(href):
                links.append(href)
        
        return links
    
    @staticmethod
    def get_images(soup: BeautifulSoup) -> List[Dict]:
        return [{
            "src": img.get('src', ''),
            "alt": img.get('alt', ''),
            "width": img.get('width'),
            "height": img.get('height'),
            "class": img.get('class', [])
        } for img in soup.find_all('img')]




class WebsiteScraper: 
    """
    Scrapes documents from websites and saves them to a specified directory.
    """
    def __init__(
            self,
            base_url: str,
            output_dir: Path = Path("dataset"),
            project_name: str = None,
            max_depth: int = 3
    ): 
        self.base_url = base_url
        self.output_dir = output_dir
        self.project_name = project_name
        self.max_depth = max_depth
        self.visited_urls = set()

    def is_valid_url(self, url: str) -> bool:
        validation = validators.url(url)

        return validation


    def scrape_page(self, url: str, current_depth: int = 0) -> Optional[Dict]:
        if current_depth > self.max_depth:
            return None 
        
        try: 
            self.visited_urls.add(url)
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
        
            page_data = {
                "url": url,
                "title": PageScraper.get_title(soup),
                "metadata": PageScraper.get_metadata(soup),
                "paragraphs": PageScraper.get_paragraphs(soup),
                "headings": PageScraper.get_headings(soup),
                "images": PageScraper.get_images(soup),
                'depth': current_depth,
                "links": []
                }
            
            # Save the page data to a file
            for link in PageScraper.get_links(soup, url):
                if self.is_valid_url(link):
                    page_data["links"].append(link)
                    # Recursively scrape linked pages
                    linked_data = self.scrape_page(link, current_depth + 1)
                    if linked_data:
                        page_data.setdefault("linked_pages", []).append(linked_data)
            
            self.save_page(page_data)
            return page_data
    
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None
        
    @staticmethod
    def save_page(project_name: str, page_data: Dict) -> None:

        for key, value in page_data.items():
            if page_data[key] == 'title':
                page_title = page_data[value]

        
        file_path = FilePathManager.get_project_filepath(project_name, page_title)

        if not file_path:
            logging.error(f"Failed to get file path for {page_title}")
            return
        else:
            logging.info(f"File path for {page_title} is {file_path}")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(page_data, f, ensure_ascii=False, indent=7)
                logging.info(f"Saved page data to {file_path}")

            

                
if __name__ == "__main__":
    base_url = 'https://en.wikipedia.org/wiki/Aristotle'
    output_dir = Path("dataset")
    project_name = 'docs_crawler'
    
    scraper = WebsiteScraper(base_url, output_dir, project_name)
    scrapped_data = scraper.scrape_page(base_url)
    print(f"Scraped data from {base_url}")
    scraper.save_page(project_name, scrapped_data)
    
        

        




