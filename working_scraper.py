#!/usr/bin/env python3
"""
Working MLB Player Props Scraper using proven Chrome setup
"""
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import json
import logging
import re
from datetime import datetime

# Configuration
LOGIN_URL = "https://evanalytics.com/login"
MODELS_URL = "https://evanalytics.com/mlb/models/players"
USERNAME = "alexcnickel@me.com"
PASSWORD = "Football25!"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def init_browser():
    """Initialize Chrome with proven working options"""
    options = webdriver.ChromeOptions()

    # Use options that worked in basic test
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')

    # Additional options for stability
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    # Images enabled to detect Expected Value indicators
    # JavaScript enabled for login form

    # Use the working chromedriver path
    chromedriver_path = "/Users/doug/new_scraper/chromedriver-mac-arm64/chromedriver"
    service = Service(chromedriver_path)

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)

    logger.info("Chrome driver initialized successfully")
    return driver


def login_simple(driver):
    """Simple login without complex waits"""
    try:
        logger.info("Starting login process...")
        driver.get(LOGIN_URL)
        time.sleep(5)  # Simple wait instead of complex WebDriverWait

        # Debug: save login page source
        with open("debug_login_page.html", "w") as f:
            f.write(driver.page_source)
        logger.info("Saved login page source to debug_login_page.html")

        # Find and fill email (name="username" according to HTML)
        email_field = driver.find_element(By.NAME, "username")
        email_field.clear()
        email_field.send_keys(USERNAME)
        logger.info("Email entered")
        time.sleep(1)

        # Find and fill password (name="pwd" according to HTML)
        password_field = driver.find_element(By.NAME, "pwd")
        password_field.clear()
        password_field.send_keys(PASSWORD)
        logger.info("Password entered")
        time.sleep(1)

        # Click login button (just a simple button with "Submit" text)
        login_button = driver.find_element(By.XPATH, "//button[text()='Submit']")
        login_button.click()
        logger.info("Login button clicked")

        # Wait for login to complete
        time.sleep(10)

        logger.info("Login completed")
        return True

    except Exception as e:
        logger.error(f"Login failed: {e}")
        return False


def scrape_basic():
    """Basic scraping approach with minimal complexity"""
    driver = None
    all_data = []

    try:
        # Initialize browser
        driver = init_browser()

        # Login
        if not login_simple(driver):
            logger.error("Login failed - stopping")
            return []

        # Navigate to models page
        logger.info("Navigating to models page...")
        driver.get(MODELS_URL)
        time.sleep(10)  # Give page time to load

        # Use the proven pagination logic
        logger.info("Starting pagination scraping...")

        wait = WebDriverWait(driver, 10)
        page_num = 1

        while True:
            try:
                # Wait for table rows to be present (using proven selector)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr td")))
            except Exception as e:
                logger.error(f"Timeout waiting for table rows on page {page_num}: {e}")
                logger.debug(f"Page source preview: {driver.page_source[:1000]}")
                break

            # Get headers (case-insensitive match for EV) and rows
            headers = [th.text.strip() for th in driver.find_elements(By.CSS_SELECTOR, "table thead tr th")]
            rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            row_count = len(rows)

            # Locate EV column index flexibly
            ev_col_index = None
            for j, header in enumerate(headers):
                if 'expected value' in header.lower():  # case-insensitive match
                    ev_col_index = j
                    break

            logger.info(f"Scraped page {page_num} with {row_count} rows.")

            # Process each row
            for i, row in enumerate(rows):
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) != len(headers):
                    continue

                # Build base row data
                values = [col.text.strip() for col in cols]
                row_data = dict(zip(headers, values))

                # Extract EV images and always produce JSON blob
                if ev_col_index is not None:
                    try:
                        html_content = cols[ev_col_index].get_attribute('innerHTML')
                        images = cols[ev_col_index].find_elements(By.TAG_NAME, "img")
                        image_info = []
                        for img in images:
                            src = img.get_attribute('src')
                            if src:
                                image_info.append({
                                    'type': 'img',
                                    'src': src,
                                    'alt': img.get_attribute('alt') or '',
                                    'title': img.get_attribute('title') or '',
                                    'class': img.get_attribute('class') or ''
                                })
                        ev_data = {
                            'text': '',
                            'images': image_info,
                            'html': html_content
                        }
                        row_data['EXPECTED VALUE'] = json.dumps(ev_data)

                        # Immediately parse into a tier
                        row_data['EV_TIER'] = parse_expected_value(row_data['EXPECTED VALUE'])
                    except Exception:
                        logger.debug(f"Failed to extract EV column for row {i + 1} on page {page_num}")

                # Add metadata
                row_data['scrape_timestamp'] = datetime.now().isoformat()
                row_data['page_number'] = page_num

                all_data.append(row_data)

            # Check if we've reached the final page
            if row_count < 250:
                logger.info("Fewer than 250 rows found — reached final page.")
                break

            # Navigate to next page
            try:
                next_buttons = driver.find_elements(By.ID, "nextButton")
                if not next_buttons:
                    logger.info("Next button not found — stopping.")
                    break

                next_button = next_buttons[0]
                first_row = rows[0]
                driver.execute_script("arguments[0].click();", next_button)
                page_num += 1
                WebDriverWait(driver, 2).until(EC.staleness_of(first_row))
                time.sleep(0.2)
            except Exception as e:
                logger.info(f"Pagination ended: {e}")
                logger.debug(f"Page source preview: {driver.page_source[:1000]}")
                break

        logger.info(f"Scraping completed. Total rows: {len(all_data)}")

    except Exception as e:
        logger.error(f"Scraper failed: {e}")

    finally:
        if driver:
            driver.quit()
            logger.info("Browser closed")

    return all_data


def parse_expected_value(ev_data):
    """Parse Expected Value information from image source URLs"""
    try:
        parsed = json.loads(ev_data)
    except (ValueError, TypeError):
        return 'F'  # assume no data => lowest tier

    images = parsed.get('images', [])
    if not images:
        return 'F'

    # strip query parameters, lowercase
    src = images[0].get('src', '').lower().split('?', 1)[0]
    if 'plus_e' in src:
        return 'A'
    if 'plus_d' in src:
        return 'B'
    if 'plus_c' in src:
        return 'C'
    if 'plus_b' in src:
        return 'D'
    if 'plus_a' in src:
        return 'E'
    return 'F'


def save_data(data, filename=None):
    """Save data to JSON file"""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mlb_data_{timestamp}.json"

    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Data saved to {filename}")
        return filename
    except Exception as e:
        logger.error(f"Failed to save data: {e}")
        return None


if __name__ == "__main__":
    logger.info("Starting MLB scraper...")
    data = scrape_basic()
    if data:
        filename = save_data(data)
        print(f"\nScraping Summary:")
        print(f"Total records: {len(data)}")
        if filename:
            print(f"Data saved to: {filename}")
        if data:
            print(f"Sample record: {data[0]}")
            print(f"Column names: {list(data[0].keys())}")
    else:
        print("No data scraped")
