import requests
from bs4 import BeautifulSoup
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
import time

def setup_driver():
    chrome_options = webdriver.ChromeOptions()
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def go_to_page(driver, url):
    driver.get(url)
    time.sleep(2)

def click_element(driver, xpath):
    element = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, xpath)))
    actions = ActionChains(driver)
    actions.move_to_element(element).click().perform()
    time.sleep(2)

def parse_players(driver):
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    table = soup.find('div', {'id': 'league-players'}).find('table')
    tbody = table.find('tbody')
    players = []
    for row in tbody.find_all('tr'):
        cols = row.find_all('td')
        player_data = [
            cols[1].text.strip(),  # Player
            cols[2].text.strip(),  # Team
            cols[3].text.strip(),  # Apps
            cols[4].text.strip(),  # Min
            cols[5].text.strip(),  # G
            cols[6].text.strip()   # A
        ]
        players.append(player_data)
    return players

def go_to_page_number(driver, page_number):
    page_xpath = f'//div[@class="table-control-panel"]//ul[@class="pagination"]//li[@data-page="{page_number}"]'
    click_element(driver, page_xpath)

def get_total_pages(driver):
    pagination = driver.find_element(By.XPATH, '//div[@class="table-control-panel"]//ul[@class="pagination"]')
    pages = pagination.find_elements(By.CLASS_NAME, 'page')
    return len(pages)

def get_nationality_from_wikipedia(player_name):
    search_url = f"https://en.wikipedia.org/wiki/{player_name.replace(' ', '_')}"
    response = requests.get(search_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    mw_page_container = soup.find('div', class_='mw-page-container')
    if mw_page_container:
        mw_content_container = mw_page_container.find('div', class_='mw-content-container')
        if mw_content_container:
            body_content = mw_content_container.find('div', id='bodyContent')
            if body_content:
                mw_content_text = body_content.find('div', id='mw-content-text')
                if mw_content_text:
                    mw_content_ltr = mw_content_text.find('div', class_='mw-content-ltr mw-parser-output')
                    if mw_content_ltr:
                        infobox = mw_content_ltr.find('table', class_='infobox infobox-table vcard')
                        if infobox:
                            rows = infobox.find_all('td')
                            for row in rows:
                                if 'national team' in row.text.lower() or 'nationality' in row.text.lower():
                                    nationality_link = row.find('a')
                                    if nationality_link:
                                        return nationality_link.text.strip()
    return "Unknown"

def main():
    driver = setup_driver()
    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    all_players = []
    try:
        for year in years:
            url = f'https://understat.com/league/EPL/{year}'
            go_to_page(driver, url)
            click_element(driver, "//div[@class='filters']//*[contains(text(), 'All')]")
            driver.execute_script("""
                var fButton = document.querySelector("ul.custom-select-options li[rel='F']");
                if (fButton) {
                    fButton.click();
                }
            """)
            time.sleep(2)
            click_element(driver, "//button[@id='players-filter']")
            time.sleep(2)
            total_pages = get_total_pages(driver)
            for page in range(1, total_pages + 1):
                go_to_page_number(driver, page)
                players = parse_players(driver)
                for player in players:
                    player_name = player[0]
                    nationality = get_nationality_from_wikipedia(player_name)
                    player.append(nationality)
                all_players.extend(players)
                print(f"Спарсены данные со страницы: {page} за {year} год")
        with open('players.tsv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerows(all_players)
        print("Данные успешно сохранены в файл players_data.tsv")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()