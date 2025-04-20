from playwright.sync_api import sync_playwright

def extract_folder_ids(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Run headlessly
        page = browser.new_page()
        page.goto(url)
        folder_elements = page.query_selector_all('tr[data-target="doc"]')
        folder_ids = [element.get_attribute('data-id') for element in folder_elements]
        for folder_id in folder_ids:
            print(f"https://drive.google.com/drive/folders/{folder_id}")
        browser.close()

if __name__ == "__main__":
    google_drive_url = 'https://drive.google.com/drive/folders/18-Tvu8pX5ZCLAfZyRujGT8JRa5uRLKUz'
    extract_folder_ids(google_drive_url)