import time
import json
import csv
import argparse
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

class ElasticCloudScraper:
    def __init__(self, headless=False, timeout=20):
        self.headless = headless
        self.timeout = timeout
        
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        try:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=chrome_options
            )
            self.driver.set_page_load_timeout(self.timeout)
            print("WebDriver initialized successfully")
        except Exception as e:
            print(f"Error initializing WebDriver: {e}")
            raise

    def navigate_to_main_page(self):
        url = "https://cloud.elastic.co/cloud-pricing-table"
        print(f"Navigating to: {url}")
        
        try:
            self.driver.get(url)
            
            WebDriverWait(self.driver, self.timeout).until(
                lambda d: "Elastic Cloud Pricing" in d.title
            )
            print(f"Page loaded: {self.driver.current_url}")
            
            try:
                cookie_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-test-id='acceptAllCookies']"))
                )
                cookie_button.click()
                print("Clicked cookie acceptance button")
                time.sleep(1)
            except:
                print("No cookie banner found or could not click it")
            
            return True
        except Exception as e:
            print(f"Error navigating to main page: {e}")
            return False
    
    def find_element_by_label(self, label_text):
        try:
            label = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, f"//label[contains(text(), '{label_text}')]"))
            )
            
            label_id = label.get_attribute("id")
            
            if label_id and "-label" in label_id:
                base_id = label_id.replace("-label", "")
                
                button = self.driver.find_element(By.CSS_SELECTOR, f"button[id='{base_id}-button']")
                return button
            
            form_row = label.find_element(By.XPATH, "./ancestor::div[contains(@class, 'euiFormRow')]")
            button = form_row.find_element(By.CSS_SELECTOR, "button.euiSuperSelectControl")
            return button
        except Exception as e:
            print(f"Error finding element with label '{label_text}': {e}")
            return None
    
    def find_deprecated_toggle(self):
        try:
            toggle = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//span[contains(text(), 'Show deprecated SKUs')]/preceding-sibling::button"))
            )
            return toggle
        except Exception as e:
            print(f"Error finding 'Show deprecated SKUs' toggle: {e}")
            try:
                switches = self.driver.find_elements(By.CSS_SELECTOR, ".euiSwitch__button")
                for switch in switches:
                    parent = switch.find_element(By.XPATH, "./..")
                    label_element = parent.find_element(By.CSS_SELECTOR, ".euiSwitch__label")
                    if "deprecated" in label_element.text.lower():
                        return switch
            except:
                pass
            return None
    
    def get_cloud_providers(self):
        providers = []
        
        try:
            provider_dropdown = self.find_element_by_label("Cloud provider")
            
            if not provider_dropdown:
                print("Could not find Cloud Provider dropdown")
                return []
            
            current_provider_text = provider_dropdown.text.strip()
            print(f"Current selected provider: {current_provider_text}")
            
            self.driver.save_screenshot("before_provider_click.png")
            print("Screenshot saved to before_provider_click.png")
            
            provider_dropdown.click()
            print("Clicked cloud provider dropdown")
            
            time.sleep(2)
            
            self.driver.save_screenshot("after_provider_click.png")
            print("Screenshot saved to after_provider_click.png")
            
            provider_options = self.driver.find_elements(By.CSS_SELECTOR, ".euiSuperSelect__listbox button[role='option']")
            print(f"Found {len(provider_options)} provider options")
            
            with open("provider_dropdown_source.html", "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            print("Saved provider dropdown HTML to provider_dropdown_source.html")
            
            for option in provider_options:
                try:
                    provider_id = option.get_attribute("id")
                    
                    provider_text = option.text.strip()
                    
                    if provider_id and provider_text:
                        providers.append({
                            "id": provider_id,
                            "name": provider_text
                        })
                        print(f"Found provider: {provider_id} - {provider_text}")
                except Exception as e:
                    print(f"Error processing provider option: {e}")
            
            try:
                self.driver.find_element(By.TAG_NAME, "body").click()
            except:
                pass
            
            return providers
        except Exception as e:
            print(f"Error getting cloud providers: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def select_provider(self, provider_id):
        try:
            provider_dropdown = self.find_element_by_label("Cloud provider")
            
            if not provider_dropdown:
                print(f"Could not find Cloud Provider dropdown to select {provider_id}")
                return False
            
            current_provider_text = provider_dropdown.text.strip()
            print(f"Current provider: {current_provider_text}")
            
            provider_dropdown.click()
            print("Clicked provider dropdown")
            time.sleep(2)
            
            provider_option = self.driver.find_element(By.CSS_SELECTOR, f"button[id='{provider_id}']")
            provider_option.click()
            print(f"Selected provider: {provider_id}")
            time.sleep(3)
            
            return True
        except Exception as e:
            print(f"Error selecting provider {provider_id}: {e}")
            return False
    
    def get_regions_for_current_provider(self):
        regions = []
        
        try:
            region_dropdown = self.find_element_by_label("Region")
            
            if not region_dropdown:
                print("Could not find Region dropdown")
                return []
            
            current_region_text = region_dropdown.text.strip()
            print(f"Current selected region: {current_region_text}")
            
            self.driver.save_screenshot("before_region_click.png")
            print("Screenshot saved to before_region_click.png")
            
            region_dropdown.click()
            print("Clicked region dropdown")
            
            time.sleep(2)
            
            self.driver.save_screenshot("after_region_click.png")
            print("Screenshot saved to after_region_click.png")
            
            region_options = self.driver.find_elements(By.CSS_SELECTOR, ".euiSuperSelect__listbox button[role='option']")
            print(f"Found {len(region_options)} region options")
            
            with open("region_dropdown_source.html", "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            print("Saved region dropdown HTML to region_dropdown_source.html")
            
            current_url = self.driver.current_url
            provider_param = current_url.split("provider=")[1].split("&")[0] if "provider=" in current_url else None
            
            for option in region_options:
                try:
                    region_id = option.get_attribute("id")
                    
                    region_text = option.text.strip()
                    
                    region_name = region_text
                    region_code = ""
                    
                    if "(" in region_text and ")" in region_text:
                        region_code = region_text.split("(")[1].split(")")[0].strip()
                        name_part = region_text.split("(")[0].strip()
                        
                        if len(name_part) > 3:
                            start_idx = 0
                            for i in range(len(name_part)):
                                if name_part[i].isalpha():
                                    start_idx = i
                                    break
                            region_name = name_part[start_idx:].strip()
                    
                    url = f"https://cloud.elastic.co/cloud-pricing-table?productType=stack_hosted&provider={provider_param}&region={region_id}"
                    
                    regions.append({
                        "id": region_id,
                        "name": region_name,
                        "code": region_code,
                        "raw_text": region_text,
                        "url": url
                    })
                    
                    print(f"Found region: {region_name} (ID: {region_id})")
                except Exception as e:
                    print(f"Error processing region option: {e}")
            
            try:
                self.driver.find_element(By.TAG_NAME, "body").click()
            except:
                pass
            
            return regions
        except Exception as e:
            print(f"Error getting regions: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def select_region(self, region_id):
        try:
            region_dropdown = self.find_element_by_label("Region")
            
            if not region_dropdown:
                print(f"Could not find Region dropdown to select {region_id}")
                return False
            
            current_region_text = region_dropdown.text.strip()
            print(f"Current region: {current_region_text}")
            
            region_dropdown.click()
            print("Clicked region dropdown")
            time.sleep(2)
            
            region_option = self.driver.find_element(By.CSS_SELECTOR, f"button[id='{region_id}']")
            region_option.click()
            print(f"Selected region: {region_id}")
            time.sleep(3)
            
            return True
        except Exception as e:
            print(f"Error selecting region {region_id}: {e}")
            return False
    
    def toggle_deprecated_skus(self, show=True):
        try:
            toggle = self.find_deprecated_toggle()
            
            if not toggle:
                print("Could not find 'Show deprecated SKUs' toggle")
                return False
            
            is_checked = toggle.get_attribute("aria-checked") == "true"
            print(f"Current 'Show deprecated SKUs' state: {is_checked}")
            
            if (show and not is_checked) or (not show and is_checked):
                toggle.click()
                print(f"Toggled 'Show deprecated SKUs' to {show}")
                time.sleep(2)
            else:
                print(f"'Show deprecated SKUs' already set to {show}")
            
            return True
        except Exception as e:
            print(f"Error toggling 'Show deprecated SKUs': {e}")
            return False
    
    def extract_pricing_table(self, provider_name, region_name, region_code):
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
            )
            
            self.driver.save_screenshot(f"pricing_table_{provider_name}_{region_name}.png")
            
            headers = []
            header_elements = self.driver.find_elements(By.CSS_SELECTOR, "table th")
            
            if not header_elements:
                print("No table headers found")
                return []
            
            for header in header_elements:
                headers.append(header.text.strip())
            
            pricing_data = []
            row_elements = self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            
            for row in row_elements:
                row_data = {
                    "cloud_provider": provider_name,
                    "region": region_name,
                    "region_code": region_code
                }
                
                cells = row.find_elements(By.TAG_NAME, "td")
                
                if not cells:
                    continue
                
                row_data["product"] = cells[0].text.strip()
                
                tiers = ["standard", "gold", "platinum", "enterprise"]
                for i, tier in enumerate(tiers):
                    cell_index = i + 1
                    if cell_index < len(cells) - 1:
                        row_data[tier] = cells[cell_index].text.strip()
                    else:
                        row_data[tier] = ""
                
                if len(cells) > 1:
                    row_data["unit"] = cells[-1].text.strip()
                else:
                    row_data["unit"] = ""
                
                pricing_data.append(row_data)
            
            print(f"Extracted {len(pricing_data)} pricing entries")
            return pricing_data
        except Exception as e:
            print(f"Error extracting pricing table: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def discover_all_pricing(self, with_deprecated=True, output_csv="elastic_pricing.csv"):
        all_pricing_data = []
        
        if not self.navigate_to_main_page():
            print("Failed to load the main page")
            return all_pricing_data
        
        self.toggle_deprecated_skus(with_deprecated)
        
        providers = self.get_cloud_providers()
        
        if not providers:
            print("No providers found")
            return all_pricing_data
        
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['cloud_provider', 'region', 'region_code', 'product', 'standard', 'gold', 'platinum', 'enterprise', 'unit']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for provider in providers:
                provider_id = provider["id"]
                provider_name = provider["name"]
                
                print(f"\nDiscovering regions for {provider_name} ({provider_id})...")
                
                if not self.select_provider(provider_id):
                    print(f"Failed to select provider {provider_id}, skipping...")
                    continue
                
                regions = self.get_regions_for_current_provider()
                
                for region in regions:
                    region_id = region["id"]
                    region_name = region["name"]
                    region_code = region["code"] if "code" in region else ""
                    
                    print(f"\nExtracting pricing for {provider_name} - {region_name} ({region_id})...")
                    
                    if not self.select_region(region_id):
                        print(f"Failed to select region {region_id}, skipping...")
                        continue
                    
                    pricing_data = self.extract_pricing_table(provider_name, region_name, region_code)
                    
                    for item in pricing_data:
                        writer.writerow(item)
                        all_pricing_data.append(item)
                    
                    print(f"Wrote {len(pricing_data)} entries for {provider_name} - {region_name}")
        
        print(f"Total pricing entries: {len(all_pricing_data)}")
        return all_pricing_data
    
    def close(self):
        if self.driver:
            self.driver.quit()

def main():
    parser = argparse.ArgumentParser(description='Elastic Cloud Price Scraper')
    parser.add_argument('--output', type=str, default='elastic_pricing.csv', 
                       help='Output CSV file (default: elastic_pricing.csv)')
    parser.add_argument('--headless', action='store_true', 
                       help='Run in headless mode (default: false, shows browser)')
    parser.add_argument('--no-deprecated', action='store_true',
                       help='Do not show deprecated SKUs (default: show deprecated SKUs)')
    
    args = parser.parse_args()
    
    scraper = None
    try:
        print("Starting Elastic Cloud price scraping...")
        scraper = ElasticCloudScraper(headless=args.headless)
        pricing_data = scraper.discover_all_pricing(
            with_deprecated=not args.no_deprecated,
            output_csv=args.output
        )
        
        print(f"\nScraping completed. Extracted {len(pricing_data)} pricing entries.")
        print(f"Data saved to {args.output}")
    except Exception as e:
        print(f"An error occurred during scraping: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    main()