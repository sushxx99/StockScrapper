"""
Automated Stock Scraper for Trendlyne Top Gainers (3-Month) and Zerodha 5x Leverage
Combines live data from both sources with real-time mapping

Requirements:
pip install selenium pandas openpyxl webdriver-manager

Output: Excel/CSV with columns: Stock Name | NSE | Leverage
"""

import time
import pandas as pd
import logging
import sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Fix Unicode encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_scraper.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

class CombinedStockScraper:
    """Main class to scrape Trendlyne Top Gainers and map with Zerodha 5x leverage"""
    
    def __init__(self):
        self.trendlyne_url = "https://trendlyne.com/stock-screeners/price-based/top-gainers/3-month/index/NIFTY500/nifty-500/"
        self.zerodha_url = "https://zerodha.com/margin-calculator/Equity/"
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_excel = f"Trendlyne_TopGainers_5x_Leverage_{self.timestamp}.xlsx"
        self.output_csv = f"Trendlyne_TopGainers_5x_Leverage_{self.timestamp}.csv"
        
    def setup_driver(self, headless=True):
        """Setup Selenium Chrome driver with options"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    # ============================================================================
    # ZERODHA SCRAPING - Get all stocks with 5x leverage
    # ============================================================================
    
    def scrape_zerodha_leverage(self):
        """
        Scrape Zerodha Margin Calculator for stocks with 5x leverage
        Returns: set of NSE codes with 5x leverage
        """
        logging.info("="*80)
        logging.info("[Task 1] Scraping Zerodha Margin Calculator...")
        logging.info("="*80)
        
        driver = self.setup_driver(headless=True)
        zerodha_5x_set = set()
        all_leverage_data = {}
        
        try:
            logging.info(f"Navigating to: {self.zerodha_url}")
            driver.get(self.zerodha_url)
            
            # Wait for table to load
            logging.info("Waiting for table to load...")
            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tr[data-scrip]"))
            )
            time.sleep(5)
            
            # Scroll down to load all entries
            logging.info("Loading all stocks...")
            for i in range(20):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.5)
            
            # Extract all rows
            rows = driver.find_elements(By.CSS_SELECTOR, "tr[data-scrip]")
            logging.info(f"Found {len(rows)} stocks in Zerodha")
            
            for row in rows:
                try:
                    scrip = row.get_attribute("data-scrip")
                    leverage = row.get_attribute("data-mis_multiplier") or "0"
                    
                    if scrip:
                        scrip = scrip.strip().upper()
                        leverage = leverage.strip()
                        
                        # Store all leverage data
                        all_leverage_data[scrip] = leverage + "x" if leverage.isdigit() else leverage
                        
                        # Collect 5x leverage stocks
                        if leverage == "5":
                            zerodha_5x_set.add(scrip)
                except Exception as e:
                    continue
            
            logging.info(f"✓ Extracted {len(all_leverage_data)} stocks from Zerodha")
            logging.info(f"✓ Stocks with 5x leverage: {len(zerodha_5x_set)}")
            
            return zerodha_5x_set, all_leverage_data
            
        except Exception as e:
            logging.error(f"Error scraping Zerodha: {str(e)}")
            return set(), {}
        
        finally:
            driver.quit()

    # ============================================================================
    # TRENDLYNE SCRAPING - Get top 100 gainers with full names and NSE
    # ============================================================================
    
    def scrape_trendlyne_gainers(self):
        """
        Scrape Trendlyne Top Gainers for 3-Month
        Navigates to each stock to extract full name and NSE code
        Returns: DataFrame with Name, NSE columns
        """
        logging.info("\n" + "="*80)
        logging.info("[Task 2] Scraping Trendlyne Top 100 Gainers (3-Month)...")
        logging.info("="*80)
        
        driver = self.setup_driver(headless=True)
        trendlyne_data = []
        
        try:
            logging.info(f"Navigating to: {self.trendlyne_url}")
            driver.get(self.trendlyne_url)
            
            # Wait for page to load
            logging.info("Waiting for page to load...")
            time.sleep(8)
            
            # Find and click dropdown to show 100 entries
            logging.info("Selecting 100 entries from dropdown...")
            try:
                # Wait for dropdown option
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//option[@value='100']"))
                )
                
                # Click the option to show 100
                option_100 = driver.find_element(By.XPATH, "//option[@value='100']")
                option_100.click()
                logging.info("✓ Selected 100 entries")
                time.sleep(5)
            except Exception as e:
                logging.warning(f"Could not find 100 option dropdown: {str(e)}")
                logging.info("Attempting alternative method...")
            
            # Scroll aggressively to load all 100 entries
            logging.info("Loading all 100 top gainers...")
            for scroll_count in range(30):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.8)
            
            # Find all stock rows
            stock_rows = driver.find_elements(By.XPATH, "//tbody/tr")
            logging.info(f"Found {len(stock_rows)} rows in the table")
            
            # Extract stock links from rows
            stock_links = []
            for row in stock_rows:
                try:
                    link = row.find_element(By.XPATH, ".//a[contains(@href, '/equity/')]")
                    href = link.get_attribute("href")
                    if href:
                        stock_links.append(href)
                except:
                    pass
            
            logging.info(f"Extracted {len(stock_links)} stock links")
            logging.info("Processing each stock to extract full name and NSE code...\n")
            
            # Process each stock link
            for idx, stock_url in enumerate(stock_links[:100], 1):
                try:
                    # Open in new tab
                    driver.execute_script("window.open(arguments[0], '_blank');", stock_url)
                    time.sleep(1)
                    
                    # Switch to new tab
                    original_window = driver.current_window_handle
                    WebDriverWait(driver, 10).until(EC.number_of_windows_to_be(2))
                    driver.switch_to.window(driver.window_handles[-1])
                    time.sleep(2)
                    
                    # Extract full stock name from stock_info_heading
                    full_name = "N/A"
                    try:
                        full_name = driver.find_element(By.CSS_SELECTOR, "span.stock_info_heading").text.strip()
                    except:
                        try:
                            full_name = driver.find_element(By.TAG_NAME, "h1").text.strip()
                        except:
                            pass
                    
                    # Extract NSE code from stock_exchange_details
                    nse_code = "N/A"
                    try:
                        stock_exchange_div = driver.find_element(By.CSS_SELECTOR, "span.stock_exchange_details")
                        nse_text = stock_exchange_div.text
                        
                        # Parse NSE code - format: "NSE: SYMBOLCODE | BSE: 123456 | ASM"
                        lines = nse_text.split("\n")
                        for line in lines:
                            if "NSE:" in line:
                                # Extract just the symbol after "NSE:"
                                nse_part = line.split("NSE:")[1].strip()
                                # Take only the first part before any pipe or special character
                                nse_code = nse_part.split("|")[0].strip()
                                break
                    except:
                        pass
                    
                    # Store data
                    if full_name != "N/A" and nse_code != "N/A":
                        trendlyne_data.append({
                            "Stock Name": full_name,
                            "NSE": nse_code
                        })
                        logging.info(f"  [{idx:3}] {full_name:<50} | NSE: {nse_code}")
                    else:
                        logging.warning(f"  [{idx:3}] Failed to extract - Name: {full_name}, NSE: {nse_code}")
                    
                    # Close tab and switch back to main window
                    driver.close()
                    driver.switch_to.window(original_window)
                    time.sleep(0.5)
                    
                except Exception as e:
                    logging.error(f"  [{idx:3}] Error: {str(e)}")
                    try:
                        if len(driver.window_handles) > 1:
                            driver.close()
                            driver.switch_to.window(driver.window_handles[0])
                    except:
                        pass
                    continue
            
            logging.info(f"\n✓ Successfully extracted {len(trendlyne_data)} top gainers from Trendlyne")
            return pd.DataFrame(trendlyne_data)
            
        except Exception as e:
            logging.error(f"Error scraping Trendlyne: {str(e)}")
            return pd.DataFrame(columns=["Stock Name", "NSE"])
        
        finally:
            driver.quit()

    # ============================================================================
    # MAP AND FILTER - Add Leverage Column
    # ============================================================================
    
    def map_leverage(self, trendlyne_df, zerodha_5x_set):
        """
        Map Trendlyne NSE codes to Zerodha 5x leverage
        Returns: DataFrame with Leverage column added
        """
        logging.info("\n" + "="*80)
        logging.info("[Task 3] Mapping Zerodha 5x Leverage to Trendlyne Stocks...")
        logging.info("="*80)
        
        result_df = trendlyne_df.copy()
        
        # Add Leverage column - 5x if in Zerodha's 5x set, else NA
        result_df['Leverage'] = result_df['NSE'].apply(
            lambda x: '5x' if x.upper() in zerodha_5x_set else 'NA'
        )
        
        # Reorder columns
        result_df = result_df[['Stock Name', 'NSE', 'Leverage']]
        
        # Count results
        count_5x = len(result_df[result_df['Leverage'] == '5x'])
        count_na = len(result_df[result_df['Leverage'] == 'NA'])
        
        logging.info(f"✓ Mapping complete!")
        logging.info(f"  • Stocks with 5x leverage: {count_5x}")
        logging.info(f"  • Stocks with NA leverage: {count_na}")
        
        return result_df

    # ============================================================================
    # SAVE RESULTS
    # ============================================================================
    
    def save_results(self, result_df):
        """Save results to Excel and CSV files"""
        logging.info("\n" + "="*80)
        logging.info("[Task 4] Saving Results...")
        logging.info("="*80)
        
        try:
            # Separate into 5x and NA, but keep original order within each group
            result_df_5x = result_df[result_df['Leverage'] == '5x'].reset_index(drop=True)
            result_df_na = result_df[result_df['Leverage'] == 'NA'].reset_index(drop=True)
            
            # Concatenate: 5x first (in Trendlyne order), then NA (in Trendlyne order)
            result_df_sorted = pd.concat([result_df_5x, result_df_na], ignore_index=True)
            result_df_clean = result_df_sorted[['Stock Name', 'NSE', 'Leverage']].copy()
            
            # Save to Excel
            result_df_clean.to_excel(self.output_excel, index=False, engine='openpyxl')
            logging.info(f"✓ Saved to Excel: {self.output_excel}")
            
            # Save to CSV
            result_df_clean.to_csv(self.output_csv, index=False, encoding='utf-8-sig')
            logging.info(f"✓ Saved to CSV: {self.output_csv}")
            
            return result_df_clean
        except Exception as e:
            logging.error(f"Error saving files: {str(e)}")
            return result_df

    # ============================================================================
    # DISPLAY RESULTS
    # ============================================================================
    
    def display_results(self, result_df):
        """Display results in console"""
        print("\n" + "="*100)
        print("FINAL RESULTS - TRENDLYNE TOP GAINERS MAPPED WITH ZERODHA 5X LEVERAGE")
        print("="*100)
        print(f"\n{result_df[['Stock Name', 'NSE', 'Leverage']].to_string(index=False)}")
        print("\n" + "="*100)

    # ============================================================================
    # MAIN EXECUTION
    # ============================================================================
    
    def run(self):
        """Main execution"""
        print("\n" + "="*100)
        print("COMBINED STOCK SCRAPER - TRENDLYNE TOP GAINERS + ZERODHA 5X LEVERAGE")
        print("="*100)
        
        start_time = time.time()
        
        # Step 1: Scrape Zerodha (faster)
        zerodha_5x_set, all_leverage_data = self.scrape_zerodha_leverage()
        
        if not zerodha_5x_set:
            logging.error("Failed to scrape Zerodha. Aborting.")
            return False
        
        # Step 2: Scrape Trendlyne Top 100 Gainers
        trendlyne_df = self.scrape_trendlyne_gainers()
        
        if trendlyne_df.empty:
            logging.error("Failed to scrape Trendlyne. Aborting.")
            return False
        
        # Step 3: Map Leverage
        result_df = self.map_leverage(trendlyne_df, zerodha_5x_set)
        
        # Step 4: Save Results
        result_df_sorted = self.save_results(result_df)
        
        # Step 5: Display Results
        self.display_results(result_df_sorted)
        
        # Summary
        execution_time = time.time() - start_time
        logging.info("\n" + "="*80)
        logging.info("EXECUTION SUMMARY")
        logging.info("="*80)
        logging.info(f"Total Trendlyne Top Gainers: {len(trendlyne_df)}")
        logging.info(f"Total Zerodha Stocks Checked: {len(all_leverage_data)}")
        logging.info(f"Stocks with 5x Leverage: {len(result_df[result_df['Leverage'] == '5x'])}")
        logging.info(f"Stocks with NA Leverage: {len(result_df[result_df['Leverage'] == 'NA'])}")
        logging.info("="*80)
        
        return True


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    scraper = CombinedStockScraper()
    success = scraper.run()
    
    if success:
        print("\n✅ Scraping completed successfully!")
    else:
        print("\n❌ Scraping failed!")