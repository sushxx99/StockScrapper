"""
Telegram Bot - Wrapper for script5.py
Runs your existing script and returns filtered results
"""

import requests
import time
import subprocess
import os
import glob
import logging
from datetime import datetime
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
TELEGRAM_BOT_TOKEN = '8443913816:AAHdiOM5sey7mCX86z7OIzu4jV87aEFfm_M'
TELEGRAM_API_URL = f'https://api.telegram.org/bot8443913816:AAHdiOM5sey7mCX86z7OIzu4jV87aEFfm_M'

SCRIPT_PATH = r'C:\Users\sush7\OneDrive\Desktop\WebScrapping\script5.py'
PYTHON_311_PATH = r'C:\Users\sush7\AppData\Local\Programs\Python\Python311\python.exe'

LAST_UPDATE_ID = 0


def run_script5():
    """Run script5.py and wait for it to complete"""
    logger.info("Running script5.py...")
    try:
        # Run the script
        result = subprocess.run(
            [PYTHON_311_PATH, SCRIPT_PATH],
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes timeout
        )
        
        logger.info("Script execution completed")
        return True
    except subprocess.TimeoutExpired:
        logger.error("Script timeout (10 minutes)")
        return False
    except Exception as e:
        logger.error(f"Error running script: {e}")
        return False


def get_latest_csv():
    """Find the most recently created CSV file from script5"""
    try:
        # Look for CSV files in the script directory
        csv_files = glob.glob(os.path.join(
            r'C:\Users\sush7\OneDrive\Desktop\WebScrapping',
            '*.csv'
        ))
        
        if not csv_files:
            return None
        
        # Get the most recently modified file
        latest_csv = max(csv_files, key=os.path.getmtime)
        logger.info(f"Found CSV: {latest_csv}")
        return latest_csv
    except Exception as e:
        logger.error(f"Error finding CSV: {e}")
        return None


def read_csv_data(csv_file):
    """Read CSV and return as list of dicts"""
    try:
        df = pd.read_csv(csv_file)
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        return None


def send_message(chat_id, text):
    """Send text message"""
    url = f'{TELEGRAM_API_URL}/sendMessage'
    try:
        requests.post(url, json={'chat_id': chat_id, 'text': text}, timeout=10)
        logger.info(f"Message sent to {chat_id}")
    except Exception as e:
        logger.error(f"Error sending message: {e}")


def send_document(chat_id, file_path):
    """Send CSV file to user"""
    url = f'{TELEGRAM_API_URL}/sendDocument'
    try:
        with open(file_path, 'rb') as f:
            files = {'document': f}
            requests.post(url, data={'chat_id': chat_id}, files=files, timeout=30)
        logger.info(f"Document sent to {chat_id}")
    except Exception as e:
        logger.error(f"Error sending document: {e}")


def get_updates(offset=0):
    """Get new messages from Telegram"""
    url = f'{TELEGRAM_API_URL}/getUpdates'
    try:
        resp = requests.get(url, params={'offset': offset, 'timeout': 30}, timeout=40)
        return resp.json().get('result', [])
    except Exception as e:
        logger.error(f"Error getting updates: {e}")
        return []


def format_stocks(stocks):
    """Format stocks for display"""
    message = "Top Gainers with Leverage\n\n"
    message += "Stock Name                    NSE      Leverage\n"
    message += "-" * 60 + "\n"
    
    for idx, stock in enumerate(stocks, 1):
        # Handle different column names
        name = stock.get('Stock Name', stock.get('Full_Name', 'N/A'))
        nse = stock.get('NSE', stock.get('NSE_Code', 'N/A'))
        lev = stock.get('Leverage', 'N/A')
        
        name_str = str(name)[:28].ljust(28)
        nse_str = str(nse)[:8].ljust(8)
        lev_str = str(lev)
        
        message += f"{idx:2}. {name_str} {nse_str} {lev_str}\n"
    
    return message


def process_message(message_text, chat_id):
    """Process incoming messages"""
    text = message_text.strip().lower()
    
    if text == '/start':
        send_message(chat_id, """Stock Scraper Bot

Send a number (1-100) to get top X gainers:
Example: 20 (for top 20 gainers)

Commands:
/top10 - Top 10
/top25 - Top 25
/top50 - Top 50
/all - All 100
/refresh - Run fresh scrape""")
    
    elif text == '/refresh':
        send_message(chat_id, "Running scraper... This may take 5-10 minutes. Please wait...")
        
        success = run_script5()
        
        if success:
            time.sleep(2)
            csv_file = get_latest_csv()
            if csv_file:
                send_message(chat_id, "Scraping complete! Getting data...")
            else:
                send_message(chat_id, "Scraping complete but couldn't find CSV file")
        else:
            send_message(chat_id, "Error running scraper. Try again.")
    
    elif text in ['/top10', '/top25', '/top50', '/all']:
        limits = {'/top10': 10, '/top25': 25, '/top50': 50, '/all': 100}
        limit = limits[text]
        
        send_message(chat_id, f"Fetching top {limit}...")
        
        # Run script
        success = run_script5()
        
        if not success:
            send_message(chat_id, "Error running scraper")
            return
        
        time.sleep(2)
        
        # Get CSV
        csv_file = get_latest_csv()
        if not csv_file:
            send_message(chat_id, "Error: CSV file not found")
            return
        
        # Read data
        data = read_csv_data(csv_file)
        if not data:
            send_message(chat_id, "Error reading data")
            return
        
        # Limit and format
        stocks = data[:limit]
        message = format_stocks(stocks)
        send_message(chat_id, message)
        
        # Send CSV
        send_document(chat_id, csv_file)
    
    elif text.isdigit():
        limit = int(text)
        if 1 <= limit <= 100:
            send_message(chat_id, f"Fetching top {limit} gainers...")
            
            # Run script
            success = run_script5()
            
            if not success:
                send_message(chat_id, "Error running scraper")
                return
            
            time.sleep(2)
            
            # Get CSV
            csv_file = get_latest_csv()
            if not csv_file:
                send_message(chat_id, "Error: CSV file not found")
                return
            
            # Read data
            data = read_csv_data(csv_file)
            if not data:
                send_message(chat_id, "Error reading data")
                return
            
            # Limit and format
            stocks = data[:limit]
            message = format_stocks(stocks)
            send_message(chat_id, message)
            
            # Send CSV
            send_document(chat_id, csv_file)
        else:
            send_message(chat_id, "Please send a number between 1 and 100")
    
    else:
        send_message(chat_id, "Unknown command. Send /start for help")


def main():
    """Main bot loop"""
    global LAST_UPDATE_ID
    
    if TELEGRAM_BOT_TOKEN == 'YOUR_TELEGRAM_BOT_TOKEN_HERE':
        print("ERROR: Replace TELEGRAM_BOT_TOKEN with your actual token!")
        return
    
    if not os.path.exists(SCRIPT_PATH):
        print(f"ERROR: script5.py not found at {SCRIPT_PATH}")
        return
    
    logger.info("Bot started. Waiting for messages...")
    
    while True:
        try:
            updates = get_updates(LAST_UPDATE_ID)
            
            for update in updates:
                LAST_UPDATE_ID = update['update_id'] + 1
                
                if 'message' in update and 'text' in update['message']:
                    chat_id = update['message']['chat']['id']
                    text = update['message']['text']
                    logger.info(f"Message from {chat_id}: {text}")
                    process_message(text, chat_id)
            
            time.sleep(1)
        
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            time.sleep(5)


if __name__ == '__main__':
    main()