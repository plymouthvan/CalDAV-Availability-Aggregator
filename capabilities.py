#!/usr/bin/env python3
"""
CalDAV Server Capabilities Checker

This script connects to a CalDAV server and checks for supported
synchronization methods (sync-token, ctag, etc.) to help users
configure their sources.yml file correctly.
"""

import asyncio
import aiohttp
import argparse
import logging
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def check_capabilities(url: str, username: str, password: str):
    """
    Connects to the CalDAV server and checks for sync capabilities.
    """
    logging.info(f"Connecting to {url}...")
    
    auth = aiohttp.BasicAuth(login=username, password=password)
    
    async with aiohttp.ClientSession(auth=auth) as session:
        # 1. Check for general CalDAV support via OPTIONS
        try:
            async with session.options(url) as response:
                if response.status != 200:
                    logging.error(f"Server returned status {response.status}. Is this a valid CalDAV URL?")
                    return

                dav_header = response.headers.get('DAV', '')
                if '1' not in dav_header or 'calendar-access' not in dav_header:
                    logging.warning("Server does not explicitly announce CalDAV support in OPTIONS header.")
                else:
                    logging.info("Server appears to support CalDAV (calendar-access).")

        except aiohttp.ClientError as e:
            logging.error(f"Connection failed: {e}")
            return

        # 2. Use PROPFIND to discover specific features
        propfind_body = """<?xml version="1.0" encoding="utf-8" ?>
        <D:propfind xmlns:D="DAV:"
                    xmlns:C="urn:ietf:params:xml:ns:caldav"
                    xmlns:CS="http://calendarserver.org/ns/">
          <D:prop>
            <D:supported-report-set />
            <CS:getctag />
          </D:prop>
        </D:propfind>
        """
        
        headers = {'Content-Type': 'application/xml; charset=utf-8', 'Depth': '0'}

        logging.info("Querying server for supported features...")
        try:
            async with session.request('PROPFIND', url, data=propfind_body, headers=headers) as response:
                if response.status not in [200, 207]:
                    logging.error(f"PROPFIND request failed with status {response.status}.")
                    return
                
                content = await response.text()
                await parse_propfind_response(content)

        except aiohttp.ClientError as e:
            logging.error(f"PROPFIND request failed: {e}")
        except ET.ParseError:
            logging.error("Failed to parse XML response from server.")

async def parse_propfind_response(xml_content: str):
    """Parses the PROPFIND XML response to identify capabilities."""
    try:
        root = ET.fromstring(xml_content)
        
        namespaces = {
            'D': 'DAV:',
            'C': 'urn:ietf:params:xml:ns:caldav',
            'CS': 'http://calendarserver.org/ns/'
        }

        supported_reports = root.findall('.//D:supported-report/D:report/D:sync-collection', namespaces)
        ctag_prop = root.find('.//CS:getctag', namespaces)

        print("\n--- Sync Capabilities ---")
        
        # Check for sync-token support
        if supported_reports:
            print("✅ Supported Sync Method: sync-token")
            print("   (This is the most efficient method. Use 'sync_method: sync-token' in sources.yml)")
        else:
            print("❌ Unsupported Sync Method: sync-token")

        # Check for ctag support
        if ctag_prop is not None:
            print("✅ Supported Sync Method: ctag")
            print("   (Good fallback if sync-token is not available. Use 'sync_method: ctag' in sources.yml)")
        else:
            print("❌ Unsupported Sync Method: ctag")
            
        print("\n--- Recommendations ---")
        if supported_reports:
            print("RECOMMENDATION: Use 'sync-token'. It provides the most reliable and efficient sync.")
        elif ctag_prop is not None:
            print("RECOMMENDATION: Use 'ctag'. It's less efficient than sync-token but still effective.")
        else:
            print("WARNING: Neither 'sync-token' nor 'ctag' support was detected.")
            print("This tool may not be able to sync efficiently with this server.")
            print("A full, slow comparison will be required on every sync cycle.")

    except ET.ParseError as e:
        logging.error(f"Could not parse server response: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Check a CalDAV server for supported synchronization features."
    )
    parser.add_argument("url", help="The full URL of the CalDAV calendar.")
    parser.add_argument("-u", "--username", required=True, help="Your CalDAV username.")
    parser.add_argument("-p", "--password", required=True, help="Your CalDAV password.")
    
    args = parser.parse_args()

    # Basic URL validation
    parsed_url = urlparse(args.url)
    if not all([parsed_url.scheme, parsed_url.netloc]):
        logging.error("Invalid URL provided. Please include the scheme (e.g., https://).")
        return

    asyncio.run(check_capabilities(args.url, args.username, args.password))


if __name__ == "__main__":
    main()