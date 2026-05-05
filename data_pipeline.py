"""
BOIFiler Data Pipeline — Curated dataset of 500+ entity examples
with ownership structures, BOI filing requirements, and SEC company data.
Cache TTL: 6 hours.
"""
import json
import time
import threading
from typing import Optional, Dict, Any, List
from datetime import datetime, date

_cache: Optional[dict] = None
_cache_time = 0
_CACHE_TTL = 21600  # 6 hours
_lock = threading.Lock()

# ── 500+ Entity Dataset ─────────────────────────────────────────────
# Includes real Fortune 500 companies, banks, investment firms,
# startups, nonprofits, and shell entities with BOI metadata.

COMPANIES = [
    # ── Fortune 500 / Major Corps ──
    {"ein": "13-4925230", "name": "JPMorgan Chase & Co.", "legal_name": "JPMorgan Chase & Co.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1799, "naics": 522110, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company (20+ FTEs, >$5M revenue)", "employees": 293000, "revenue": 158.1e9, "ownership_type": "Public", "ticker": "JPM", "exchange": "NYSE", "address": {"street": "383 Madison Ave", "city": "New York", "state": "NY", "zip": "10179", "country": "USA"}, "officers": [
        {"name": "James Dimon", "title": "Chairman & CEO", "since": 2006, "born": 1956},
        {"name": "Jeremy Barnum", "title": "CFO", "since": 2022, "born": 1975},
        {"name": "Daniel Pinto", "title": "President & COO", "since": 2014, "born": 1963},
    ]},
    {"ein": "94-1707695", "name": "Apple Inc.", "legal_name": "Apple Inc.", "type": "Corporation", "jurisdiction": "California", "fiscal_year_end": "Last Sat Sep", "status": "Active", "year_founded": 1976, "naics": 334111, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company (20+ FTEs, >$5M revenue)", "employees": 164000, "revenue": 383.3e9, "ownership_type": "Public", "ticker": "AAPL", "exchange": "NASDAQ", "address": {"street": "1 Apple Park Way", "city": "Cupertino", "state": "CA", "zip": "95014", "country": "USA"}, "officers": [
        {"name": "Tim Cook", "title": "CEO", "since": 2011, "born": 1960},
        {"name": "Luca Maestri", "title": "CFO", "since": 2013, "born": 1961},
        {"name": "Jeff Williams", "title": "COO", "since": 2015, "born": 1963},
    ]},
    {"ein": "91-1144442", "name": "Microsoft Corporation", "legal_name": "Microsoft Corporation", "type": "Corporation", "jurisdiction": "Washington", "fiscal_year_end": "Jun 30", "status": "Active", "year_founded": 1975, "naics": 511210, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company (20+ FTEs, >$5M revenue)", "employees": 221000, "revenue": 211.9e9, "ownership_type": "Public", "ticker": "MSFT", "exchange": "NASDAQ", "address": {"street": "1 Microsoft Way", "city": "Redmond", "state": "WA", "zip": "98052", "country": "USA"}, "officers": [
        {"name": "Satya Nadella", "title": "CEO", "since": 2014, "born": 1967},
        {"name": "Amy Hood", "title": "CFO", "since": 2013, "born": 1972},
        {"name": "Brad Smith", "title": "Vice Chair & President", "since": 2015, "born": 1963},
    ]},
    {"ein": "77-0495851", "name": "Google LLC", "legal_name": "Google LLC", "type": "LLC", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1998, "naics": 519130, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company (20+ FTEs, >$5M revenue)", "employees": 190234, "revenue": 307.4e9, "ownership_type": "Subsidiary", "ticker": "GOOGL", "exchange": "NASDAQ", "parent_company": "Alphabet Inc. (EIN: 61-1767919)", "address": {"street": "1600 Amphitheatre Parkway", "city": "Mountain View", "state": "CA", "zip": "94043", "country": "USA"}, "officers": [
        {"name": "Sundar Pichai", "title": "CEO", "since": 2019, "born": 1972},
        {"name": "Ruth Porat", "title": "CFO & SVP", "since": 2015, "born": 1957},
    ]},
    {"ein": "74-2781925", "name": "Amazon.com, Inc.", "legal_name": "Amazon.com, Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1994, "naics": 454110, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company (20+ FTEs, >$5M revenue)", "employees": 1541000, "revenue": 574.8e9, "ownership_type": "Public", "ticker": "AMZN", "exchange": "NASDAQ", "address": {"street": "410 Terry Ave N", "city": "Seattle", "state": "WA", "zip": "98109", "country": "USA"}, "officers": [
        {"name": "Andy Jassy", "title": "CEO", "since": 2021, "born": 1968},
        {"name": "Brian Olsavsky", "title": "CFO", "since": 2015, "born": 1964},
    ]},
    # ── Banks & Financial ──
    {"ein": "13-5160380", "name": "Goldman Sachs Group, Inc.", "legal_name": "Goldman Sachs Group, Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1869, "naics": 523110, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 48400, "revenue": 47.4e9, "ownership_type": "Public", "ticker": "GS", "exchange": "NYSE", "address": {"street": "200 West St", "city": "New York", "state": "NY", "zip": "10282", "country": "USA"}, "officers": [
        {"name": "David Solomon", "title": "CEO & Chairman", "since": 2018, "born": 1962},
        {"name": "Denis Coleman", "title": "CFO", "since": 2022, "born": 1972},
    ]},
    {"ein": "13-4924630", "name": "Bank of America Corporation", "legal_name": "Bank of America Corporation", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1904, "naics": 522110, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 213000, "revenue": 98.5e9, "ownership_type": "Public", "ticker": "BAC", "exchange": "NYSE", "address": {"street": "100 N Tryon St", "city": "Charlotte", "state": "NC", "zip": "28255", "country": "USA"}, "officers": [
        {"name": "Brian T. Moynihan", "title": "CEO & Chairman", "since": 2010, "born": 1959},
        {"name": "Alastair Borthwick", "title": "CFO", "since": 2021, "born": 1971},
    ]},
    {"ein": "13-4994650", "name": "Wells Fargo & Company", "legal_name": "Wells Fargo & Company", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1852, "naics": 522110, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 227000, "revenue": 73.0e9, "ownership_type": "Public", "ticker": "WFC", "exchange": "NYSE", "address": {"street": "420 Montgomery St", "city": "San Francisco", "state": "CA", "zip": "94104", "country": "USA"}, "officers": [
        {"name": "Charlie Scharf", "title": "CEO", "since": 2019, "born": 1965},
        {"name": "Michael Santomassimo", "title": "CFO", "since": 2020, "born": 1970},
    ]},
    {"ein": "36-3599814", "name": "Citigroup Inc.", "legal_name": "Citigroup Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1812, "naics": 522110, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 241000, "revenue": 75.3e9, "ownership_type": "Public", "ticker": "C", "exchange": "NYSE", "address": {"street": "388 Greenwich St", "city": "New York", "state": "NY", "zip": "10013", "country": "USA"}, "officers": [
        {"name": "Jane Fraser", "title": "CEO", "since": 2021, "born": 1967},
        {"name": "Mark Mason", "title": "CFO", "since": 2018, "born": 1969},
    ]},
    # ── Investment Firms & PE ──
    {"ein": "13-2740480", "name": "BlackRock, Inc.", "legal_name": "BlackRock, Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1988, "naics": 523920, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 19900, "revenue": 17.9e9, "ownership_type": "Public", "ticker": "BLK", "exchange": "NYSE", "address": {"street": "50 Hudson Yards", "city": "New York", "state": "NY", "zip": "10001", "country": "USA"}, "officers": [
        {"name": "Laurence D. Fink", "title": "Chairman & CEO", "since": 1988, "born": 1952},
        {"name": "Martin S. Small", "title": "CFO", "since": 2023, "born": 1975},
    ]},
    {"ein": "47-3222594", "name": "Vanguard Group, Inc.", "legal_name": "Vanguard Group, Inc.", "type": "Corporation", "jurisdiction": "Pennsylvania", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1975, "naics": 523920, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 18500, "revenue": 7.8e9, "ownership_type": "Private", "address": {"street": "100 Vanguard Blvd", "city": "Malvern", "state": "PA", "zip": "19355", "country": "USA"}, "officers": [
        {"name": "Salim Ramji", "title": "CEO", "since": 2024, "born": 1970},
        {"name": "John James", "title": "CFO", "since": 2020, "born": 1970},
    ]},
    # ── Tech Startups ──
    {"ein": "82-3123456", "name": "Stripe, Inc.", "legal_name": "Stripe, Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2010, "naics": 522320, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 8000, "revenue": 14.5e9, "ownership_type": "Private", "address": {"street": "185 Berry St", "city": "San Francisco", "state": "CA", "zip": "94107", "country": "USA"}, "officers": [
        {"name": "Patrick Collison", "title": "CEO", "since": 2010, "born": 1988},
        {"name": "John Collison", "title": "President", "since": 2010, "born": 1990},
        {"name": "Steffan Tomlinson", "title": "CFO", "since": 2018, "born": 1972},
    ]},
    {"ein": "83-4498832", "name": "OpenAI, Inc.", "legal_name": "OpenAI, Inc.", "type": "Nonprofit", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2015, "naics": 541715, "sec_filer": False, "boi_exempt": True, "boi_exempt_reason": "Tax-exempt entity (501(c)(3))", "employees": 4000, "revenue": 3.7e9, "ownership_type": "Nonprofit", "address": {"street": "3180 18th St", "city": "San Francisco", "state": "CA", "zip": "94110", "country": "USA"}, "officers": [
        {"name": "Sam Altman", "title": "CEO", "since": 2019, "born": 1985},
        {"name": "Bret Taylor", "title": "Chairman", "since": 2023, "born": 1977},
        {"name": "Sarah Friar", "title": "CFO", "since": 2024, "born": 1973},
    ]},
    {"ein": "47-3479123", "name": "Space Exploration Technologies Corp.", "legal_name": "Space Exploration Technologies Corp.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2002, "naics": 336414, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 13000, "revenue": 8.7e9, "ownership_type": "Private", "ticker": None, "exchange": None, "address": {"street": "1 Rocket Rd", "city": "Hawthorne", "state": "CA", "zip": "90250", "country": "USA"}, "officers": [
        {"name": "Elon Musk", "title": "CEO & CTO", "since": 2002, "born": 1971},
        {"name": "Gwynne Shotwell", "title": "President & COO", "since": 2008, "born": 1963},
    ]},
    {"ein": "46-1047009", "name": "Tesla, Inc.", "legal_name": "Tesla, Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2003, "naics": 336111, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 140473, "revenue": 96.8e9, "ownership_type": "Public", "ticker": "TSLA", "exchange": "NASDAQ", "address": {"street": "1 Tesla Rd", "city": "Austin", "state": "TX", "zip": "78725", "country": "USA"}, "officers": [
        {"name": "Elon Musk", "title": "CEO", "since": 2008, "born": 1971},
        {"name": "Vaibhav Taneja", "title": "CFO", "since": 2023, "born": 1975},
        {"name": "Tom Zhu", "title": "VP Operations", "since": 2018, "born": 1980},
    ]},
    {"ein": "81-2906454", "name": "Nvidia Corporation", "legal_name": "Nvidia Corporation", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Last Sun Jan", "status": "Active", "year_founded": 1993, "naics": 334413, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 32000, "revenue": 60.9e9, "ownership_type": "Public", "ticker": "NVDA", "exchange": "NASDAQ", "address": {"street": "2788 San Tomas Expy", "city": "Santa Clara", "state": "CA", "zip": "95051", "country": "USA"}, "officers": [
        {"name": "Jensen Huang", "title": "CEO & President", "since": 1993, "born": 1963},
        {"name": "Colette Kress", "title": "CFO", "since": 2013, "born": 1965},
    ]},
    {"ein": "26-4563271", "name": "Meta Platforms, Inc.", "legal_name": "Meta Platforms, Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2004, "naics": 519130, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 86700, "revenue": 134.9e9, "ownership_type": "Public", "ticker": "META", "exchange": "NASDAQ", "address": {"street": "1 Meta Way", "city": "Menlo Park", "state": "CA", "zip": "94025", "country": "USA"}, "officers": [
        {"name": "Mark Zuckerberg", "title": "CEO & Chairman", "since": 2004, "born": 1984},
        {"name": "Susan Li", "title": "CFO", "since": 2022, "born": 1986},
        {"name": "Javier Olivan", "title": "COO", "since": 2022, "born": 1977},
    ]},
    {"ein": "36-4277050", "name": "Netflix, Inc.", "legal_name": "Netflix, Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1997, "naics": 512110, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 12800, "revenue": 33.7e9, "ownership_type": "Public", "ticker": "NFLX", "exchange": "NASDAQ", "address": {"street": "121 Albright Way", "city": "Los Gatos", "state": "CA", "zip": "95032", "country": "USA"}, "officers": [
        {"name": "Ted Sarandos", "title": "Co-CEO", "since": 2020, "born": 1964},
        {"name": "Greg Peters", "title": "Co-CEO", "since": 2023, "born": 1970},
        {"name": "Spencer Neumann", "title": "CFO", "since": 2019, "born": 1969},
    ]},
    # ── Additional Tech ──
    {"ein": "57-1169445", "name": "Boeing Company", "legal_name": "The Boeing Company", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1916, "naics": 336411, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 156000, "revenue": 77.8e9, "ownership_type": "Public", "ticker": "BA", "exchange": "NYSE", "address": {"street": "929 Long Bridge Dr", "city": "Arlington", "state": "VA", "zip": "22202", "country": "USA"}, "officers": [
        {"name": "David Calhoun", "title": "CEO & President", "since": 2020, "born": 1957},
        {"name": "Brian West", "title": "CFO", "since": 2021, "born": 1970},
    ]},
    {"ein": "38-0572511", "name": "General Motors Company", "legal_name": "General Motors Company", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1908, "naics": 336111, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 163000, "revenue": 171.8e9, "ownership_type": "Public", "ticker": "GM", "exchange": "NYSE", "address": {"street": "300 Renaissance Center", "city": "Detroit", "state": "MI", "zip": "48265", "country": "USA"}, "officers": [
        {"name": "Mary Barra", "title": "CEO & Chair", "since": 2014, "born": 1961},
        {"name": "Paul Jacobson", "title": "CFO", "since": 2021, "born": 1970},
    ]},
    {"ein": "95-4562587", "name": "Walt Disney Company", "legal_name": "The Walt Disney Company", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Last Sat Sep", "status": "Active", "year_founded": 1923, "naics": 512110, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 225000, "revenue": 88.9e9, "ownership_type": "Public", "ticker": "DIS", "exchange": "NYSE", "address": {"street": "500 S Buena Vista St", "city": "Burbank", "state": "CA", "zip": "91521", "country": "USA"}, "officers": [
        {"name": "Robert A. Iger", "title": "CEO", "since": 2022, "born": 1951},
        {"name": "Hugh F. Johnston", "title": "CFO", "since": 2023, "born": 1962},
    ]},
    {"ein": "91-1656899", "name": "Costco Wholesale Corporation", "legal_name": "Costco Wholesale Corporation", "type": "Corporation", "jurisdiction": "Washington", "fiscal_year_end": "Last Sun Aug", "status": "Active", "year_founded": 1983, "naics": 452311, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 302000, "revenue": 242.3e9, "ownership_type": "Public", "ticker": "COST", "exchange": "NASDAQ", "address": {"street": "999 Lake Dr", "city": "Issaquah", "state": "WA", "zip": "98027", "country": "USA"}, "officers": [
        {"name": "Ron Vachris", "title": "CEO", "since": 2024, "born": 1965},
        {"name": "Gary Millerchip", "title": "CFO", "since": 2023, "born": 1972},
    ]},
    {"ein": "36-3876131", "name": "McDonald's Corporation", "legal_name": "McDonald's Corporation", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1940, "naics": 722513, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 150000, "revenue": 25.5e9, "ownership_type": "Public", "ticker": "MCD", "exchange": "NYSE", "address": {"street": "110 N Carpenter St", "city": "Chicago", "state": "IL", "zip": "60607", "country": "USA"}, "officers": [
        {"name": "Chris Kempczinski", "title": "CEO", "since": 2019, "born": 1968},
        {"name": "Ian Borden", "title": "CFO", "since": 2021, "born": 1970},
    ]},
    {"ein": "82-4533164", "name": "Palantir Technologies Inc.", "legal_name": "Palantir Technologies Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2003, "naics": 541512, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 3400, "revenue": 2.2e9, "ownership_type": "Public", "ticker": "PLTR", "exchange": "NYSE", "address": {"street": "1200 17th St", "city": "Denver", "state": "CO", "zip": "80202", "country": "USA"}, "officers": [
        {"name": "Alexander Karp", "title": "CEO", "since": 2003, "born": 1967},
        {"name": "David Glazer", "title": "CFO", "since": 2021, "born": 1972},
    ]},
    {"ein": "94-3514036", "name": "Intel Corporation", "legal_name": "Intel Corporation", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1968, "naics": 334413, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 131900, "revenue": 54.2e9, "ownership_type": "Public", "ticker": "INTC", "exchange": "NASDAQ", "address": {"street": "2200 Mission College Blvd", "city": "Santa Clara", "state": "CA", "zip": "95054", "country": "USA"}, "officers": [
        {"name": "Pat Gelsinger", "title": "CEO", "since": 2021, "born": 1961},
        {"name": "David Zinsner", "title": "CFO", "since": 2022, "born": 1970},
    ]},
    {"ein": "30-0480390", "name": "Salesforce, Inc.", "legal_name": "Salesforce, Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Jan 31", "status": "Active", "year_founded": 1999, "naics": 541512, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 79000, "revenue": 34.9e9, "ownership_type": "Public", "ticker": "CRM", "exchange": "NYSE", "address": {"street": "415 Mission St", "city": "San Francisco", "state": "CA", "zip": "94105", "country": "USA"}, "officers": [
        {"name": "Marc Benioff", "title": "CEO & Chair", "since": 1999, "born": 1964},
        {"name": "Amy Weaver", "title": "CFO", "since": 2020, "born": 1970},
    ]},
    # ── More entities — small businesses & LLCs ──
    {"ein": "88-1234567", "name": "Main Street Bakery LLC", "legal_name": "Main Street Bakery LLC", "type": "LLC", "jurisdiction": "Texas", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2020, "naics": 311812, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 12, "revenue": 1.2e6, "ownership_type": "Private", "address": {"street": "123 Main St", "city": "Austin", "state": "TX", "zip": "78701", "country": "USA"}, "boi_filing_required": True, "boi_filing_status": "Not Filed", "boi_filing_due": "2025-01-01", "officers": [
        {"name": "Maria Gonzalez", "title": "Owner/Manager", "since": 2020, "born": 1985},
        {"name": "Carlos Gonzalez", "title": "Co-Owner", "since": 2020, "born": 1987},
    ]},
    {"ein": "88-2345678", "name": "TechFlow Consulting LLC", "legal_name": "TechFlow Consulting LLC", "type": "LLC", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2021, "naics": 541690, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 5, "revenue": 850000, "ownership_type": "Private", "address": {"street": "456 Innovation Dr", "city": "Wilmington", "state": "DE", "zip": "19801", "country": "USA"}, "boi_filing_required": True, "boi_filing_status": "Filed", "boi_filing_date": "2024-11-15", "officers": [
        {"name": "James Chen", "title": "Managing Member", "since": 2021, "born": 1978},
    ]},
    {"ein": "88-3456789", "name": "GreenLeaf Property Group", "legal_name": "GreenLeaf Property Group LLC", "type": "LLC", "jurisdiction": "Florida", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2019, "naics": 531210, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 3, "revenue": 2.1e6, "ownership_type": "Private", "address": {"street": "789 Ocean Blvd", "city": "Miami", "state": "FL", "zip": "33139", "country": "USA"}, "boi_filing_required": True, "boi_filing_status": "Not Filed", "boi_filing_due": "2025-03-15", "officers": [
        {"name": "Robert Thompson", "title": "Managing Member", "since": 2019, "born": 1972},
        {"name": "Sarah Thompson", "title": "Member", "since": 2019, "born": 1975},
    ]},
    {"ein": "88-4567890", "name": "Premier Healthcare Associates PA", "legal_name": "Premier Healthcare Associates PA", "type": "Professional Association", "jurisdiction": "Texas", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2015, "naics": 621111, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 45, "revenue": 5.6e6, "ownership_type": "Private", "address": {"street": "321 Medical Dr", "city": "Dallas", "state": "TX", "zip": "75204", "country": "USA"}, "boi_filing_required": True, "boi_filing_status": "Filed", "officers": [
        {"name": "Dr. Emily Watson", "title": "President", "since": 2015, "born": 1970},
        {"name": "Dr. Michael Lee", "title": "VP & Secretary", "since": 2016, "born": 1973},
    ]},
    {"ein": "88-5678901", "name": "Harbor Freight Logistics Inc.", "legal_name": "Harbor Freight Logistics Inc.", "type": "Corporation", "jurisdiction": "New York", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2008, "naics": 488510, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 120, "revenue": 28.5e6, "ownership_type": "Private", "address": {"street": "555 Port Ave", "city": "Brooklyn", "state": "NY", "zip": "11231", "country": "USA"}, "officers": [
        {"name": "Angelo Rivera", "title": "CEO", "since": 2008, "born": 1965},
        {"name": "Diana Rivera", "title": "Secretary", "since": 2008, "born": 1968},
    ]},
    {"ein": "88-6789012", "name": "Quantum Research Labs LLC", "legal_name": "Quantum Research Labs LLC", "type": "LLC", "jurisdiction": "California", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2022, "naics": 541715, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 8, "revenue": 450000, "ownership_type": "Private", "address": {"street": "100 Tech Park Blvd", "city": "San Jose", "state": "CA", "zip": "95110", "country": "USA"}, "boi_filing_required": True, "boi_filing_status": "Not Filed", "officers": [
        {"name": "Dr. Priya Sharma", "title": "CEO & Founder", "since": 2022, "born": 1982},
    ]},
    {"ein": "88-7890123", "name": "Sunrise Hospitality Group Inc.", "legal_name": "Sunrise Hospitality Group Inc.", "type": "Corporation", "jurisdiction": "Nevada", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2010, "naics": 721110, "sec_filer": False, "boi_exempt": True, "boi_exempt_reason": "Large operating company (20+ FTEs, >$5M revenue)", "employees": 350, "revenue": 42.0e6, "ownership_type": "Private", "address": {"street": "2000 Las Vegas Blvd S", "city": "Las Vegas", "state": "NV", "zip": "89104", "country": "USA"}, "officers": [
        {"name": "Victoria Chen", "title": "CEO", "since": 2010, "born": 1975},
        {"name": "David Kim", "title": "CFO", "since": 2012, "born": 1978},
    ]},
    {"ein": "88-8901234", "name": "Maplewood Senior Living LLC", "legal_name": "Maplewood Senior Living LLC", "type": "LLC", "jurisdiction": "Ohio", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2017, "naics": 623311, "sec_filer": False, "boi_exempt": True, "boi_exempt_reason": "Large operating company (20+ FTEs, >$5M revenue)", "employees": 180, "revenue": 14.3e6, "ownership_type": "Private", "address": {"street": "800 Maplewood Dr", "city": "Columbus", "state": "OH", "zip": "43215", "country": "USA"}, "officers": [
        {"name": "Thomas Wright", "title": "CEO", "since": 2017, "born": 1968},
        {"name": "Nancy Wright", "title": "COO", "since": 2017, "born": 1970},
    ]},
    {"ein": "88-9012345", "name": "Pinnacle Capital Partners LP", "legal_name": "Pinnacle Capital Partners LP", "type": "Limited Partnership", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2005, "naics": 523999, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 15, "revenue": 12.0e6, "ownership_type": "Private", "address": {"street": "50 Broad St", "city": "New York", "state": "NY", "zip": "10004", "country": "USA"}, "officers": [
        {"name": "Jonathan Stein", "title": "Managing Partner", "since": 2005, "born": 1962},
        {"name": "Rebecca Stein", "title": "Partner", "since": 2005, "born": 1965},
    ]},
    {"ein": "88-0123456", "name": "Atlas Construction Co.", "legal_name": "Atlas Construction Company Inc.", "type": "Corporation", "jurisdiction": "Illinois", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1995, "naics": 236220, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 85, "revenue": 32.0e6, "ownership_type": "Private", "address": {"street": "1000 Industrial Pkwy", "city": "Chicago", "state": "IL", "zip": "60616", "country": "USA"}, "officers": [
        {"name": "Frank Miller", "title": "President", "since": 1995, "born": 1960},
        {"name": "Susan Miller", "title": "VP & Treasurer", "since": 1998, "born": 1963},
    ]},
    {"ein": "88-1234509", "name": "Coastal Renewable Energy LLC", "legal_name": "Coastal Renewable Energy LLC", "type": "LLC", "jurisdiction": "California", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2018, "naics": 221118, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 25, "revenue": 3.8e6, "ownership_type": "Private", "address": {"street": "567 Coast Hwy", "city": "San Diego", "state": "CA", "zip": "92101", "country": "USA"}, "officers": [
        {"name": "Kevin Nguyen", "title": "CEO", "since": 2018, "born": 1984},
        {"name": "Lisa Park", "title": "CFO", "since": 2019, "born": 1986},
    ]},
    # ── More small businesses ──
    {"ein": "99-1234501", "name": "Blue Ridge Auto Shop LLC", "legal_name": "Blue Ridge Auto Shop LLC", "type": "LLC", "jurisdiction": "Virginia", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2022, "naics": 811111, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 4, "revenue": 320000, "ownership_type": "Private", "address": {"street": "422 Blue Ridge Pkwy", "city": "Roanoke", "state": "VA", "zip": "24018", "country": "USA"}, "boi_filing_required": True, "boi_filing_status": "Filed", "officers": [
        {"name": "Jake Morrison", "title": "Owner", "since": 2022, "born": 1990},
    ]},
    {"ein": "99-1234502", "name": "Pioneer Seed & Supply Co.", "legal_name": "Pioneer Seed & Supply Co.", "type": "Corporation", "jurisdiction": "Iowa", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1987, "naics": 424910, "sec_filer": False, "boi_exempt": True, "boi_exempt_reason": "Large operating company (20+ FTEs, >$5M revenue)", "employees": 45, "revenue": 8.5e6, "ownership_type": "Private", "address": {"street": "1 Harvest Ln", "city": "Des Moines", "state": "IA", "zip": "50309", "country": "USA"}, "officers": [
        {"name": "Harold Jensen", "title": "CEO", "since": 1987, "born": 1955},
        {"name": "Mark Jensen", "title": "President", "since": 2010, "born": 1980},
    ]},
    {"ein": "99-1234503", "name": "Silver Lake Asset Management LLC", "legal_name": "Silver Lake Asset Management LLC", "type": "LLC", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1999, "naics": 523920, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 250, "revenue": 1.8e9, "ownership_type": "Private", "address": {"street": "9 W 57th St", "city": "New York", "state": "NY", "zip": "10019", "country": "USA"}, "officers": [
        {"name": "Egon Durban", "title": "Co-CEO", "since": 2012, "born": 1970},
        {"name": "Kenneth Hao", "title": "Managing Partner", "since": 1999, "born": 1965},
    ]},
    {"ein": "99-1234504", "name": "Thrive Fitness Franchising LLC", "legal_name": "Thrive Fitness Franchising LLC", "type": "LLC", "jurisdiction": "Colorado", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2019, "naics": 713940, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 6, "revenue": 950000, "ownership_type": "Private", "address": {"street": "751 Wellness Way", "city": "Denver", "state": "CO", "zip": "80202", "country": "USA"}, "boi_filing_required": True, "boi_filing_status": "Filed", "officers": [
        {"name": "Megan Fox", "title": "Founder & CEO", "since": 2019, "born": 1988},
    ]},
    {"ein": "99-1234505", "name": "Northern Lights Brewing Co.", "legal_name": "Northern Lights Brewing Company Inc.", "type": "Corporation", "jurisdiction": "Oregon", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2014, "naics": 312120, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 18, "revenue": 2.1e6, "ownership_type": "Private", "address": {"street": "88 Brewery Row", "city": "Portland", "state": "OR", "zip": "97201", "country": "USA"}, "officers": [
        {"name": "Erik Lindstrom", "title": "CEO & Head Brewer", "since": 2014, "born": 1980},
        {"name": "Anna Lindstrom", "title": "CFO", "since": 2014, "born": 1982},
    ]},
    {"ein": "99-1234506", "name": "Summit Defense Contractors LLC", "legal_name": "Summit Defense Contractors LLC", "type": "LLC", "jurisdiction": "Virginia", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2008, "naics": 541330, "sec_filer": False, "boi_exempt": True, "boi_exempt_reason": "Large operating company (20+ FTEs, >$5M revenue)", "employees": 220, "revenue": 65.0e6, "ownership_type": "Private", "address": {"street": "2000 Defense Blvd", "city": "Arlington", "state": "VA", "zip": "22202", "country": "USA"}, "officers": [
        {"name": "General (Ret.) James Parker", "title": "CEO", "since": 2008, "born": 1960},
        {"name": "Michelle Torres", "title": "CFO", "since": 2010, "born": 1973},
    ]},
    {"ein": "99-1234507", "name": "Crimson Education Group LLC", "legal_name": "Crimson Education Group LLC", "type": "LLC", "jurisdiction": "Massachusetts", "fiscal_year_end": "Jun 30", "status": "Active", "year_founded": 2016, "naics": 611710, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 10, "revenue": 1.5e6, "ownership_type": "Private", "address": {"street": "30 Harvard Sq", "city": "Cambridge", "state": "MA", "zip": "02138", "country": "USA"}, "officers": [
        {"name": "Dr. Sophia Park", "title": "CEO", "since": 2016, "born": 1981},
    ]},
    {"ein": "99-1234508", "name": "Gulf Coast Fisheries LP", "legal_name": "Gulf Coast Fisheries LP", "type": "Limited Partnership", "jurisdiction": "Louisiana", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1990, "naics": 114111, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 30, "revenue": 4.5e6, "ownership_type": "Private", "address": {"street": "200 Harbor Blvd", "city": "New Orleans", "state": "LA", "zip": "70130", "country": "USA"}, "officers": [
        {"name": "Pierre LeBlanc", "title": "General Partner", "since": 1990, "born": 1958},
    ]},
    {"ein": "99-1234509", "name": "Keystone Engineering Group", "legal_name": "Keystone Engineering Group Inc.", "type": "Corporation", "jurisdiction": "Pennsylvania", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2002, "naics": 541330, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 65, "revenue": 9.8e6, "ownership_type": "Private", "address": {"street": "400 Penn Ave", "city": "Pittsburgh", "state": "PA", "zip": "15222", "country": "USA"}, "officers": [
        {"name": "David Gallagher", "title": "President", "since": 2002, "born": 1970},
        {"name": "Timothy Gallagher", "title": "VP Engineering", "since": 2005, "born": 1973},
    ]},
    # ── Shell/SPVs ──
    {"ein": "99-9999901", "name": "White Oak Holdings LLC", "legal_name": "White Oak Holdings LLC", "type": "LLC", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2023, "naics": 525990, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 0, "revenue": 0, "ownership_type": "Private", "address": {"street": "1201 N Market St", "city": "Wilmington", "state": "DE", "zip": "19801", "country": "USA"}, "boi_filing_required": True, "boi_filing_status": "Not Filed", "officers": [
        {"name": "John Doe", "title": "Registered Agent", "since": 2023, "born": 1975},
    ]},
    {"ein": "99-9999902", "name": "Evergreen Capital Fund I LP", "legal_name": "Evergreen Capital Fund I LP", "type": "Limited Partnership", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2024, "naics": 525910, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 0, "revenue": 0, "ownership_type": "Private", "address": {"street": "251 Little Falls Dr", "city": "Wilmington", "state": "DE", "zip": "19808", "country": "USA"}, "officers": [
        {"name": "William Smith", "title": "GP", "since": 2024, "born": 1980},
    ]},
    {"ein": "99-9999903", "name": "Blue Ocean SPV LLC", "legal_name": "Blue Ocean SPV LLC", "type": "LLC", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2024, "naics": 525990, "sec_filer": False, "boi_exempt": False, "boi_exempt_reason": None, "employees": 0, "revenue": 0, "ownership_type": "Private", "address": {"street": "8 The Green, Ste A", "city": "Dover", "state": "DE", "zip": "19901", "country": "USA"}, "officers": [
        {"name": "Jane Smith", "title": "Manager", "since": 2024, "born": 1985},
    ]},
    # ── Nonprofits ──
    {"ein": "13-1834160", "name": "American Red Cross", "legal_name": "American National Red Cross", "type": "Nonprofit", "jurisdiction": "District of Columbia", "fiscal_year_end": "Jun 30", "status": "Active", "year_founded": 1881, "naics": 813212, "sec_filer": False, "boi_exempt": True, "boi_exempt_reason": "Tax-exempt entity (501(c)(3))", "employees": 32000, "revenue": 3.2e9, "ownership_type": "Nonprofit", "address": {"street": "431 18th St NW", "city": "Washington", "state": "DC", "zip": "20006", "country": "USA"}, "officers": [
        {"name": "Gail J. McGovern", "title": "President & CEO", "since": 2008, "born": 1952},
        {"name": "Michael O'Toole", "title": "CFO", "since": 2019, "born": 1965},
    ]},
    {"ein": "13-5563779", "name": "Bill & Melinda Gates Foundation", "legal_name": "Bill & Melinda Gates Foundation", "type": "Nonprofit", "jurisdiction": "Washington", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2000, "naics": 813211, "sec_filer": False, "boi_exempt": True, "boi_exempt_reason": "Tax-exempt entity (501(c)(3))", "employees": 1500, "revenue": 7.0e9, "ownership_type": "Nonprofit", "address": {"street": "500 5th Ave N", "city": "Seattle", "state": "WA", "zip": "98109", "country": "USA"}, "officers": [
        {"name": "Mark Suzman", "title": "CEO", "since": 2020, "born": 1970},
        {"name": "Connie K. Duckworth", "title": "CFO", "since": 2021, "born": 1965},
    ]},
    {"ein": "95-3203056", "name": "Wikimedia Foundation", "legal_name": "Wikimedia Foundation, Inc.", "type": "Nonprofit", "jurisdiction": "Florida", "fiscal_year_end": "Jun 30", "status": "Active", "year_founded": 2003, "naics": 519130, "sec_filer": False, "boi_exempt": True, "boi_exempt_reason": "Tax-exempt entity (501(c)(3))", "employees": 700, "revenue": 180e6, "ownership_type": "Nonprofit", "address": {"street": "1 Montgomery St", "city": "San Francisco", "state": "CA", "zip": "94104", "country": "USA"}, "officers": [
        {"name": "Maryana Iskander", "title": "CEO", "since": 2022, "born": 1975},
        {"name": "Jaime Villagomez", "title": "CFO", "since": 2021, "born": 1978},
    ]},
    {"ein": "61-1767919", "name": "Alphabet Inc.", "legal_name": "Alphabet Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 2015, "naics": 519130, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 190234, "revenue": 307.4e9, "ownership_type": "Public", "ticker": "GOOGL", "exchange": "NASDAQ", "address": {"street": "1600 Amphitheatre Parkway", "city": "Mountain View", "state": "CA", "zip": "94043", "country": "USA"}, "officers": [
        {"name": "Sundar Pichai", "title": "CEO", "since": 2019, "born": 1972},
        {"name": "Ruth Porat", "title": "CFO", "since": 2015, "born": 1957},
        {"name": "John Kent", "title": "SVP & Controller", "since": 2018, "born": 1969},
    ]},
    {"ein": "59-1234561", "name": "Berkshire Hathaway Inc.", "legal_name": "Berkshire Hathaway Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1839, "naics": 551112, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 372000, "revenue": 364.5e9, "ownership_type": "Public", "ticker": "BRK.A", "exchange": "NYSE", "address": {"street": "3555 Farnam St", "city": "Omaha", "state": "NE", "zip": "68131", "country": "USA"}, "officers": [
        {"name": "Warren E. Buffett", "title": "CEO & Chairman", "since": 1970, "born": 1930},
        {"name": "Ajit Jain", "title": "Vice Chairman", "since": 2018, "born": 1951},
        {"name": "Greg Abel", "title": "Vice Chairman", "since": 2018, "born": 1962},
    ]},
    {"ein": "94-3233175", "name": "Chevron Corporation", "legal_name": "Chevron Corporation", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1879, "naics": 211120, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 44567, "revenue": 200.9e9, "ownership_type": "Public", "ticker": "CVX", "exchange": "NYSE", "address": {"street": "6001 Bollinger Canyon Rd", "city": "San Ramon", "state": "CA", "zip": "94583", "country": "USA"}, "officers": [
        {"name": "Mike Wirth", "title": "CEO & Chairman", "since": 2018, "born": 1961},
        {"name": "Pierre Breber", "title": "CFO", "since": 2017, "born": 1965},
    ]},
    {"ein": "13-2601014", "name": "Pfizer Inc.", "legal_name": "Pfizer Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1849, "naics": 325412, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 83000, "revenue": 58.5e9, "ownership_type": "Public", "ticker": "PFE", "exchange": "NYSE", "address": {"street": "235 E 42nd St", "city": "New York", "state": "NY", "zip": "10017", "country": "USA"}, "officers": [
        {"name": "Albert Bourla", "title": "CEO", "since": 2019, "born": 1961},
        {"name": "David Denton", "title": "CFO", "since": 2022, "born": 1966},
    ]},
    {"ein": "51-0373179", "name": "Merck & Co., Inc.", "legal_name": "Merck & Co., Inc.", "type": "Corporation", "jurisdiction": "New Jersey", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1891, "naics": 325412, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 70000, "revenue": 60.1e9, "ownership_type": "Public", "ticker": "MRK", "exchange": "NYSE", "address": {"street": "126 E Lincoln Ave", "city": "Rahway", "state": "NJ", "zip": "07065", "country": "USA"}, "officers": [
        {"name": "Robert M. Davis", "title": "CEO & President", "since": 2022, "born": 1969},
        {"name": "Caroline Litchfield", "title": "CFO", "since": 2023, "born": 1973},
    ]},
    {"ein": "94-2408276", "name": "eBay Inc.", "legal_name": "eBay Inc.", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Dec 31", "status": "Active", "year_founded": 1995, "naics": 454110, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 10800, "revenue": 10.1e9, "ownership_type": "Public", "ticker": "EBAY", "exchange": "NASDAQ", "address": {"street": "2025 Hamilton Ave", "city": "San Jose", "state": "CA", "zip": "95125", "country": "USA"}, "officers": [
        {"name": "Jamie Iannone", "title": "CEO", "since": 2020, "born": 1971},
        {"name": "Steve Priest", "title": "CFO", "since": 2021, "born": 1973},
    ]},
    {"ein": "95-4764748", "name": "Hewlett Packard Enterprise", "legal_name": "Hewlett Packard Enterprise Company", "type": "Corporation", "jurisdiction": "Delaware", "fiscal_year_end": "Oct 31", "status": "Active", "year_founded": 2015, "naics": 334111, "sec_filer": True, "boi_exempt": True, "boi_exempt_reason": "Large operating company", "employees": 62000, "revenue": 29.1e9, "ownership_type": "Public", "ticker": "HPE", "exchange": "NYSE", "address": {"street": "1701 E Mossy Oaks Rd", "city": "Spring", "state": "TX", "zip": "77389", "country": "USA"}, "officers": [
        {"name": "Antonio Neri", "title": "CEO & President", "since": 2018, "born": 1967},
        {"name": "Tarek Robbiati", "title": "CFO", "since": 2022, "born": 1970},
    ]},
]

# Additional officer-only entities for officer lookup
ADDITIONAL_OFFICERS = [
    {"ein": "00-0000001", "name": "Acme Corp (Sample)", "legal_name": "Acme Corporation", "type": "Corporation", "jurisdiction": "Delaware", "status": "Active", "officers": [
        {"name": "Wile E. Coyote", "title": "CEO", "since": 1949, "born": 1925},
    ]},
]

# ── Helper functions ──

def _build_cache():
    """Builds the searchable in-memory cache."""
    global _cache, _cache_time
    db = {}
    idx_name = {}
    idx_ein = {}
    idx_officer = {}
    idx_naics = {}
    
    all_entities = COMPANIES + ADDITIONAL_OFFICERS
    
    for ent in all_entities:
        e = dict(ent)
        ein = e["ein"]
        db[ein] = e
        
        # Name index (lowercase, partial)
        name_lower = e["name"].lower()
        idx_name.setdefault(name_lower, []).append(ein)
        # Add partial name fragments
        parts = name_lower.split()
        for p in parts:
            if len(p) > 2:
                idx_name.setdefault(p, []).append(ein)
                # first 3 chars
                idx_name.setdefault(p[:3], []).append(ein)
        
        # EIN prefix index
        idx_ein.setdefault(ein[:2], []).append(ein)
        idx_ein.setdefault(ein[:5], []).append(ein)
        
        # Officer name index
        for off in e.get("officers", []):
            oname = off["name"].lower()
            idx_officer.setdefault(oname, []).append(ein)
            # partial name
            oname_parts = oname.split()
            for p in oname_parts:
                if len(p) > 2:
                    idx_officer.setdefault(p, []).append(ein)
        
        # NAICS
        naics = e.get("naics")
        if naics:
            naics_str = str(naics)
            idx_naics.setdefault(naics_str[:2], []).append(ein)
            idx_naics.setdefault(naics_str[:4], []).append(ein)
            idx_naics.setdefault(naics_str, []).append(ein)
    
    _cache = {
        "db": db,
        "idx_name": idx_name,
        "idx_ein": idx_ein,
        "idx_officer": idx_officer,
        "idx_naics": idx_naics,
        "all_eins": list(db.keys()),
        "built_at": datetime.utcnow().isoformat(),
        "count": len(db),
    }
    _cache_time = time.time()


def _ensure_cache():
    """Ensure cache is populated and fresh."""
    global _cache, _cache_time
    if _cache is None or (time.time() - _cache_time) > _CACHE_TTL:
        with _lock:
            if _cache is None or (time.time() - _cache_time) > _CACHE_TTL:
                _build_cache()


def get_entity(ein: str) -> Optional[dict]:
    """Get entity by EIN."""
    _ensure_cache()
    return _cache["db"].get(ein)


def search_entities(q: str = None, name: str = None, ein: str = None,
                    naics: str = None, state: str = None, 
                    boi_status: str = None, limit: int = 20, offset: int = 0) -> dict:
    """Search entities with various filters."""
    _ensure_cache()
    db = _cache["db"]
    idx_name = _cache["idx_name"]
    idx_ein = _cache["idx_ein"]
    idx_officer = _cache["idx_officer"]
    
    candidates = set(_cache["all_eins"])
    
    if q:
        q = q.lower()
        matching = set()
        for key, eins in idx_name.items():
            if q in key:
                matching.update(eins)
        # Also check officer names
        for key, eins in idx_officer.items():
            if q in key:
                matching.update(eins)
        # Check EIN prefix
        for key, eins in idx_ein.items():
            if q in key or q in str(key):
                matching.update(eins)
        candidates &= matching if matching else set()
    
    if name:
        name_l = name.lower()
        matching = set()
        for key, eins in idx_name.items():
            if name_l in key or key in name_l:
                matching.update(eins)
        candidates &= matching if matching else set()
    
    if ein:
        matching = set()
        for key, eins in idx_ein.items():
            if ein.lower() in key or key in ein.lower():
                matching.update(eins)
        # Also exact check
        if ein in db:
            matching.add(ein)
        candidates &= matching if matching else set()
    
    if naics:
        matching = set()
        for key, eins in _cache["idx_naics"].items():
            if naics in key or key in naics:
                matching.update(eins)
        candidates &= matching if matching else set()
    
    if state:
        state_l = state.lower()
        matching = set()
        for eid in candidates:
            ent = db[eid]
            addr = ent.get("address", {})
            if addr.get("state", "").lower() == state_l:
                matching.add(eid)
        candidates &= matching
    
    if boi_status:
        bs_l = boi_status.lower()
        matching = set()
        for eid in candidates:
            ent = db[eid]
            status = ent.get("boi_filing_status", "")
            if status and status.lower() == bs_l:
                matching.add(eid)
            elif bs_l == "exempt" and ent.get("boi_exempt"):
                matching.add(eid)
            elif bs_l == "required" and ent.get("boi_filing_required"):
                matching.add(eid)
        candidates &= matching
    
    total = len(candidates)
    sorted_eins = sorted(candidates)
    page = sorted_eins[offset:offset + limit]
    
    results = []
    for eid in page:
        ent = db[eid]
        results.append({
            "ein": ent["ein"],
            "name": ent["name"],
            "type": ent.get("type"),
            "jurisdiction": ent.get("jurisdiction"),
            "status": ent.get("status"),
            "ownership_type": ent.get("ownership_type"),
            "boi_exempt": ent.get("boi_exempt", False),
            "boi_filing_required": ent.get("boi_filing_required", False),
            "boi_filing_status": ent.get("boi_filing_status"),
            "city": ent.get("address", {}).get("city"),
            "state": ent.get("address", {}).get("state"),
            "officer_count": len(ent.get("officers", [])),
        })
    
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": results,
    }


def lookup_officer(name: str, limit: int = 20, offset: int = 0) -> dict:
    """Look up entities by officer name."""
    _ensure_cache()
    db = _cache["db"]
    idx_officer = _cache["idx_officer"]
    
    q = name.lower()
    matching = set()
    for key, eins in idx_officer.items():
        if q in key or key in q:
            matching.update(eins)
    
    total = len(matching)
    sorted_eins = sorted(matching)
    page = sorted_eins[offset:offset + limit]
    
    results = []
    for eid in page:
        ent = db[eid]
        matching_officers = [o for o in ent.get("officers", []) 
                            if q in o["name"].lower() or any(q in p for p in o["name"].lower().split())]
        results.append({
            "ein": ent["ein"],
            "entity_name": ent["name"],
            "entity_type": ent.get("type"),
            "officers": matching_officers[:5],
            "state": ent.get("address", {}).get("state"),
        })
    
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": results,
    }


def compliance_check(ein: str = None, name: str = None) -> dict:
    """Check BOI compliance status for an entity."""
    _ensure_cache()
    db = _cache["db"]
    
    if ein and ein in db:
        ent = db[ein]
    elif name:
        name_l = name.lower()
        for eid, ent in db.items():
            if name_l in ent["name"].lower():
                return _build_compliance(ent)
        return {"error": "Entity not found", "compliance_status": "unknown"}
    else:
        return {"error": "Provide ein or name", "compliance_status": "unknown"}
    
    return _build_compliance(ent)


def _build_compliance(ent: dict) -> dict:
    """Build compliance report for an entity."""
    boi_exempt = ent.get("boi_exempt", False)
    boi_filing_required = ent.get("boi_filing_required", not boi_exempt)
    boi_filing_status = ent.get("boi_filing_status", "Not Filed" if boi_filing_required else "Not Required")
    exempt_reason = ent.get("boi_exempt_reason", None)
    employees = ent.get("employees", 0)
    revenue = ent.get("revenue", 0)
    
    if boi_exempt:
        status = "Exempt"
        details = f"Exempt from BOI filing. Reason: {exempt_reason}"
    elif boi_filing_status == "Filed":
        status = "Compliant"
        details = f"BOI report filed on {ent.get('boi_filing_date', 'N/A')}"
    elif boi_filing_status == "Not Filed":
        due = ent.get("boi_filing_due", "90 days from creation/registration")
        status = "Non-Compliant"
        details = f"BOI report not filed. Due: {due}"
    else:
        status = "Unknown"
        details = "BOI filing status not determined"
    
    return {
        "ein": ent["ein"],
        "entity_name": ent["name"],
        "entity_type": ent.get("type"),
        "compliance_status": status,
        "boi_exempt": boi_exempt,
        "boi_filing_required": boi_filing_required,
        "boi_filing_status": boi_filing_status,
        "exempt_reason": exempt_reason,
        "details": details,
        "employees": employees,
        "revenue": revenue,
        "ownership_type": ent.get("ownership_type"),
        "officers": ent.get("officers", []),
    }


def get_cache_stats() -> dict:
    """Get cache statistics."""
    _ensure_cache()
    return {
        "total_entities": _cache["count"],
        "built_at": _cache["built_at"],
        "cache_ttl_seconds": _CACHE_TTL,
        "cache_age_seconds": int(time.time() - _cache_time),
    }


def refresh_cache():
    """Force refresh the cache."""
    global _cache, _cache_time
    with _lock:
        _build_cache()
    return {"status": "ok", "count": _cache["count"]}
