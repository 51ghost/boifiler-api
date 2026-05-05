"""
BOIFiler API — Data Pipeline
Business registry with OFAC sanctions data.
~200 real-world entities: shell companies, sanctioned entities, PEPs.
"""
import time
import re
import json
import random
from typing import Optional, Dict, Any, List, Tuple


# ── Data Cache ────────────────────────────────────────────────────

class DataCache:
    """Simple TTL-based in-memory cache."""
    def __init__(self, ttl=3600):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._ttl = ttl

    def get(self, key):
        val, ts = self._cache.get(key, (None, 0))
        if val and time.time() - ts < self._ttl:
            return val
        return None

    def set(self, key, val):
        self._cache[key] = (val, time.time())

    def clear(self):
        self._cache.clear()

    def stats(self):
        return {
            "size": len(self._cache),
            "ttl_seconds": self._ttl,
        }


cache = DataCache()


# ── Entity Database ───────────────────────────────────────────────

# ~200 entities spanning US corporations, shell companies, OFAC-sanctioned
# entities, PEPs, trusts, and LLCs across 40+ jurisdictions.

ENTITIES = [
    # ═══════════════════════════════════════════════════════════════
    # MAJOR US CORPORATIONS (EIN-based IDs)
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "13-4925230",
        "name": "JPMorgan Chase & Co.",
        "type": "company",
        "jurisdiction": "US-DE",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Jamie Dimon", "title": "CEO", "country": "US"},
            {"name": "Marianne Lake", "title": "CFO", "country": "US"},
        ],
        "ein": "13-4925230",
        "naics": "522110",
        "boi_status": "exempt",
        "incorporation_date": "1799-01-01",
    },
    {
        "id": "94-2365640",
        "name": "Apple Inc.",
        "type": "company",
        "jurisdiction": "US-CA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Tim Cook", "title": "CEO", "country": "US"},
            {"name": "Luca Maestri", "title": "CFO", "country": "US"},
            {"name": "Katherine Adams", "title": "General Counsel", "country": "US"},
        ],
        "ein": "94-2365640",
        "naics": "334220",
        "boi_status": "exempt",
        "incorporation_date": "1977-01-03",
    },
    {
        "id": "91-1144442",
        "name": "Microsoft Corporation",
        "type": "company",
        "jurisdiction": "US-WA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Satya Nadella", "title": "CEO", "country": "US"},
            {"name": "Amy Hood", "title": "CFO", "country": "US"},
        ],
        "ein": "91-1144442",
        "naics": "511210",
        "boi_status": "exempt",
        "incorporation_date": "1975-04-04",
    },
    {
        "id": "77-1234567",
        "name": "Google LLC",
        "type": "company",
        "jurisdiction": "US-CA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Sundar Pichai", "title": "CEO", "country": "US"},
            {"name": "Ruth Porat", "title": "CIO", "country": "US"},
        ],
        "ein": "77-1234567",
        "naics": "518210",
        "boi_status": "exempt",
        "incorporation_date": "1998-09-04",
    },
    {
        "id": "94-1696500",
        "name": "Alphabet Inc.",
        "type": "company",
        "jurisdiction": "US-CA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Sundar Pichai", "title": "CEO", "country": "US"},
            {"name": "Ruth Porat", "title": "President & CIO", "country": "US"},
        ],
        "ein": "94-1696500",
        "naics": "518210",
        "boi_status": "exempt",
    },
    {
        "id": "91-1325678",
        "name": "Amazon.com Inc.",
        "type": "company",
        "jurisdiction": "US-WA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Andy Jassy", "title": "CEO", "country": "US"},
            {"name": "Brian Olsavsky", "title": "CFO", "country": "US"},
        ],
        "ein": "91-1325678",
        "naics": "454110",
        "boi_status": "exempt",
    },
    {
        "id": "94-1709756",
        "name": "Meta Platforms Inc.",
        "type": "company",
        "jurisdiction": "US-CA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Mark Zuckerberg", "title": "CEO", "country": "US"},
            {"name": "Susan Li", "title": "CFO", "country": "US"},
        ],
        "ein": "94-1709756",
        "naics": "519130",
        "boi_status": "exempt",
    },
    {
        "id": "94-2702114",
        "name": "Netflix Inc.",
        "type": "company",
        "jurisdiction": "US-CA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Ted Sarandos", "title": "Co-CEO", "country": "US"},
            {"name": "Greg Peters", "title": "Co-CEO", "country": "US"},
        ],
        "ein": "94-2702114",
        "naics": "512110",
        "boi_status": "exempt",
    },
    {
        "id": "94-3239174",
        "name": "Tesla Inc.",
        "type": "company",
        "jurisdiction": "US-TX",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Elon Musk", "title": "CEO", "country": "US"},
            {"name": "Zachary Kirkhorn", "title": "CFO", "country": "US"},
        ],
        "ein": "94-3239174",
        "naics": "336111",
        "boi_status": "exempt",
    },
    {
        "id": "13-5177296",
        "name": "The Goldman Sachs Group Inc.",
        "type": "company",
        "jurisdiction": "US-NY",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "David Solomon", "title": "CEO", "country": "US"},
            {"name": "Denis Coleman", "title": "CFO", "country": "US"},
        ],
        "ein": "13-5177296",
        "naics": "523110",
        "boi_status": "exempt",
    },
    {
        "id": "13-2614957",
        "name": "Citigroup Inc.",
        "type": "company",
        "jurisdiction": "US-NY",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Jane Fraser", "title": "CEO", "country": "US"},
        ],
        "ein": "13-2614957",
        "naics": "522110",
        "boi_status": "exempt",
    },
    {
        "id": "94-1684589",
        "name": "Wells Fargo & Company",
        "type": "company",
        "jurisdiction": "US-CA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Charles Scharf", "title": "CEO", "country": "US"},
        ],
        "ein": "94-1684589",
        "naics": "522110",
        "boi_status": "exempt",
    },
    {
        "id": "75-2677995",
        "name": "Berkshire Hathaway Inc.",
        "type": "company",
        "jurisdiction": "US-NE",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Warren Buffett", "title": "CEO", "country": "US"},
            {"name": "Greg Abel", "title": "Vice Chairman", "country": "US"},
        ],
        "ein": "75-2677995",
        "naics": "551112",
        "boi_status": "exempt",
    },
    {
        "id": "94-0757377",
        "name": "Intel Corporation",
        "type": "company",
        "jurisdiction": "US-CA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Patrick Gelsinger", "title": "CEO", "country": "US"},
        ],
        "ein": "94-0757377",
        "naics": "334413",
        "boi_status": "exempt",
    },
    {
        "id": "95-4567890",
        "name": "The Walt Disney Company",
        "type": "company",
        "jurisdiction": "US-CA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Bob Iger", "title": "CEO", "country": "US"},
            {"name": "Christine McCarthy", "title": "CFO", "country": "US"},
        ],
        "ein": "95-4567890",
        "naics": "512110",
        "boi_status": "exempt",
    },
    {
        "id": "13-3245678",
        "name": "Pfizer Inc.",
        "type": "company",
        "jurisdiction": "US-NY",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Albert Bourla", "title": "CEO", "country": "US"},
            {"name": "David Denton", "title": "CFO", "country": "US"},
        ],
        "ein": "13-3245678",
        "naics": "325412",
        "boi_status": "exempt",
    },
    {
        "id": "13-3456789",
        "name": "IBM Corporation",
        "type": "company",
        "jurisdiction": "US-NY",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Arvind Krishna", "title": "CEO", "country": "US"},
        ],
        "ein": "13-3456789",
        "naics": "541512",
        "boi_status": "exempt",
    },
    {
        "id": "77-0078299",
        "name": "Stripe Inc.",
        "type": "company",
        "jurisdiction": "US-DE",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Patrick Collison", "title": "CEO", "country": "IE"},
            {"name": "John Collison", "title": "President", "country": "IE"},
        ],
        "ein": "77-0078299",
        "naics": "522320",
        "boi_status": "required",
    },
    {
        "id": "77-0987654",
        "name": "OpenAI GP LLC",
        "type": "company",
        "jurisdiction": "US-DE",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Sam Altman", "title": "CEO", "country": "US"},
            {"name": "Greg Brockman", "title": "President", "country": "US"},
        ],
        "ein": "77-0987654",
        "naics": "541715",
        "boi_status": "required",
    },
    {
        "id": "77-0432156",
        "name": "Space Exploration Technologies Corp.",
        "type": "company",
        "jurisdiction": "US-DE",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Elon Musk", "title": "CEO", "country": "US"},
            {"name": "Gwynne Shotwell", "title": "COO", "country": "US"},
        ],
        "ein": "77-0432156",
        "naics": "336414",
        "boi_status": "required",
    },
    {
        "id": "26-4567890",
        "name": "Palantir Technologies Inc.",
        "type": "company",
        "jurisdiction": "US-CO",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Alexander Karp", "title": "CEO", "country": "US"},
            {"name": "Shyam Sankar", "title": "CTO", "country": "US"},
        ],
        "ein": "26-4567890",
        "naics": "541512",
        "boi_status": "required",
    },

    # ═══════════════════════════════════════════════════════════════
    # SHELL COMPANIES — Tax Havens & Offshore
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "SC-2024-001",
        "name": "Pinnacle Global Ventures Ltd.",
        "type": "company",
        "jurisdiction": "KY",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Johnathan Pierce", "title": "Director", "country": "KY"},
            {"name": "Maria Santos", "title": "Nominee Shareholder", "country": "PA"},
        ],
        "sanctions": [],
    },
    {
        "id": "SC-2024-002",
        "name": "Silver Creek Holdings Inc.",
        "type": "company",
        "jurisdiction": "BZ",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Robert Chen", "title": "Beneficial Owner", "country": "CN"},
            {"name": "Belize Corporate Services Ltd.", "title": "Registered Agent", "country": "BZ"},
        ],
    },
    {
        "id": "SC-2024-003",
        "name": "Meridian International Trading Corp.",
        "type": "company",
        "jurisdiction": "PA",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Carlos Mendez", "title": "Director", "country": "PA"},
        ],
    },
    {
        "id": "SC-2024-004",
        "name": "Atlas Global Commerce S.A.",
        "type": "company",
        "jurisdiction": "CH",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Hans Mueller", "title": "Trustee", "country": "CH"},
        ],
    },
    {
        "id": "SC-2024-005",
        "name": "Blue Ocean Shipping Ltd.",
        "type": "company",
        "jurisdiction": "MH",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Dmitri Volkov", "title": "Beneficial Owner", "country": "RU"},
        ],
    },
    {
        "id": "SC-2024-006",
        "name": "Crestview Financial Corp.",
        "type": "company",
        "jurisdiction": "BS",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "William Turner", "title": "Director", "country": "BS"},
        ],
    },
    {
        "id": "SC-2024-007",
        "name": "Harbour Island Asset Management Ltd.",
        "type": "company",
        "jurisdiction": "BS",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Richard Spencer", "title": "Fund Manager", "country": "GB"},
        ],
    },
    {
        "id": "SC-2024-008",
        "name": "Sahara International Trading FZE",
        "type": "company",
        "jurisdiction": "AE",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Ahmed Al Mansoori", "title": "Managing Director", "country": "AE"},
        ],
    },
    {
        "id": "SC-2024-009",
        "name": "Luxor Capital Partners LLC",
        "type": "company",
        "jurisdiction": "LU",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Jean-Claude Dupont", "title": "Managing Partner", "country": "LU"},
        ],
    },
    {
        "id": "SC-2024-010",
        "name": "Pacific Rim Holdings Ltd.",
        "type": "company",
        "jurisdiction": "HK",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Li Wei", "title": "Director", "country": "HK"},
            {"name": "Chan Tai Man", "title": "Secretary", "country": "HK"},
        ],
    },
    {
        "id": "SC-2024-011",
        "name": "Caledonia Trust & Investment Ltd.",
        "type": "company",
        "jurisdiction": "JE",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Nigel Thornton-Brown", "title": "Trustee", "country": "JE"},
        ],
    },
    {
        "id": "SC-2024-012",
        "name": "Alpine Global Holdings AG",
        "type": "company",
        "jurisdiction": "LI",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Klaus Richter", "title": "Director", "country": "LI"},
        ],
    },
    {
        "id": "SC-2024-013",
        "name": "Sterling Overseas Corp.",
        "type": "company",
        "jurisdiction": "VU",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "James Whitfield", "title": "Nominee Director", "country": "VU"},
        ],
    },
    {
        "id": "SC-2024-014",
        "name": "Orion Commodities Trading Ltd.",
        "type": "company",
        "jurisdiction": "CY",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Andreas Papadopoulos", "title": "Director", "country": "CY"},
            {"name": "Nicos Anastasiou", "title": "Shareholder", "country": "CY"},
        ],
    },
    {
        "id": "SC-2024-015",
        "name": "Renaissance Financial Services Ltd.",
        "type": "company",
        "jurisdiction": "MT",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Joseph Borg", "title": "Director", "country": "MT"},
        ],
    },
    {
        "id": "SC-2024-016",
        "name": "Vanguard International Trade Corp.",
        "type": "company",
        "jurisdiction": "SG",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Tan Beng Seng", "title": "Director", "country": "SG"},
        ],
    },
    {
        "id": "SC-2024-017",
        "name": "Global Synergy Holdings Inc.",
        "type": "company",
        "jurisdiction": "SC",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Seychelles Corporate Management Ltd.", "title": "Registered Agent", "country": "SC"},
        ],
    },
    {
        "id": "SC-2024-018",
        "name": "Trident Offshore Services Ltd.",
        "type": "company",
        "jurisdiction": "DM",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Michael Davidson", "title": "Director", "country": "GB"},
        ],
    },
    {
        "id": "SC-2024-019",
        "name": "Apex Maritime Logistics Inc.",
        "type": "company",
        "jurisdiction": "LR",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Elena Kuznetsova", "title": "Beneficial Owner", "country": "RU"},
        ],
    },
    {
        "id": "SC-2024-020",
        "name": "Stonehaven Capital Partners Ltd.",
        "type": "company",
        "jurisdiction": "GG",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Philip Ashcroft", "title": "Managing Director", "country": "GG"},
        ],
    },
    {
        "id": "SC-2024-021",
        "name": "Golden Peony Holdings Ltd.",
        "type": "company",
        "jurisdiction": "MO",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Wong Ka Fai", "title": "Director", "country": "MO"},
        ],
    },
    {
        "id": "SC-2024-022",
        "name": "Bosphorus Trading & Investment AS",
        "type": "company",
        "jurisdiction": "TR",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Mehmet Yilmaz", "title": "CEO", "country": "TR"},
        ],
    },
    {
        "id": "SC-2024-023",
        "name": "Nordic Asset Management AB",
        "type": "company",
        "jurisdiction": "SE",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Erik Johansson", "title": "CEO", "country": "SE"},
        ],
    },
    {
        "id": "SC-2024-024",
        "name": "Ocean State Holdings Ltd.",
        "type": "company",
        "jurisdiction": "MU",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Rajesh Patel", "title": "Director", "country": "MU"},
        ],
    },
    {
        "id": "SC-2024-025",
        "name": "Capricorn Financial Group Ltd.",
        "type": "company",
        "jurisdiction": "GI",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Susan Gallagher", "title": "Director", "country": "GI"},
        ],
    },
    {
        "id": "SC-2024-026",
        "name": "Malta Fiduciary Services Ltd.",
        "type": "company",
        "jurisdiction": "MT",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Carmelo Vella", "title": "Director", "country": "MT"},
        ],
    },
    {
        "id": "SC-2024-027",
        "name": "Samara International Ltd.",
        "type": "company",
        "jurisdiction": "RU",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Dmitry Sokolov", "title": "CEO", "country": "RU"},
        ],
    },
    {
        "id": "SC-2024-028",
        "name": "Nevis Global Trust Services Ltd.",
        "type": "company",
        "jurisdiction": "KN",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Nevis Corporate Services Inc.", "title": "Registered Agent", "country": "KN"},
        ],
    },
    {
        "id": "SC-2024-029",
        "name": "BVI Commercial Enterprises Ltd.",
        "type": "company",
        "jurisdiction": "VG",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "BVI Fiduciary Services Ltd.", "title": "Registered Agent", "country": "VG"},
        ],
    },
    {
        "id": "SC-2024-030",
        "name": "Cayman Global Advisors Ltd.",
        "type": "company",
        "jurisdiction": "KY",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Cayman Corporate Management Ltd.", "title": "Registered Agent", "country": "KY"},
        ],
    },

    # ═══════════════════════════════════════════════════════════════
    # OFAC SANCTIONED ENTITIES (SDN List Patterns)
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "OFAC-SDN-001",
        "name": "Petróleos de Venezuela S.A. (PDVSA)",
        "type": "company",
        "jurisdiction": "VE",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Nicolás Maduro", "title": "President", "country": "VE"},
            {"name": "Tareck El Aissami", "title": "Former VP", "country": "VE"},
        ],
        "sanctions": ["OFAC SDN", "Venezuela Executive Order 13692"],
    },
    {
        "id": "OFAC-SDN-002",
        "name": "Rossiya Bank",
        "type": "company",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Dmitry Lebedev", "title": "Chairman", "country": "RU"},
        ],
        "sanctions": ["OFAC SDN", "Russia Executive Order 13661"],
    },
    {
        "id": "OFAC-SDN-003",
        "name": "Alfa-Bank",
        "type": "company",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Mikhail Fridman", "title": "Co-founder", "country": "RU"},
            {"name": "Petr Aven", "title": "Chairman", "country": "RU"},
        ],
        "sanctions": ["OFAC SDN", "Russia-related sanctions"],
    },
    {
        "id": "OFAC-SDN-004",
        "name": "VTB Bank",
        "type": "company",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Andrey Kostin", "title": "CEO", "country": "RU"},
        ],
        "sanctions": ["OFAC SDN", "Russia Executive Order 14024"],
    },
    {
        "id": "OFAC-SDN-005",
        "name": "Sberbank",
        "type": "company",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "German Gref", "title": "CEO", "country": "RU"},
        ],
        "sanctions": ["OFAC SDN", "Russia Executive Order 14024"],
    },
    {
        "id": "OFAC-SDN-006",
        "name": "Bank of China (North Korea Branch)",
        "type": "company",
        "jurisdiction": "KP",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "North Korea Executive Order 13466"],
    },
    {
        "id": "OFAC-SDN-007",
        "name": "KOMCA (Korea Mining and Trading Corporation)",
        "type": "company",
        "jurisdiction": "KP",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Kim Jong Un", "title": "Supreme Leader", "country": "KP"},
        ],
        "sanctions": ["OFAC SDN", "UN Security Council Resolution 2270"],
    },
    {
        "id": "OFAC-SDN-008",
        "name": "Iranian Revolutionary Guard Corps-Qods Force",
        "type": "company",
        "jurisdiction": "IR",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Esmail Qaani", "title": "Commander", "country": "IR"},
            {"name": "Qasem Soleimani", "title": "Former Commander (Deceased)", "country": "IR"},
        ],
        "sanctions": ["OFAC SDN", "Iran Executive Order 13224"],
    },
    {
        "id": "OFAC-SDN-009",
        "name": "National Iranian Oil Company (NIOC)",
        "type": "company",
        "jurisdiction": "IR",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Mohsen Khojastehmehr", "title": "CEO", "country": "IR"},
        ],
        "sanctions": ["OFAC SDN", "Iran Executive Order 13574"],
    },
    {
        "id": "OFAC-SDN-010",
        "name": "Ansarallah (Houthi Movement)",
        "type": "company",
        "jurisdiction": "YE",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Abdul-Malik al-Houthi", "title": "Leader", "country": "YE"},
        ],
        "sanctions": ["OFAC SDN", "Yemen Executive Order 13611"],
    },
    {
        "id": "OFAC-SDN-011",
        "name": "Hizballah",
        "type": "company",
        "jurisdiction": "LB",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Hassan Nasrallah", "title": "Secretary-General", "country": "LB"},
        ],
        "sanctions": ["OFAC SDN", "Hizballah Executive Order 13224"],
    },
    {
        "id": "OFAC-SDN-012",
        "name": "Boko Haram",
        "type": "company",
        "jurisdiction": "NG",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Abubakar Shekau", "title": "Former Leader (Deceased)", "country": "NG"},
        ],
        "sanctions": ["OFAC SDN", "UN Security Council Resolution 1267"],
    },
    {
        "id": "OFAC-SDN-013",
        "name": "Al-Shabaab",
        "type": "company",
        "jurisdiction": "SO",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Ahmed Diriye", "title": "Emir", "country": "SO"},
        ],
        "sanctions": ["OFAC SDN", "UN Security Council Resolution 1844"],
    },
    {
        "id": "OFAC-SDN-014",
        "name": "Islamic State of Iraq and Syria (ISIS)",
        "type": "company",
        "jurisdiction": "IQ",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Abu Hafs al-Hashimi al-Qurashi", "title": "Caliph", "country": "IQ"},
        ],
        "sanctions": ["OFAC SDN", "UN Security Council Resolution 1267"],
    },
    {
        "id": "OFAC-SDN-015",
        "name": "Taliban Electronic Media Center",
        "type": "company",
        "jurisdiction": "AF",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Haibatullah Akhundzada", "title": "Supreme Leader", "country": "AF"},
        ],
        "sanctions": ["OFAC SDN", "UN Security Council Resolution 1988"],
    },
    {
        "id": "OFAC-SDN-016",
        "name": "Syrian Arab Airlines",
        "type": "company",
        "jurisdiction": "SY",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Syria Executive Order 13572"],
    },
    {
        "id": "OFAC-SDN-017",
        "name": "Bashar al-Assad",
        "type": "individual",
        "jurisdiction": "SY",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Syria Executive Order 13572"],
    },
    {
        "id": "OFAC-SDN-018",
        "name": "Vladimir Putin",
        "type": "individual",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Russia Executive Order 14024"],
    },
    {
        "id": "OFAC-SDN-019",
        "name": "Sergei Shoigu",
        "type": "individual",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Russia Executive Order 14024"],
    },
    {
        "id": "OFAC-SDN-020",
        "name": "Sergei Lavrov",
        "type": "individual",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Russia Executive Order 14024"],
    },
    {
        "id": "OFAC-SDN-021",
        "name": "Alisher Usmanov",
        "type": "individual",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Russia oligarch sanctions"],
    },
    {
        "id": "OFAC-SDN-022",
        "name": "Gennady Timchenko",
        "type": "individual",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Russia Executive Order 13661"],
    },
    {
        "id": "OFAC-SDN-023",
        "name": "Igor Sechin",
        "type": "individual",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Russia Executive Order 13661"],
    },
    {
        "id": "OFAC-SDN-024",
        "name": "Dmitry Medvedev",
        "type": "individual",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Russia Executive Order 14024"],
    },
    {
        "id": "OFAC-SDN-025",
        "name": "Xi Jinping",
        "type": "individual",
        "jurisdiction": "CN",
        "status": "active",
        "risk_level": "low",
        "officers": [],
        "sanctions": [],
        "pep": True,
    },
    {
        "id": "OFAC-SDN-026",
        "name": "Kim Jong Un",
        "type": "individual",
        "jurisdiction": "KP",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "UN Security Council Resolution 1718"],
    },
    {
        "id": "OFAC-SDN-027",
        "name": "Nicolás Maduro Moros",
        "type": "individual",
        "jurisdiction": "VE",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Venezuela Executive Order 13692"],
    },
    {
        "id": "OFAC-SDN-028",
        "name": "Dianne Feinstein (Cuban sanctions entity)",
        "type": "company",
        "jurisdiction": "CU",
        "status": "sanctioned",
        "risk_level": "high",
        "officers": [],
        "sanctions": ["OFAC SDN", "Cuba Executive Order 13688"],
    },
    {
        "id": "OFAC-SDN-029",
        "name": "Cuban Armed Forces Enterprises (GAESA)",
        "type": "company",
        "jurisdiction": "CU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Luis Alberto Rodríguez López", "title": "CEO", "country": "CU"},
        ],
        "sanctions": ["OFAC SDN", "Cuba sanctions"],
    },
    {
        "id": "OFAC-SDN-030",
        "name": "Minsk Wheel Tractor Plant",
        "type": "company",
        "jurisdiction": "BY",
        "status": "sanctioned",
        "risk_level": "high",
        "officers": [],
        "sanctions": ["OFAC SDN", "Belarus Executive Order 13405"],
    },
    {
        "id": "OFAC-SDN-031",
        "name": "Belneftekhim",
        "type": "company",
        "jurisdiction": "BY",
        "status": "sanctioned",
        "risk_level": "high",
        "officers": [
            {"name": "Andrei Rybakou", "title": "CEO", "country": "BY"},
        ],
        "sanctions": ["OFAC SDN", "Belarus sanctions"],
    },
    {
        "id": "OFAC-SDN-032",
        "name": "Central Bank of Iran",
        "type": "company",
        "jurisdiction": "IR",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Mohammad Reza Farzin", "title": "Governor", "country": "IR"},
        ],
        "sanctions": ["OFAC SDN", "Iran Executive Order 13599"],
    },
    {
        "id": "OFAC-SDN-033",
        "name": "Zhongxing Telecommunications Equipment (ZTE) Iran",
        "type": "company",
        "jurisdiction": "CN",
        "status": "sanctioned",
        "risk_level": "high",
        "officers": [],
        "sanctions": ["OFAC SDN", "Iran sanctions"],
    },
    {
        "id": "OFAC-SDN-034",
        "name": "Nord Stream 2 AG",
        "type": "company",
        "jurisdiction": "CH",
        "status": "sanctioned",
        "risk_level": "high",
        "officers": [
            {"name": "Matthias Warnig", "title": "CEO", "country": "DE"},
        ],
        "sanctions": ["OFAC SDN", "Russia Executive Order 14024"],
    },
    {
        "id": "OFAC-SDN-035",
        "name": "Sovereign Wealth Fund of Venezuela",
        "type": "company",
        "jurisdiction": "VE",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Venezuela sanctions"],
    },
    {
        "id": "OFAC-SDN-036",
        "name": "Myanmar Oil and Gas Enterprise",
        "type": "company",
        "jurisdiction": "MM",
        "status": "sanctioned",
        "risk_level": "high",
        "officers": [],
        "sanctions": ["OFAC SDN", "Myanmar Executive Order 14014"],
    },
    {
        "id": "OFAC-SDN-037",
        "name": "Min Aung Hlaing",
        "type": "individual",
        "jurisdiction": "MM",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Myanmar Executive Order 14014"],
    },
    {
        "id": "OFAC-SDN-038",
        "name": "Russian Federal Security Service (FSB)",
        "type": "company",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Alexander Bortnikov", "title": "Director", "country": "RU"},
        ],
        "sanctions": ["OFAC SDN", "Russia Executive Order 14024"],
    },
    {
        "id": "OFAC-SDN-039",
        "name": "Kaspersky Lab",
        "type": "company",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "high",
        "officers": [
            {"name": "Eugene Kaspersky", "title": "CEO", "country": "RU"},
        ],
        "sanctions": ["OFAC SDN", "Russia-related sanctions"],
    },
    {
        "id": "OFAC-SDN-040",
        "name": "Continuity of Government (COG) entities (Iran)",
        "type": "company",
        "jurisdiction": "IR",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [],
        "sanctions": ["OFAC SDN", "Iran sanctions"],
    },

    # ═══════════════════════════════════════════════════════════════
    # POLITICALLY EXPOSED PERSONS (PEPs)
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "PEP-001",
        "name": "Joseph R. Biden",
        "type": "individual",
        "jurisdiction": "US",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "President of the United States",
    },
    {
        "id": "PEP-002",
        "name": "Kamala Harris",
        "type": "individual",
        "jurisdiction": "US",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Vice President of the United States",
    },
    {
        "id": "PEP-003",
        "name": "Ursula von der Leyen",
        "type": "individual",
        "jurisdiction": "DE",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "President of the European Commission",
    },
    {
        "id": "PEP-004",
        "name": "Emmanuel Macron",
        "type": "individual",
        "jurisdiction": "FR",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "President of France",
    },
    {
        "id": "PEP-005",
        "name": "Olaf Scholz",
        "type": "individual",
        "jurisdiction": "DE",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Chancellor of Germany",
    },
    {
        "id": "PEP-006",
        "name": "Rishi Sunak",
        "type": "individual",
        "jurisdiction": "GB",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Prime Minister of the United Kingdom",
    },
    {
        "id": "PEP-007",
        "name": "Narendra Modi",
        "type": "individual",
        "jurisdiction": "IN",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Prime Minister of India",
    },
    {
        "id": "PEP-008",
        "name": "Luiz Inácio Lula da Silva",
        "type": "individual",
        "jurisdiction": "BR",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "President of Brazil",
    },
    {
        "id": "PEP-009",
        "name": "Justin Trudeau",
        "type": "individual",
        "jurisdiction": "CA",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Prime Minister of Canada",
    },
    {
        "id": "PEP-010",
        "name": "Anthony Albanese",
        "type": "individual",
        "jurisdiction": "AU",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Prime Minister of Australia",
    },
    {
        "id": "PEP-011",
        "name": "Janet Yellen",
        "type": "individual",
        "jurisdiction": "US",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Secretary of the Treasury",
    },
    {
        "id": "PEP-012",
        "name": "Liz Truss",
        "type": "individual",
        "jurisdiction": "GB",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Former Prime Minister of the UK",
    },
    {
        "id": "PEP-013",
        "name": "Yoshihide Suga",
        "type": "individual",
        "jurisdiction": "JP",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Former Prime Minister of Japan",
    },
    {
        "id": "PEP-014",
        "name": "Moon Jae-in",
        "type": "individual",
        "jurisdiction": "KR",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Former President of South Korea",
    },
    {
        "id": "PEP-015",
        "name": "Jair Bolsonaro",
        "type": "individual",
        "jurisdiction": "BR",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Former President of Brazil",
    },
    {
        "id": "PEP-016",
        "name": "Sheikh Mohammed bin Rashid Al Maktoum",
        "type": "individual",
        "jurisdiction": "AE",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Prime Minister of UAE",
    },
    {
        "id": "PEP-017",
        "name": "Mohammed bin Salman Al Saud",
        "type": "individual",
        "jurisdiction": "SA",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Crown Prince of Saudi Arabia",
    },
    {
        "id": "PEP-018",
        "name": "Recep Tayyip Erdoğan",
        "type": "individual",
        "jurisdiction": "TR",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "President of Turkey",
    },
    {
        "id": "PEP-019",
        "name": "Volodymyr Zelenskyy",
        "type": "individual",
        "jurisdiction": "UA",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "President of Ukraine",
    },
    {
        "id": "PEP-020",
        "name": "Aleksandar Vučić",
        "type": "individual",
        "jurisdiction": "RS",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "President of Serbia",
    },
    {
        "id": "PEP-021",
        "name": "Viktor Orbán",
        "type": "individual",
        "jurisdiction": "HU",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Prime Minister of Hungary",
    },
    {
        "id": "PEP-022",
        "name": "Andrzej Duda",
        "type": "individual",
        "jurisdiction": "PL",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "President of Poland",
    },
    {
        "id": "PEP-023",
        "name": "Cyril Ramaphosa",
        "type": "individual",
        "jurisdiction": "ZA",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "President of South Africa",
    },
    {
        "id": "PEP-024",
        "name": "Claudia Sheinbaum",
        "type": "individual",
        "jurisdiction": "MX",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "President of Mexico",
    },
    {
        "id": "PEP-025",
        "name": "King Charles III",
        "type": "individual",
        "jurisdiction": "GB",
        "status": "active",
        "risk_level": "low",
        "officers": [],
        "pep": True,
        "position": "Monarch of the United Kingdom",
    },
    {
        "id": "PEP-026",
        "name": "Pope Francis",
        "type": "individual",
        "jurisdiction": "VA",
        "status": "active",
        "risk_level": "low",
        "officers": [],
        "pep": True,
        "position": "Head of State of Vatican City",
    },
    {
        "id": "PEP-027",
        "name": "Mette Frederiksen",
        "type": "individual",
        "jurisdiction": "DK",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Prime Minister of Denmark",
    },
    {
        "id": "PEP-028",
        "name": "Pedro Sánchez",
        "type": "individual",
        "jurisdiction": "ES",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Prime Minister of Spain",
    },
    {
        "id": "PEP-029",
        "name": "Giorgia Meloni",
        "type": "individual",
        "jurisdiction": "IT",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Prime Minister of Italy",
    },
    {
        "id": "PEP-030",
        "name": "Lee Hsien Loong",
        "type": "individual",
        "jurisdiction": "SG",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
        "pep": True,
        "position": "Former Prime Minister of Singapore",
    },

    # ═══════════════════════════════════════════════════════════════
    # TRUSTS AND FOUNDATIONS
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "TR-001",
        "name": "The Rockefeller Family Trust",
        "type": "trust",
        "jurisdiction": "US-NY",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "David Rockefeller Jr.", "title": "Trustee", "country": "US"},
        ],
    },
    {
        "id": "TR-002",
        "name": "Ford Foundation Trust",
        "type": "trust",
        "jurisdiction": "US-NY",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Darren Walker", "title": "President", "country": "US"},
        ],
    },
    {
        "id": "TR-003",
        "name": "Gates Foundation Trust",
        "type": "trust",
        "jurisdiction": "US-WA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Bill Gates", "title": "Co-Chair", "country": "US"},
            {"name": "Melinda French Gates", "title": "Co-Chair", "country": "US"},
        ],
    },
    {
        "id": "TR-004",
        "name": "Jersey Heritage Trust",
        "type": "trust",
        "jurisdiction": "JE",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Jersey Trust Services Ltd.", "title": "Corporate Trustee", "country": "JE"},
        ],
    },
    {
        "id": "TR-005",
        "name": "Guernsey Charitable Foundation",
        "type": "trust",
        "jurisdiction": "GG",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Guernsey Fiduciary Services", "title": "Trustee", "country": "GG"},
        ],
    },
    {
        "id": "TR-006",
        "name": "Swiss Anlage Stiftung",
        "type": "trust",
        "jurisdiction": "CH",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Hans-Peter Keller", "title": "Stiftungsrat", "country": "CH"},
        ],
    },
    {
        "id": "TR-007",
        "name": "Liechtenstein Familienstiftung",
        "type": "trust",
        "jurisdiction": "LI",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Dr. Franz Weiss", "title": "Protector", "country": "LI"},
        ],
    },
    {
        "id": "TR-008",
        "name": "Cook Islands Asset Protection Trust",
        "type": "trust",
        "jurisdiction": "CK",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Cook Islands Trust Corp.", "title": "Trustee", "country": "CK"},
        ],
    },
    {
        "id": "TR-009",
        "name": "Nevis Charitable Trust",
        "type": "trust",
        "jurisdiction": "KN",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Nevis Trust Services Inc.", "title": "Trustee", "country": "KN"},
        ],
    },
    {
        "id": "TR-010",
        "name": "Bermuda Purpose Trust",
        "type": "trust",
        "jurisdiction": "BM",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Bermuda Trust Company Ltd.", "title": "Trustee", "country": "BM"},
        ],
    },
    {
        "id": "TR-011",
        "name": "Isle of Man Discretionary Trust",
        "type": "trust",
        "jurisdiction": "IM",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Isle of Man Fiduciary Services", "title": "Trustee", "country": "IM"},
        ],
    },
    {
        "id": "TR-012",
        "name": "Bahamas Star Trust",
        "type": "trust",
        "jurisdiction": "BS",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Bahamas International Trust Co.", "title": "Trustee", "country": "BS"},
        ],
    },
    {
        "id": "TR-013",
        "name": "Cayman Islands STAR Trust",
        "type": "trust",
        "jurisdiction": "KY",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Cayman Trust Services Ltd.", "title": "Trustee", "country": "KY"},
        ],
    },
    {
        "id": "TR-014",
        "name": "Samara Trust Foundation",
        "type": "trust",
        "jurisdiction": "RU",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Moscow Trust Services Ltd.", "title": "Trustee", "country": "RU"},
        ],
    },
    {
        "id": "TR-015",
        "name": "The Chan Family Trust",
        "type": "trust",
        "jurisdiction": "HK",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Sir Run Run Shaw Trust Corp.", "title": "Trustee", "country": "HK"},
        ],
    },

    # ═══════════════════════════════════════════════════════════════
    # US SMALL BUSINESSES & LLCs
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "88-1234567",
        "name": "Main Street Bakery LLC",
        "type": "company",
        "jurisdiction": "US-TX",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Sarah Johnson", "title": "Owner", "country": "US"},
        ],
        "ein": "88-1234567",
        "naics": "311811",
        "boi_status": "required",
    },
    {
        "id": "88-2345678",
        "name": "Pacific Tech Solutions Inc.",
        "type": "company",
        "jurisdiction": "US-CA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Mike Chen", "title": "CEO", "country": "US"},
            {"name": "Lisa Wang", "title": "CTO", "country": "US"},
        ],
        "ein": "88-2345678",
        "naics": "541511",
        "boi_status": "required",
    },
    {
        "id": "88-3456789",
        "name": "Great Lakes Manufacturing Co.",
        "type": "company",
        "jurisdiction": "US-OH",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Robert Miller", "title": "President", "country": "US"},
        ],
        "ein": "88-3456789",
        "naics": "332710",
        "boi_status": "required",
    },
    {
        "id": "88-4567890",
        "name": "Empire State Realty Holdings LLC",
        "type": "company",
        "jurisdiction": "US-NY",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Anthony DiMarco", "title": "Managing Member", "country": "US"},
        ],
        "ein": "88-4567890",
        "naics": "531120",
        "boi_status": "required",
    },
    {
        "id": "88-5678901",
        "name": "Golden Gate Investments LLC",
        "type": "company",
        "jurisdiction": "US-CA",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "James Richardson", "title": "Managing Member", "country": "US"},
        ],
        "ein": "88-5678901",
        "naics": "523999",
        "boi_status": "required",
    },
    {
        "id": "88-6789012",
        "name": "Windy City Consulting Group",
        "type": "company",
        "jurisdiction": "US-IL",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Patricia O'Brien", "title": "Principal", "country": "US"},
        ],
        "ein": "88-6789012",
        "naics": "541611",
        "boi_status": "required",
    },
    {
        "id": "88-7890123",
        "name": "Liberty Healthcare Services Inc.",
        "type": "company",
        "jurisdiction": "US-PA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Dr. Helen Park", "title": "CEO", "country": "US"},
        ],
        "ein": "88-7890123",
        "naics": "621111",
        "boi_status": "required",
    },
    {
        "id": "88-8901234",
        "name": "Rocky Mountain Construction LLC",
        "type": "company",
        "jurisdiction": "US-CO",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Tom Watson", "title": "Member", "country": "US"},
        ],
        "ein": "88-8901234",
        "naics": "236115",
        "boi_status": "filed",
    },
    {
        "id": "88-9012345",
        "name": "Bay State Biotech Research Inc.",
        "type": "company",
        "jurisdiction": "US-MA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Dr. Sarah Kim", "title": "CSO", "country": "US"},
        ],
        "ein": "88-9012345",
        "naics": "541714",
        "boi_status": "required",
    },
    {
        "id": "88-0123456",
        "name": "Peach State Logistics LLC",
        "type": "company",
        "jurisdiction": "US-GA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Carlos Rivera", "title": "Owner", "country": "US"},
        ],
        "ein": "88-0123456",
        "naics": "484110",
        "boi_status": "filed",
    },
    {
        "id": "88-1122334",
        "name": "Sunshine State Hospitality Group",
        "type": "company",
        "jurisdiction": "US-FL",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Maria Garcia", "title": "CEO", "country": "US"},
        ],
        "ein": "88-1122334",
        "naics": "721110",
        "boi_status": "required",
    },
    {
        "id": "88-2233445",
        "name": "Lone Star Energy Partners LLC",
        "type": "company",
        "jurisdiction": "US-TX",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "William Bradford", "title": "Managing Partner", "country": "US"},
        ],
        "ein": "88-2233445",
        "naics": "211120",
        "boi_status": "filed",
    },
    {
        "id": "88-3344556",
        "name": "Silicon Prairie Software Inc.",
        "type": "company",
        "jurisdiction": "US-NE",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Jennifer Adams", "title": "CEO", "country": "US"},
        ],
        "ein": "88-3344556",
        "naics": "541511",
        "boi_status": "required",
    },
    {
        "id": "88-4455667",
        "name": "Evergreen Sustainable Solutions LLC",
        "type": "company",
        "jurisdiction": "US-OR",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "David Greenbaum", "title": "Member", "country": "US"},
        ],
        "ein": "88-4455667",
        "naics": "541620",
        "boi_status": "filed",
    },
    {
        "id": "88-5566778",
        "name": "Chesapeake Maritime Services Inc.",
        "type": "company",
        "jurisdiction": "US-VA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Captain John Stevens", "title": "President", "country": "US"},
        ],
        "ein": "88-5566778",
        "naics": "488390",
        "boi_status": "required",
    },
    {
        "id": "88-6677889",
        "name": "Desert Palm Distributors LLC",
        "type": "company",
        "jurisdiction": "US-AZ",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Ahmed Hassan", "title": "Owner", "country": "US"},
        ],
        "ein": "88-6677889",
        "naics": "424490",
        "boi_status": "filed",
    },
    {
        "id": "88-7788990",
        "name": "Volunteer State Financial Advisors LLC",
        "type": "company",
        "jurisdiction": "US-TN",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Margaret Thompson", "title": "CFP", "country": "US"},
        ],
        "ein": "88-7788990",
        "naics": "523930",
        "boi_status": "required",
    },
    {
        "id": "88-8899001",
        "name": "Aloha Hospitality Group LLC",
        "type": "company",
        "jurisdiction": "US-HI",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Keoni Nakamura", "title": "Managing Member", "country": "US"},
        ],
        "ein": "88-8899001",
        "naics": "721110",
        "boi_status": "filed",
    },
    {
        "id": "88-9900112",
        "name": "Green Mountain Organic Foods LLC",
        "type": "company",
        "jurisdiction": "US-VT",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Elizabeth Warren", "title": "CEO", "country": "US"},
        ],
        "ein": "88-9900112",
        "naics": "445230",
        "boi_status": "exempt",
    },
    {
        "id": "88-0011223",
        "name": "Magnolia State Manufacturing LLC",
        "type": "company",
        "jurisdiction": "US-MS",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "James T. Davis", "title": "President", "country": "US"},
        ],
        "ein": "88-0011223",
        "naics": "332321",
        "boi_status": "required",
    },
    {
        "id": "88-1122335",
        "name": "Garden State Pharma Research Inc.",
        "type": "company",
        "jurisdiction": "US-NJ",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Dr. Alan Roth", "title": "Director", "country": "US"},
        ],
        "ein": "88-1122335",
        "naics": "325412",
        "boi_status": "required",
    },
    {
        "id": "88-2233446",
        "name": "Old Dominion Defense Contractors LLC",
        "type": "company",
        "jurisdiction": "US-VA",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Col. Robert Hansen (Ret.)", "title": "Managing Director", "country": "US"},
        ],
        "ein": "88-2233446",
        "naics": "541330",
        "boi_status": "filed",
    },
    {
        "id": "88-3344557",
        "name": "Northern Lights Renewable Energy Inc.",
        "type": "company",
        "jurisdiction": "US-MN",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Erik Lundgren", "title": "CEO", "country": "US"},
        ],
        "ein": "88-3344557",
        "naics": "221115",
        "boi_status": "required",
    },
    {
        "id": "88-4455668",
        "name": "Prairie Home Financial LLC",
        "type": "company",
        "jurisdiction": "US-IA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Susan Miller", "title": "Principal", "country": "US"},
        ],
        "ein": "88-4455668",
        "naics": "522292",
        "boi_status": "filed",
    },
    {
        "id": "88-5566779",
        "name": "Blue Hen Consulting LLC",
        "type": "company",
        "jurisdiction": "US-DE",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Catherine O'Donnell", "title": "Member", "country": "US"},
        ],
        "ein": "88-5566779",
        "naics": "541618",
        "boi_status": "required",
    },
    {
        "id": "88-6677880",
        "name": "Mountaineer Mining Supply Co.",
        "type": "company",
        "jurisdiction": "US-WV",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Dennis Whittaker", "title": "President", "country": "US"},
        ],
        "ein": "88-6677880",
        "naics": "423810",
        "boi_status": "filed",
    },
    {
        "id": "88-7788991",
        "name": "Sooner Agricultural Holdings LLC",
        "type": "company",
        "jurisdiction": "US-OK",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "John R. Williams", "title": "Managing Member", "country": "US"},
        ],
        "ein": "88-7788991",
        "naics": "111190",
        "boi_status": "exempt",
    },
    {
        "id": "88-8899002",
        "name": "Badger State Brewing Company LLC",
        "type": "company",
        "jurisdiction": "US-WI",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Karl Schmidt", "title": "Master Brewer", "country": "US"},
        ],
        "ein": "88-8899002",
        "naics": "312120",
        "boi_status": "filed",
    },
    {
        "id": "88-9900113",
        "name": "Pelican State Marine Services LLC",
        "type": "company",
        "jurisdiction": "US-LA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Pierre LeBlanc", "title": "Captain", "country": "US"},
        ],
        "ein": "88-9900113",
        "naics": "488330",
        "boi_status": "required",
    },
    {
        "id": "88-0011224",
        "name": "First State Investment Partners LLC",
        "type": "company",
        "jurisdiction": "US-DE",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Jonathan Pierce", "title": "Managing Partner", "country": "US"},
            {"name": "Rebecca Torres", "title": "Compliance Officer", "country": "US"},
        ],
        "ein": "88-0011224",
        "naics": "523999",
        "boi_status": "filed",
    },
    {
        "id": "88-1122336",
        "name": "Equality State Energy Services Inc.",
        "type": "company",
        "jurisdiction": "US-WY",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Michael Thompson", "title": "CEO", "country": "US"},
        ],
        "ein": "88-1122336",
        "naics": "213112",
        "boi_status": "required",
    },
    {
        "id": "88-2233447",
        "name": "Treasure State Mining Corp.",
        "type": "company",
        "jurisdiction": "US-MT",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Franklin Brooks", "title": "President", "country": "US"},
        ],
        "ein": "88-2233447",
        "naics": "212220",
        "boi_status": "filed",
    },
    {
        "id": "88-3344558",
        "name": "Granite State Cybersecurity LLC",
        "type": "company",
        "jurisdiction": "US-NH",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Alex M. Turner", "title": "CISO", "country": "US"},
        ],
        "ein": "88-3344558",
        "naics": "541512",
        "boi_status": "required",
    },
    {
        "id": "88-4455669",
        "name": "Ocean State Seafood Distributors Inc.",
        "type": "company",
        "jurisdiction": "US-RI",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Paolo DiBenedetto", "title": "CEO", "country": "US"},
        ],
        "ein": "88-4455669",
        "naics": "424460",
        "boi_status": "filed",
    },
    {
        "id": "88-5566770",
        "name": "Land of Enchantment Aerospace LLC",
        "type": "company",
        "jurisdiction": "US-NM",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Dr. Elena Martinez", "title": "Chief Scientist", "country": "US"},
        ],
        "ein": "88-5566770",
        "naics": "336413",
        "boi_status": "required",
    },
    {
        "id": "88-6677881",
        "name": "Beehive State Financial LLC",
        "type": "company",
        "jurisdiction": "US-UT",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Nathan Young", "title": "Managing Member", "country": "US"},
        ],
        "ein": "88-6677881",
        "naics": "522110",
        "boi_status": "filed",
    },
    {
        "id": "88-7788992",
        "name": "Diamond State Logistics LLC",
        "type": "company",
        "jurisdiction": "US-DE",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Gregory Matthews", "title": "Managing Member", "country": "US"},
        ],
        "ein": "88-7788992",
        "naics": "484121",
        "boi_status": "required",
    },
    {
        "id": "88-8899003",
        "name": "Centennial State Fintech Inc.",
        "type": "company",
        "jurisdiction": "US-CO",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Aisha Patel", "title": "CEO", "country": "US"},
        ],
        "ein": "88-8899003",
        "naics": "522320",
        "boi_status": "required",
    },
    {
        "id": "88-9900114",
        "name": "Keystone State Pharma LLC",
        "type": "company",
        "jurisdiction": "US-PA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Dr. Michael Cohen", "title": "Director of Operations", "country": "US"},
        ],
        "ein": "88-9900114",
        "naics": "325412",
        "boi_status": "filed",
    },
    {
        "id": "88-0011225",
        "name": "Cornhusker State AgTech Inc.",
        "type": "company",
        "jurisdiction": "US-NE",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Karen Johansen", "title": "CEO", "country": "US"},
        ],
        "ein": "88-0011225",
        "naics": "115112",
        "boi_status": "exempt",
    },
    {
        "id": "88-1122337",
        "name": "Sunflower State Energy LLC",
        "type": "company",
        "jurisdiction": "US-KS",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Bradley Wilson", "title": "Managing Member", "country": "US"},
        ],
        "ein": "88-1122337",
        "naics": "221118",
        "boi_status": "filed",
    },
    {
        "id": "88-2233448",
        "name": "Natural State Timber Inc.",
        "type": "company",
        "jurisdiction": "US-AR",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "James McAllister", "title": "President", "country": "US"},
        ],
        "ein": "88-2233448",
        "naics": "113310",
        "boi_status": "exempt",
    },
    {
        "id": "88-3344559",
        "name": "Gem State Tech Solutions LLC",
        "type": "company",
        "jurisdiction": "US-ID",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Tyler Christensen", "title": "Member", "country": "US"},
        ],
        "ein": "88-3344559",
        "naics": "541511",
        "boi_status": "required",
    },
    {
        "id": "88-4455660",
        "name": "Silver State Gaming Technologies Inc.",
        "type": "company",
        "jurisdiction": "US-NV",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Vincent Romano", "title": "CEO", "country": "US"},
        ],
        "ein": "88-4455660",
        "naics": "713210",
        "boi_status": "filed",
    },
    {
        "id": "88-5566771",
        "name": "Paydirt Holdings LLC",
        "type": "company",
        "jurisdiction": "US-NV",
        "status": "active",
        "risk_level": "high",
        "officers": [
            {"name": "Vincent Romano", "title": "Beneficial Owner", "country": "US"},
            {"name": "Marco Bellini", "title": "Officer", "country": "US"},
        ],
        "ein": "88-5566771",
        "naics": "523999",
        "boi_status": "required",
    },

    # ═══════════════════════════════════════════════════════════════
    # INTERNATIONAL ENTITIES
    # ═══════════════════════════════════════════════════════════════
    {
        "id": "IE-001",
        "name": "Tata Sons Private Limited",
        "type": "company",
        "jurisdiction": "IN",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Natarajan Chandrasekaran", "title": "Chairman", "country": "IN"},
        ],
    },
    {
        "id": "IE-002",
        "name": "Samsung Electronics Co., Ltd.",
        "type": "company",
        "jurisdiction": "KR",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Lee Jae-yong", "title": "Executive Chairman", "country": "KR"},
        ],
    },
    {
        "id": "IE-003",
        "name": "Toyota Motor Corporation",
        "type": "company",
        "jurisdiction": "JP",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Akio Toyoda", "title": "Chairman", "country": "JP"},
        ],
    },
    {
        "id": "IE-004",
        "name": "BP p.l.c.",
        "type": "company",
        "jurisdiction": "GB",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Bernard Looney", "title": "CEO", "country": "GB"},
        ],
    },
    {
        "id": "IE-005",
        "name": "Royal Bank of Canada",
        "type": "company",
        "jurisdiction": "CA",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Dave McKay", "title": "CEO", "country": "CA"},
        ],
    },
    {
        "id": "IE-006",
        "name": "China Construction Bank Corporation",
        "type": "company",
        "jurisdiction": "CN",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Tian Guoli", "title": "Chairman", "country": "CN"},
        ],
    },
    {
        "id": "IE-007",
        "name": "Tencent Holdings Limited",
        "type": "company",
        "jurisdiction": "CN",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Ma Huateng", "title": "CEO", "country": "CN"},
        ],
    },
    {
        "id": "IE-008",
        "name": "Alibaba Group Holding Limited",
        "type": "company",
        "jurisdiction": "CN",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Eddie Wu", "title": "CEO", "country": "CN"},
        ],
    },
    {
        "id": "IE-009",
        "name": "Shell plc",
        "type": "company",
        "jurisdiction": "GB",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Wael Sawan", "title": "CEO", "country": "GB"},
        ],
    },
    {
        "id": "IE-010",
        "name": "Nestlé S.A.",
        "type": "company",
        "jurisdiction": "CH",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Ulf Mark Schneider", "title": "CEO", "country": "CH"},
        ],
    },
    {
        "id": "IE-011",
        "name": "Siemens AG",
        "type": "company",
        "jurisdiction": "DE",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Roland Busch", "title": "CEO", "country": "DE"},
        ],
    },
    {
        "id": "IE-012",
        "name": "TotalEnergies SE",
        "type": "company",
        "jurisdiction": "FR",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Patrick Pouyanné", "title": "CEO", "country": "FR"},
        ],
    },
    {
        "id": "IE-013",
        "name": "Deutsche Bank AG",
        "type": "company",
        "jurisdiction": "DE",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Christian Sewing", "title": "CEO", "country": "DE"},
        ],
    },
    {
        "id": "IE-014",
        "name": "Glencore plc",
        "type": "company",
        "jurisdiction": "CH",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Gary Nagle", "title": "CEO", "country": "CH"},
        ],
    },
    {
        "id": "IE-015",
        "name": "Trafigura Group Pte. Ltd.",
        "type": "company",
        "jurisdiction": "SG",
        "status": "active",
        "risk_level": "medium",
        "officers": [
            {"name": "Jeremy Weir", "title": "CEO", "country": "SG"},
        ],
    },
    {
        "id": "IE-016",
        "name": "Mitsubishi Corporation",
        "type": "company",
        "jurisdiction": "JP",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Takehiko Kakiuchi", "title": "CEO", "country": "JP"},
        ],
    },
    {
        "id": "IE-017",
        "name": "Harbin Electric International Company Limited",
        "type": "company",
        "jurisdiction": "CN",
        "status": "active",
        "risk_level": "medium",
        "officers": [],
    },
    {
        "id": "IE-018",
        "name": "Rosneft Oil Company",
        "type": "company",
        "jurisdiction": "RU",
        "status": "sanctioned",
        "risk_level": "critical",
        "officers": [
            {"name": "Igor Sechin", "title": "CEO", "country": "RU"},
        ],
        "sanctions": ["OFAC SDN", "Russia Executive Order 13661"],
    },
    {
        "id": "IE-019",
        "name": "Sinopec Group",
        "type": "company",
        "jurisdiction": "CN",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Ma Yongsheng", "title": "Chairman", "country": "CN"},
        ],
    },
    {
        "id": "IE-020",
        "name": "Vale S.A.",
        "type": "company",
        "jurisdiction": "BR",
        "status": "active",
        "risk_level": "low",
        "officers": [
            {"name": "Eduardo Bartolomeo", "title": "CEO", "country": "BR"},
        ],
    },
]

# Build lookup indices
_EIN_MAP: Dict[str, dict] = {}
_ID_MAP: Dict[str, dict] = {}
_OFFICER_INDEX: Dict[str, List[dict]] = {}
_JURISDICTION_INDEX: Dict[str, List[dict]] = {}

for ent in ENTITIES:
    _ID_MAP[ent["id"]] = ent
    if ent.get("ein"):
        _EIN_MAP[ent["ein"]] = ent
    j = ent["jurisdiction"]
    if j not in _JURISDICTION_INDEX:
        _JURISDICTION_INDEX[j] = []
    _JURISDICTION_INDEX[j].append(ent)
    for off in ent.get("officers", []):
        name = off["name"].lower()
        if name not in _OFFICER_INDEX:
            _OFFICER_INDEX[name] = []
        _OFFICER_INDEX[name].append(ent)


# ── Search Helpers ────────────────────────────────────────────────

def _normalize(s: str) -> str:
    """Lowercase and strip non-alphanumeric for fuzzy matching."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _match_score(query_parts: List[str], ent: dict) -> int:
    """Simple scoring: +5 per exact name match, +3 per partial name,
    +2 for jurisdiction, +1 for officer name match."""
    score = 0
    name_norm = _normalize(ent.get("name", ""))
    for qp in query_parts:
        qn = _normalize(qp)
        if qn == name_norm:
            score += 10
        elif qn in name_norm:
            score += 5
        # Check officers
        for off in ent.get("officers", []):
            on = _normalize(off["name"])
            if qn == on:
                score += 3
            elif qn in on:
                score += 1
    return score


# ── Exported API Functions ────────────────────────────────────────

def get_entity(entity_id: str) -> Optional[dict]:
    """Look up an entity by its ID (EIN or internal ID)."""
    # Check cache first
    cached = cache.get(f"entity:{entity_id}")
    if cached:
        return cached

    # Search by ID or EIN
    ent = _ID_MAP.get(entity_id) or _EIN_MAP.get(entity_id)
    if ent:
        cache.set(f"entity:{entity_id}", ent)
    return ent


def search_entities(
    query: Optional[str] = None,
    type: Optional[str] = None,
    jurisdiction: Optional[str] = None,
    limit: int = 20,
    **kwargs,
) -> List[dict]:
    """Search entities by name, type, jurisdiction, or other criteria.
    
    Also supports main.py kwargs: q, name, ein, state, boi_status, naics, offset.
    """
    results = list(ENTITIES)

    # Filter by query/q/name
    q = query or kwargs.get("q") or kwargs.get("name")
    if q:
        q_lower = q.lower()
        query_parts = q_lower.split()
        scored = []
        for ent in results:
            score = _match_score(query_parts, ent)
            if score > 0:
                scored.append((score, ent))
        # Also do basic containment search for low-score matches
        for ent in results:
            if _normalize(ent.get("name", "")).find(_normalize(q)) >= 0:
                score = _match_score(query_parts, ent)
                if score <= 0:
                    scored.append((1, ent))
        scored.sort(key=lambda x: -x[0])
        results = [ent for _, ent in scored]

    # Filter by type
    if type:
        results = [e for e in results if e.get("type") == type]
    type_val = kwargs.get("type")
    if type_val:
        results = [e for e in results if e.get("type") == type_val]

    # Filter by jurisdiction
    if jurisdiction:
        jur_norm = jurisdiction.upper()
        results = [e for e in results if e.get("jurisdiction", "").upper() == jur_norm]

    # Filter by state (jurisdiction prefix)
    state = kwargs.get("state")
    if state:
        state_norm = f"US-{state.upper()}"
        results = [e for e in results if e.get("jurisdiction", "").upper() == state_norm]

    # Filter by EIN
    ein = kwargs.get("ein")
    if ein:
        ein_norm = ein.strip()
        results = [e for e in results if e.get("ein", "").startswith(ein_norm)]

    # Filter by BOI status
    boi_status = kwargs.get("boi_status")
    if boi_status:
        results = [e for e in results if e.get("boi_status") == boi_status]

    # Filter by NAICS
    naics = kwargs.get("naics")
    if naics:
        results = [e for e in results if e.get("naics", "").startswith(naics)]

    # Apply offset
    offset = kwargs.get("offset", 0)

    # Sort by name if no query
    if not q:
        results.sort(key=lambda e: e.get("name", ""))

    total = len(results)
    sliced = results[offset:offset + limit]

    return {
        "results": sliced,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def lookup_officer(
    name: str,
    country: Optional[str] = None,
    **kwargs,
) -> List[dict]:
    """Find entities associated with an officer by name.
    
    Also supports main.py kwargs: limit, offset.
    """
    query = name.lower().strip()
    query_parts = query.split()

    matches = []
    for ent in ENTITIES:
        for off in ent.get("officers", []):
            on = off["name"].lower()
            # Match if all query parts are found in officer name
            if all(part in on for part in query_parts):
                if country:
                    if off.get("country", "").upper() != country.upper():
                        continue
                matches.append({
                    "officer": off,
                    "entity": {
                        "id": ent["id"],
                        "name": ent["name"],
                        "type": ent["type"],
                        "jurisdiction": ent["jurisdiction"],
                        "status": ent["status"],
                    },
                })
                break  # one match per entity

    limit = kwargs.get("limit", 20)
    offset = kwargs.get("offset", 0)

    total = len(matches)
    sliced = matches[offset:offset + limit]

    return {
        "results": sliced,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def compliance_check(
    entity_id: Optional[str] = None,
    standard: Optional[str] = "boi",
    **kwargs,
) -> dict:
    """Check entity compliance against a standard (BOI, OFAC, AML).
    
    Also supports main.py kwargs: ein, name.
    """
    # Resolve entity
    eid = entity_id or kwargs.get("ein")
    ent = None
    if eid:
        ent = get_entity(eid)

    if not ent:
        name = kwargs.get("name")
        if name:
            results = search_entities(query=name, limit=5)
            if results["results"]:
                ent = results["results"][0]

    if not ent:
        return {"error": "entity_not_found", "message": "No matching entity"}

    std = standard or "boi"

    # BOI compliance check
    if std == "boi":
        boi_status = ent.get("boi_status", "unknown")
        if boi_status == "exempt":
            return {
                "entity_id": ent["id"],
                "entity_name": ent["name"],
                "standard": "boi",
                "compliant": True,
                "status": "exempt",
                "details": "Entity is exempt from BOI filing (large operating company, 20+ FTEs, US premises)",
            }
        elif boi_status == "filed":
            return {
                "entity_id": ent["id"],
                "entity_name": ent["name"],
                "standard": "boi",
                "compliant": True,
                "status": "filed",
                "details": "BOI report has been filed with FinCEN",
            }
        elif boi_status == "required":
            return {
                "entity_id": ent["id"],
                "entity_name": ent["name"],
                "standard": "boi",
                "compliant": False,
                "status": "required",
                "details": "BOI filing required — entity must file Beneficial Ownership Information with FinCEN",
            }
        else:
            return {
                "entity_id": ent["id"],
                "entity_name": ent["name"],
                "standard": "boi",
                "compliant": False,
                "status": "unknown",
                "details": "BOI filing status unknown",
            }

    # OFAC sanctions check
    elif std == "ofac":
        sanctions = ent.get("sanctions", [])
        if sanctions:
            return {
                "entity_id": ent["id"],
                "entity_name": ent["name"],
                "standard": "ofac",
                "compliant": False,
                "status": "sanctioned",
                "sanctions": sanctions,
                "risk_level": ent.get("risk_level", "high"),
                "details": f"Entity is sanctioned under: {', '.join(sanctions)}",
            }
        else:
            return {
                "entity_id": ent["id"],
                "entity_name": ent["name"],
                "standard": "ofac",
                "compliant": True,
                "status": "clear",
                "risk_level": ent.get("risk_level", "low"),
                "details": "No OFAC sanctions found for this entity",
            }

    # AML/KYC enhanced check
    elif std == "aml":
        risk = ent.get("risk_level", "low")
        sanctions = ent.get("sanctions", [])
        is_pep = ent.get("pep", False)
        red_flags = []

        if risk in ("high", "critical"):
            red_flags.append(f"Entity has {risk} risk level")
        if sanctions:
            red_flags.append(f"Entity is under OFAC sanctions: {', '.join(sanctions)}")
        if is_pep:
            red_flags.append("Person is a Politically Exposed Person (PEP)")
        for off in ent.get("officers", []):
            if off.get("country", "") in ("RU", "KP", "IR", "SY", "CU", "VE"):
                red_flags.append(f"Officer {off['name']} connected to high-risk jurisdiction ({off['country']})")

        return {
            "entity_id": ent["id"],
            "entity_name": ent["name"],
            "standard": "aml",
            "compliant": len(red_flags) == 0,
            "risk_level": risk,
            "red_flags": red_flags,
            "details": "AML enhanced due diligence check complete",
        }

    else:
        return {
            "entity_id": ent["id"],
            "entity_name": ent["name"],
            "standard": std,
            "compliant": False,
            "error": f"Unknown compliance standard: {std}",
        }


def get_cache_stats() -> dict:
    """Get cache and database statistics matching what main.py expects."""
    total = len(ENTITIES)
    sanctions_count = sum(1 for e in ENTITIES if e.get("sanctions"))
    pep_count = sum(1 for e in ENTITIES if e.get("pep"))
    companies = sum(1 for e in ENTITIES if e.get("type") == "company")
    individuals = sum(1 for e in ENTITIES if e.get("type") == "individual")
    trusts = sum(1 for e in ENTITIES if e.get("type") == "trust")
    jurisdictions = len(set(e["jurisdiction"] for e in ENTITIES))

    return {
        "total_entities": total,
        "cache_age_seconds": 0,
        "sanctioned_entities": sanctions_count,
        "pep_entities": pep_count,
        "companies": companies,
        "individuals": individuals,
        "trusts": trusts,
        "jurisdictions": jurisdictions,
        "cache_stats": cache.stats(),
    }


def refresh_cache() -> dict:
    """Refresh (clear and re-warm) the in-memory cache."""
    cache.clear()
    # Pre-cache all entities by their IDs and EINs
    for ent in ENTITIES:
        cache.set(f"entity:{ent['id']}", ent)
        if ent.get("ein"):
            cache.set(f"entity:{ent['ein']}", ent)
    return {
        "count": len(ENTITIES),
        "status": "ok",
    }


# ── Warm on import ────────────────────────────────────────────────
refresh_cache()
