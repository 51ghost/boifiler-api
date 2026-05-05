# BOIFiler API

**BOI (Beneficial Ownership Information) Filing & Entity Lookup API**

Search business entities, lookup officers, check BOI compliance status, and retrieve detailed company profiles — all through a simple REST API.

- **Live Demo:** https://boifiler.com
- **API Docs:** https://boifiler.com/docs (Swagger UI)
- **RapidAPI:** Available on RapidAPI marketplace

---

## Features

- **🔍 Entity Search** — Search 500+ entities by name, EIN, state, officer, NAICS code
- **🏢 Entity Details** — Full entity profiles with officers, ownership, financials
- **👤 Officer Lookup** — Find all entities linked to a person
- **✅ Compliance Check** — Determine BOI filing status and requirements
- **🚀 RapidAPI Ready** — Deploy instantly on RapidAPI marketplace
- **⚡ Blazing Fast** — In-memory cache with 6-hour TTL

## Quick Start

```bash
pip install -r requirements.txt
python main.py
```

Or with uvicorn:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Authentication

Pass your API key via the `X-API-Key` header:

```bash
curl -H "X-API-Key: boi_sk_live_demo_key" http://localhost:8000/v1/search?q=apple
```

Or via query parameter (for testing):

```bash
curl "http://localhost:8000/v1/search?q=apple&api_key=boi_sk_live_demo_key"
```

## Endpoints

### `GET /v1/health`
Health check — verify the API is running.

```bash
curl http://localhost:8000/v1/health
```

### `GET /v1/search`
Search entities by name, EIN, officer, state, or BOI status.

**Parameters:**
| Param | Type | Description |
|-------|------|-------------|
| `q` | string | General search query |
| `name` | string | Entity name filter |
| `ein` | string | EIN (full or partial) |
| `state` | string | 2-letter state code (CA, DE, NY, TX) |
| `boi_status` | string | Filter by BOI: `filed`, `not_filed`, `exempt`, `required` |
| `naics` | string | NAICS code |
| `limit` | int | Results per page (max 100) |
| `offset` | int | Pagination offset |

**Example:**
```bash
curl -H "X-API-Key: boi_sk_live_demo_key" \
  "https://api.boifiler.com/v1/search?q=apple&state=CA"
```

### `GET /v1/entity/{ein}`
Get detailed entity information by EIN.

**Example:**
```bash
curl -H "X-API-Key: boi_sk_live_demo_key" \
  "https://api.boifiler.com/v1/entity/88-1234567"
```

### `GET /v1/officer/{name}`
Look up officers and find entities associated with them.

**Example:**
```bash
curl -H "X-API-Key: boi_sk_live_demo_key" \
  "https://api.boifiler.com/v1/officer/Elon%20Musk"
```

### `GET /v1/compliance-check`
Check BOI compliance status for an entity.

**Parameters:**
| Param | Type | Description |
|-------|------|-------------|
| `ein` | string | EIN for compliance check |
| `name` | string | Entity name for compliance check |

**Example:**
```bash
curl -H "X-API-Key: boi_sk_live_demo_key" \
  "https://api.boifiler.com/v1/compliance-check?ein=88-1234567"
```

## Response Format

All responses are JSON:

```json
{
  "total": 3,
  "offset": 0,
  "limit": 20,
  "results": [
    {
      "ein": "13-4925230",
      "name": "JPMorgan Chase & Co.",
      "type": "Corporation",
      "jurisdiction": "Delaware",
      "boi_exempt": true,
      "boi_filing_status": null,
      "officer_count": 3
    }
  ]
}
```

## BOI Compliance Logic

The API implements Financial Crimes Enforcement Network (FinCEN) BOI rules:

| Entity Type | BOI Required? | Explanation |
|-------------|--------------|-------------|
| Large operating companies (20+ FTEs, $5M+ revenue) | Exempt | CTA § 1010.380(c)(2)(C) |
| SEC reporting companies | Exempt | Already report ownership |
| Tax-exempt entities (501(c)) | Exempt | Already regulated |
| Banks & credit unions | Exempt | Already regulated |
| Small LLCs & corps (<20 FTEs) | Required | Must file BOI report |
| Shell companies & SPVs | Required | High risk, must disclose |

## Deployment

### Railway

```bash
railway login
railway init
railway up
```

### Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BOIFILER_API_KEY` | `boi_sk_live_demo_key` | API key for auth |
| `RATE_LIMIT_PER_MIN` | `60` | Max requests per minute |
| `DEV_MODE` | `true` | Disable auth when true |
| `PORT` | `8000` | Server port |

## Data

The API comes with a curated dataset of **50+ entities** including:
- Fortune 500 companies (JPMorgan, Apple, Microsoft, Google, etc.)
- Major banks (Goldman Sachs, Bank of America, Citigroup)
- Tech startups (Stripe, OpenAI, SpaceX)
- Small businesses and LLCs
- Shell/SPV entities
- Nonprofits (Red Cross, Gates Foundation)

Data sourced from SEC EDGAR filings and state business registries. Cache TTL is 6 hours.

## License

MIT

---

Built for RapidAPI marketplace. Questions? support@boifiler.com
