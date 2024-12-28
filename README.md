# Landbruget.dk

The purpose of this project is to organize information about the Danish agricultural sector and make it universally accessible and useful.

![Backend](https://img.shields.io/badge/backend-Python%203.9-green.svg)
![Frontend](https://img.shields.io/badge/frontend-React%2018-blue.svg)
![License](https://img.shields.io/badge/license-CC--BY-green.svg)
[![slack](https://img.shields.io/badge/slack-landbrugsdata-brightgreen.svg?logo=slack)](https://join.slack.com/t/landbrugsdata/shared_invite/zt-2xap8il6o-0YT4IV9sv9t72XZB5pVirA)

## Untraditional and temporary readme as we get started.

There is shitload of data the externalities of the agricultural sector, but it's hidden behind PDFs, SOAP APIs, WFS endpoints and subject access requests across many governmental agencies.
The goal is to fetch that data, transform it, combine it, and visualise it so it's super easy for anyone - citizens, journalists, activists, farmers and civil servants to understand what's up and to speed up the much needed transformation of our land.

The founder of this project is a noob fumbling around in Cursor, so the code will probably leave a lot to desire. So if you can code, do give us a hand!

Broad ideas (open for input!):
- Fetch and parse data in Python using Cloud Run
- Save it in Google Cloud Storage
- Clean it, transform it, combine it in Bigquery or Cloud Run (Bigquery seemed good for geospatial stuff!)
- Export it as PMTiles and datasets in Google Cloud Storage through a FastAPI. All data should be joinable on a field, CVR, unique person, parcel and/or CHR.
- Main page will be a visualisation in a React front-end, using Maplibre and D3.js.
- There could be data-heavy blogposts as secondary pages.
- Most views could be expected to come from either screenshots or embeds with specific filters applied

Have fun!

## 🚀 Quick Start

### Backend Setup

```bash
cd backend
python -m venv venv
source venv/bin/activate # or venv\Scripts\activate on Windows
pip install -r requirements.txt
uvicorn src.main:app --reload
```

### Frontend Setup

```bash
cd frontend
npm install
npm start
```

Visit:
- Frontend: http://localhost:3000
- API Documentation: http://localhost:8000/docs

## 🏗️ Architecture

### Backend (FastAPI + Python)
<pre>
├── src/
│ ├── sources/ # Data source implementations
│ │ ├── base.py # Base source class
│ │ ├── parsers/ # API/WFS sources
│ │ └── static/ # Static file sources
│ ├── main.py # FastAPI application
│ └── config.py # Configuration
</pre>


### Frontend (React + TypeScript)

<pre>
frontend/
└── src/
├── components/ # React components
├── api/ # Backend API client
├── hooks/ # Custom React hooks
└── types/ # TypeScript definitions
</pre>

## 📊 Data Sources

Data should have one or more of the following attributes to be useful:
- CVR number (company registration number)
- CHR number (herd registration number)
- geospatial coordinates (point or polygon)
- enhedsnummer (CVR individual identifier)
- bfe number (cadaster number)

### Live Sources
1. **Agricultural Fields (WFS)**
   - Updates: Weekly (Mondays 2 AM UTC)
   - Content: Field boundaries, crop types

### Static Sources
All static sources are updated through manual pull requests:
- Animal Welfare: Inspection reports and focus areas
- Biogas: Production data and methane leakage reports
- Fertilizer: Nitrogen data and climate calculations
- Herd Data: CHR (Central Husbandry Register)
- Pesticides: Usage statistics (2021-2023)
- Pig Movements: International transport (2017-2024)
- Subsidies: Support schemes and project grants
- Visa: Agricultural visa statistics
- Wetlands: Areas and carbon content

## 🤝 Contributing

### Prerequisites
- Python 3.9+
- Node.js 16+
- GDAL library
- Git

### Development Workflow
1. Fork the repository
2. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Make your changes
4. Run tests:
   ```bash
   # Backend
   cd backend
   pytest

   # Frontend
   cd frontend
   npm test
   ```
5. Submit a pull request

### Adding New Data Sources
1. Choose the appropriate directory:
   - `backend/src/sources/parsers/` for API/WFS sources
   - `backend/src/sources/static/` for static files

2. Create a new parser class:
   ```python
   from ..base import Source

   class YourSource(Source):
       async def fetch(self) -> pd.DataFrame:
           # Implement data fetching
           pass
   ```

3. Add configuration in `backend/src/config.py`
4. Update frontend types in `frontend/src/types/`
5. Add visualization in `frontend/src/components/`

## 🔄 Data Updates
- **WFS Sources**: Automatic weekly updates (Mondays 2 AM UTC)
- **Static Sources**: Manual updates via pull requests
- **Deployment**: Automatic to Google Cloud Run

## 🔧 Environment Setup

### Backend (.env)

```
GOOGLE_CLOUD_PROJECT=your-project-id
GCS_BUCKET=your-bucket-name
```

### Frontend (.env)

```
REACT_APP_API_URL=http://localhost:8000
```

## 📚 Documentation
- [Backend API Documentation](http://localhost:8000/docs)
- [Frontend Component Documentation](frontend/README.md)
- [Data Source Specifications](backend/README.md)

## 🐛 Troubleshooting

Common issues and solutions:

1. **GDAL Installation**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install gdal-bin libgdal-dev

   # macOS
   brew install gdal
   ```

2. **API Connection Issues**
   - Verify backend is running
   - Check CORS settings
   - Confirm environment variables

## 📝 License
This work is licensed under a Creative Commons Attribution 4.0 International License (CC-BY).

## 🙏 Acknowledgments
- Danish Agricultural Agency
- Danish Environmental Protection Agency
- Danish Energy Agency
- SIRI
