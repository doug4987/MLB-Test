# MLB Props System - Complete Setup and Usage Guide

This document provides comprehensive instructions for recreating the entire MLB Props betting automation system from scratch.

## Table of Contents
1. [System Overview](#system-overview)
2. [Prerequisites](#prerequisites)
3. [Infrastructure Setup](#infrastructure-setup)
4. [System Components](#system-components)
5. [Database Schema](#database-schema)
6. [Daily Workflow](#daily-workflow)
7. [Common Issues and Solutions](#common-issues-and-solutions)
8. [Usage Instructions](#usage-instructions)
9. [Maintenance and Monitoring](#maintenance-and-monitoring)
10. [Data Management](#data-management)

## System Overview

The MLB Props System is an automated betting analysis tool that:
- Scrapes MLB prop bet odds from multiple sportsbooks
- Identifies +EV (positive expected value) betting opportunities
- Collects game results and box scores
- Resolves bet outcomes automatically
- Calculates profit/loss with tiered staking strategies
- Runs fully automated via cron job

**Key Performance Metrics (June 29, 2025 test):**
- Processed 26,260 prop bet records
- Identified 93 +EV plays across tiers C, D, E
- Achieved 86% automated bet resolution rate
- Tier performance: E (6.7% win), D (43.8% win), C (48.4% win)

## Prerequisites

### Required Accounts and Access
- Google Cloud Platform account with billing enabled
- gcloud CLI installed and authenticated
- SSH access to manage VMs
- Understanding of Python, SQLite, and cron jobs

### Local Development Environment
```bash
# Required Python packages
pip install requests beautifulsoup4 sqlite3 pandas numpy selenium

# Optional for local testing
pip install jupyter notebook
```

## Infrastructure Setup

### 1. Create Google Cloud VM

```bash
# Create the VM instance
gcloud compute instances create mlb-automation-vm \
    --zone=us-central1-a \
    --machine-type=e2-medium \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
    --create-disk=auto-delete=yes,boot=yes,device-name=mlb-automation-vm,image=projects/debian-cloud/global/images/debian-12-bookworm-v20240617,mode=rw,size=20,type=projects/your-project/zones/us-central1-a/diskTypes/pd-balanced \
    --no-shim-database-flags \
    --no-enable-display-device \
    --no-enable-ip-forwarding \
    --reservation-affinity=any
```

### 2. VM Initial Setup

```bash
# Connect to VM
gcloud compute ssh dougmyers4987@mlb-automation-vm --zone=us-central1-a

# Update system
sudo apt update && sudo apt upgrade -y

# Install Python and dependencies
sudo apt install -y python3 python3-pip python3-venv git cron

# Create project directory
mkdir -p /home/dougmyers4987/mlb-system
cd /home/dougmyers4987/mlb-system

# Install Python packages
pip3 install requests beautifulsoup4 pandas numpy selenium sqlite3

# Install Chrome for Selenium (if needed)
wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
sudo sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list'
sudo apt update
sudo apt install -y google-chrome-stable

# Install ChromeDriver
wget https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
sudo mv chromedriver /usr/local/bin/
sudo chmod +x /usr/local/bin/chromedriver
```

### 3. Upload System Files

From your local machine:
```bash
# Upload all system files to VM
gcloud compute scp --recurse /path/to/mlb-system/* dougmyers4987@mlb-automation-vm:/home/dougmyers4987/mlb-system/ --zone=us-central1-a

# Make Python scripts executable
gcloud compute ssh dougmyers4987@mlb-automation-vm --zone=us-central1-a --command="chmod +x /home/dougmyers4987/mlb-system/*.py"
```

## System Components

### Core Python Scripts

1. **daily_scraper.py** - Main odds scraping engine
   - **CRITICAL FIX**: Pagination bug was fixed to handle all pages in one run
   - **Key Issue**: Original version would restart from page 1 after processing each page
   - **Solution**: Modified to properly increment page numbers and continue until no more data

2. **create_best_odds_table.py** - Processes scraped odds into best odds per market
   - Groups by player and market type
   - Selects highest odds for each combination
   - Creates normalized comparison table

3. **create_plus_ev_bets_table.py** - Identifies +EV opportunities
   - **CRITICAL DATABASE FIX**: Requires `mapping_type` column in `player_name_mapping` table
   - Calculates expected value using probability models
   - Assigns tier classifications (A, B, C, D, E)

4. **box_score_scraper.py** - Collects game results and player stats
   - Scrapes MLB.com for official box scores
   - Stores player performance data for bet resolution

5. **enhanced_bet_resolver.py** - Resolves bet outcomes
   - **DATE FORMAT ISSUE**: Database stores dates as "Jun 29" format, not "2025-06-29"
   - **CASE SENSITIVITY**: Outcomes stored as lowercase "win"/"loss", not "Win"/"Loss"
   - Matches player stats to betting markets
   - High confidence resolution (~86% success rate)

6. **calculate_tiered_betting_roi.py** - Profit/loss analysis
   - **ODDS PARSING**: Extracts American odds from stored strings like "+150" or "-110"
   - Implements tiered staking: E=3 units, D=2 units, C=1 unit
   - Calculates ROI and performance metrics

### Database Schema

**Critical Tables and Columns:**

```sql
-- Main odds storage
CREATE TABLE mlb_props (
    id INTEGER PRIMARY KEY,
    date TEXT,
    player TEXT,
    market TEXT,
    odds TEXT,
    sportsbook TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Best odds per market
CREATE TABLE best_odds (
    id INTEGER PRIMARY KEY,
    player TEXT,
    market TEXT,
    odds TEXT,
    sportsbook TEXT,
    date TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- +EV opportunities  
CREATE TABLE plus_ev_bets (
    id INTEGER PRIMARY KEY,
    player TEXT,
    market TEXT,
    odds TEXT,
    sportsbook TEXT,
    implied_prob REAL,
    fair_prob REAL,
    edge REAL,
    tier TEXT,
    date TEXT,
    outcome TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Player name mapping (CRITICAL: must have mapping_type column)
CREATE TABLE player_name_mapping (
    id INTEGER PRIMARY KEY,
    canonical_name TEXT,
    variant_name TEXT,
    mapping_type TEXT,  -- THIS COLUMN WAS MISSING INITIALLY
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Box score data
CREATE TABLE box_scores (
    id INTEGER PRIMARY KEY,
    date TEXT,
    player TEXT,
    team TEXT,
    position TEXT,
    hits INTEGER,
    runs INTEGER,
    rbis INTEGER,
    strikeouts INTEGER,
    walks INTEGER,
    stolen_bases INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Automation Scripts

**run_daily_automation.sh:**
```bash
#!/bin/bash
cd /home/dougmyers4987/mlb-system
YESTERDAY=$(date -d "yesterday" +"%b %d")
echo "Starting MLB automation for $YESTERDAY" >> automation.log
python3 daily_scraper.py >> automation.log 2>&1
python3 create_best_odds_table.py >> automation.log 2>&1
python3 box_score_scraper.py >> automation.log 2>&1  
python3 enhanced_bet_resolver.py >> automation.log 2>&1
python3 calculate_tiered_betting_roi.py "$YESTERDAY" >> automation.log 2>&1
echo "Completed MLB automation for $YESTERDAY" >> automation.log
```

**Cron Job Setup:**
```bash
# Run daily at 8:00 AM Eastern (13:00 UTC)
0 13 * * * /home/dougmyers4987/mlb-system/run_daily_automation.sh >> /home/dougmyers4987/mlb-system/automation.log 2>&1
```

## Daily Workflow

### Automated Process (8:00 AM Eastern Daily)

1. **Odds Scraping** (daily_scraper.py)
   - Scrapes multiple sportsbooks for MLB props
   - **Expected Volume**: ~25,000+ records per day
   - Stores raw odds data with timestamps

2. **Best Odds Calculation** (create_best_odds_table.py)
   - Processes raw odds into best available per market
   - **Expected Output**: ~2,500-3,000 unique player+market combinations

3. **+EV Identification** (create_plus_ev_bets_table.py)
   - Calculates expected value for each betting opportunity
   - **Expected +EV Plays**: 80-120 per day across all tiers
   - Assigns tier classifications based on edge size

4. **Box Score Collection** (box_score_scraper.py)
   - Collects previous day's game results
   - **Typical Games**: 12-15 MLB games per day
   - **Player Stats**: 400-500 player performance records

5. **Bet Resolution** (enhanced_bet_resolver.py)
   - Matches +EV plays to actual results
   - **Resolution Rate**: 85-90% automated success
   - **Typical Errors**: 10-15% due to name mismatches or missing data

6. **ROI Analysis** (calculate_tiered_betting_roi.py)
   - Calculates profit/loss using tiered staking
   - Generates performance reports by tier

## Common Issues and Solutions

### 1. Pagination Bug in Scraper
**Problem**: Scraper would restart from page 1 instead of continuing
**Symptoms**: Only getting first page of results repeatedly
**Solution**: Fixed page increment logic in daily_scraper.py
```python
# WRONG (old code):
page = 1
while True:
    # process page
    page = 1  # BUG: resets to 1

# CORRECT (fixed code):
page = 1
while True:
    # process page
    page += 1  # properly increments
```

### 2. Missing Database Column
**Problem**: `mapping_type` column missing from `player_name_mapping` table
**Symptoms**: Error when running create_plus_ev_bets_table.py
**Solution**: Add missing column
```sql
ALTER TABLE player_name_mapping ADD COLUMN mapping_type TEXT;
```

### 3. Date Format Mismatches
**Problem**: Database stores dates as "Jun 29" but queries use "2025-06-29"
**Symptoms**: No results returned from date-filtered queries
**Solution**: Use consistent date format
```python
# WRONG:
date_filter = "2025-06-29"

# CORRECT:
date_filter = "Jun 29"
```

### 4. Case Sensitivity in Outcomes
**Problem**: Outcomes stored as lowercase but queries expect titlecase
**Symptoms**: Incorrect win/loss categorization
**Solution**: Normalize case in queries
```python
# WRONG:
WHERE outcome = 'Win'

# CORRECT:
WHERE LOWER(outcome) = 'win'
```

### 5. Odds Parsing Issues
**Problem**: American odds stored as strings like "+150" need numeric extraction
**Symptoms**: Incorrect profit calculations
**Solution**: Parse odds strings properly
```python
def parse_american_odds(odds_str):
    if odds_str.startswith('+'):
        return int(odds_str[1:])
    else:
        return int(odds_str)
```

### 6. VM Timezone Issues
**Problem**: Cron runs in UTC but system expects Eastern time
**Solution**: Use UTC time for cron (13:00 UTC = 8:00 AM Eastern)

### 7. Memory Issues on VM
**Problem**: Large datasets can cause memory issues on small VMs
**Solution**: Use e2-medium or larger instance type, process data in chunks

## Usage Instructions

### Manual System Operation

**1. Run Complete Daily Workflow:**
```bash
cd /home/dougmyers4987/mlb-system
./run_daily_automation.sh
```

**2. Run Individual Components:**
```bash
# Scrape odds
python3 daily_scraper.py

# Calculate best odds
python3 create_best_odds_table.py

# Collect box scores
python3 box_score_scraper.py

# Resolve bets
python3 enhanced_bet_resolver.py

# Calculate ROI for specific date
python3 calculate_tiered_betting_roi.py "Jun 29"
```

**3. Database Queries:**
```bash
# Access database
sqlite3 mlb_props.db

# View +EV plays for date
SELECT * FROM plus_ev_bets WHERE date = 'Jun 29' AND outcome IS NOT NULL;

# Check system status
SELECT COUNT(*) as total_records FROM mlb_props WHERE date = 'Jun 29';
SELECT COUNT(*) as resolved_bets FROM plus_ev_bets WHERE outcome IS NOT NULL;
```

### Performance Analysis Queries

**Daily Summary:**
```sql
SELECT 
    tier,
    COUNT(*) as total_plays,
    SUM(CASE WHEN LOWER(outcome) = 'win' THEN 1 ELSE 0 END) as wins,
    ROUND(100.0 * SUM(CASE WHEN LOWER(outcome) = 'win' THEN 1 ELSE 0 END) / COUNT(*), 1) as win_rate
FROM plus_ev_bets 
WHERE date = 'Jun 29' AND outcome IS NOT NULL
GROUP BY tier
ORDER BY tier;
```

**Profit Analysis:**
```sql
-- This requires the ROI calculation script for proper profit computation
```

### Data Export

**Export to CSV:**
```bash
python3 export_to_csv.py
```

**Download from VM:**
```bash
# Download database
gcloud compute scp dougmyers4987@mlb-automation-vm:/home/dougmyers4987/mlb-system/mlb_props.db ./mlb_props.db --zone=us-central1-a

# Download logs
gcloud compute scp dougmyers4987@mlb-automation-vm:/home/dougmyers4987/mlb-system/automation.log ./automation.log --zone=us-central1-a
```

## Maintenance and Monitoring

### Daily Monitoring

**1. Check Automation Status:**
```bash
gcloud compute ssh dougmyers4987@mlb-automation-vm --zone=us-central1-a --command="tail -20 /home/dougmyers4987/mlb-system/automation.log"
```

**2. Verify Data Collection:**
```bash
# Check recent records count
gcloud compute ssh dougmyers4987@mlb-automation-vm --zone=us-central1-a --command="sqlite3 /home/dougmyers4987/mlb-system/mlb_props.db 'SELECT COUNT(*) FROM mlb_props WHERE date = strftime(\"%b %d\", \"now\", \"-1 day\");'"
```

**3. Monitor System Resources:**
```bash
gcloud compute ssh dougmyers4987@mlb-automation-vm --zone=us-central1-a --command="df -h && free -h"
```

### Weekly Maintenance

**1. Database Cleanup:**
```sql
-- Remove records older than 30 days
DELETE FROM mlb_props WHERE created_at < datetime('now', '-30 days');
VACUUM;
```

**2. Log Rotation:**
```bash
# Archive old logs
mv automation.log automation_$(date +%Y%m%d).log
touch automation.log
```

### Troubleshooting Common Issues

**No Data Scraped:**
- Check internet connectivity on VM
- Verify sportsbook sites are accessible
- Check for website structure changes

**Low +EV Play Count:**
- Verify odds scraping completed successfully
- Check probability calculation models
- Review tier threshold settings

**Poor Bet Resolution Rate:**
- Check MLB.com accessibility
- Verify player name mapping table
- Review box score data quality

**Automation Not Running:**
- Check cron job status: `crontab -l`
- Verify script permissions: `ls -la run_daily_automation.sh`
- Check system time and timezone settings

## Data Management

### Backup Strategy

**1. Daily Database Backup:**
```bash
# Add to cron job
0 14 * * * cp /home/dougmyers4987/mlb-system/mlb_props.db /home/dougmyers4987/mlb-system/backups/mlb_props_$(date +\%Y\%m\%d).db
```

**2. Weekly Local Backup:**
```bash
# Download weekly
gcloud compute scp dougmyers4987@mlb-automation-vm:/home/dougmyers4987/mlb-system/mlb_props.db ./backups/mlb_props_$(date +%Y%m%d).db --zone=us-central1-a
```

### Performance Optimization

**Database Indexing:**
```sql
CREATE INDEX idx_mlb_props_date ON mlb_props(date);
CREATE INDEX idx_mlb_props_player ON mlb_props(player);
CREATE INDEX idx_plus_ev_bets_date ON plus_ev_bets(date);
CREATE INDEX idx_plus_ev_bets_tier ON plus_ev_bets(tier);
```

**VM Scaling:**
- Monitor CPU and memory usage
- Scale up during high-volume days
- Consider disk space for database growth

## System Recreation Checklist

If recreating the system from scratch:

### Infrastructure Setup
- [ ] Create GCP VM with appropriate specifications
- [ ] Install required system packages
- [ ] Set up Python environment with dependencies
- [ ] Configure Chrome/ChromeDriver for scraping

### Code Deployment
- [ ] Upload all Python scripts
- [ ] Set executable permissions
- [ ] Verify database schema with all required columns
- [ ] Test individual components manually

### Database Setup
- [ ] Create SQLite database
- [ ] Run schema creation scripts
- [ ] Add required indexes
- [ ] Verify player_name_mapping has mapping_type column

### Automation Setup
- [ ] Upload automation script
- [ ] Set correct permissions
- [ ] Add cron job with proper timezone
- [ ] Test automation script manually

### Validation
- [ ] Run complete workflow manually
- [ ] Verify data collection volumes
- [ ] Check bet resolution rates
- [ ] Confirm ROI calculations

### Monitoring Setup
- [ ] Set up log monitoring
- [ ] Create backup procedures
- [ ] Document troubleshooting procedures
- [ ] Test data export processes

## Key Success Metrics

**Daily Targets:**
- Odds scraped: 20,000+ records
- Best odds calculated: 2,000+ unique markets
- +EV plays identified: 80-120 plays
- Box scores collected: 400+ player stats
- Bet resolution rate: 85%+ automated

**System Health Indicators:**
- Automation runs complete daily
- Database size grows consistently
- Error rates remain under 15%
- VM resources within acceptable limits

This comprehensive guide should enable complete system recreation and operation. The key is careful attention to the common pitfalls documented here, particularly around date formats, database schema, and pagination logic.
