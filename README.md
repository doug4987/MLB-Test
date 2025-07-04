# MLB Props Betting System - Complete Documentation
## 06/27/2025 Full Working Implementation

### 🎯 **System Overview**

This is a complete, automated MLB props betting system that:
1. **Scrapes** daily MLB player props from EVAnalytics.com
2. **Analyzes** expected value and creates best odds tables
3. **Collects** MLB box scores for bet resolution
4. **Resolves** bets automatically using actual game results
5. **Calculates** ROI and generates comprehensive reports
6. **Exports** data to CSV for betting decisions

---

## 📁 **Core System Files**

### **1. working_scraper.py**
**Purpose**: Main scraper that collects MLB props data from EVAnalytics.com
**Key Features**:
- Chrome automation with Selenium
- Optimized pagination (reduced delays for faster scraping)
- Expected Value tier detection (A, B, C, D, E)
- Handles 250 rows per page with unlimited page scraping
- JSON data export with timestamps

**Usage**: `python3 working_scraper.py`
**Output**: `mlb_data_YYYYMMDD_HHMMSS.json`

---

### **2. database.py**
**Purpose**: Core database management system using SQLite
**Key Features**:
- Complete database schema with 6 main tables:
  - `props`: Raw scraped data
  - `best_odds`: Best odds for each player+market
  - `box_scores`: MLB game statistics
  - `bet_results`: Resolved bet outcomes
  - `games`: Game reference data
  - `players`: Player reference data
- Context managers for safe database operations
- Automatic indexing for performance

**Usage**: Imported by other modules
**Database**: `mlb_props.db`

---

### **3. daily_automation.py**
**Purpose**: Master orchestration script that runs the complete daily workflow
**Key Features**:
- Runs all 6 workflow steps in sequence
- Error handling and logging
- macOS notifications
- Comprehensive logging to daily log files
- Critical vs non-critical step handling

**Usage**: `python3 daily_automation.py`
**Workflow Steps**:
1. Scrape MLB Props (60 min timeout)
2. Create Best Props & Odds (10 min)
3. Create +EV Bets Table (5 min)
4. Collect Box Scores (30 min)
5. Resolve Bets (10 min)
6. Calculate ROI & Generate Reports (5 min)

---

### **4. create_best_odds_table.py**
**Purpose**: Creates optimized betting table with best odds for each unique bet
**Key Features**:
- Finds best Expected Value tier for each player+market combination
- Handles EV tier hierarchy (E > D > C > B > A)
- Creates `best_odds` table with performance indexes
- Analysis of tier breakdown and sportsbook distribution

**Usage**: `python3 create_best_odds_table.py`
**Output**: Populated `best_odds` table

---

### **5. update_plus_ev_workflow.py**
**Purpose**: Creates and manages +EV bets table for high-value betting opportunities
**Key Features**:
- Filters for Tier C, D, E bets only (high expected value)
- Integrates with ROI calculation system
- CSV export for daily betting decisions
- Performance analysis by tier

**Usage**: `python3 update_plus_ev_workflow.py`
**Output**: 
- `plus_ev_bets` table
- `plus_ev_bets_YYYY-MM-DD.csv`

---

### **6. box_score_scraper.py**
**Purpose**: Collects comprehensive MLB player statistics for completed games
**Key Features**:
- MLB Stats API integration (primary source)
- ESPN API fallback
- Comprehensive player statistics (batting, pitching, fielding)
- Game completion status verification
- Automatic team name mapping

**Usage**: `python3 box_score_scraper.py`
**Output**: Populated `box_scores` table

---

### **7. enhanced_bet_resolver.py**
**Purpose**: Automatically resolves bets using box score data
**Key Features**:
- Matches player names between betting data and MLB stats
- Supports 20+ different market types
- Calculates actual results vs betting lines
- High confidence auto-resolution (95%)
- Player name mapping system (stored in `player_name_mapping` table)

**Supported Markets**:
- Batting: Hits, Runs, RBIs, Home Runs, Doubles, Triples, Singles
- Combined: H+R+RBI, Total Bases, Walks, Strikeouts, Stolen Bases
- Pitching: Strikeouts, Earned Runs, Hits Allowed, Innings Pitched
- Fielding: Assists, Putouts, Errors

**Usage**: `python3 enhanced_bet_resolver.py`
**Output**: Populated `bet_results` table

---

### **8. calculate_tiered_betting_roi.py**
**Purpose**: Advanced ROI calculation with tiered betting amounts based on odds
**Key Features**:
- Tiered betting strategy:
  - $15 (odds >+750)
  - $25 (odds +500 to +750)
  - $50 (odds +250 to +500)
  - $100 (odds +100 to +250 or negative odds)
- Performance analysis by stake tier
- Win rate and ROI tracking
- Enhanced CSV exports with bet results

**Usage**: `python3 calculate_tiered_betting_roi.py`
**Output**: 
- Updated `bet_results` with ROI data
- `best_odds_tiered_roi_YYYY-MM-DD.csv`

---

### **9. export_to_csv.py**
**Purpose**: Simple CSV export of daily props data
**Key Features**:
- Exports all props for current date
- Quick reference for manual analysis

**Usage**: `python3 export_to_csv.py`
**Output**: `plays_today.csv`

---

### **10. morning_workflow.py**
**Purpose**: Comprehensive morning workflow combining all steps
**Key Features**:
- Integrates scraping, analysis, and resolution
- Betting recommendations by tier
- Yesterday's bet resolution
- Daily summary reporting

**Usage**: `python3 morning_workflow.py`
**Output**: Complete workflow execution

---

### **11. daily_scraper.py**
**Purpose**: Daily scraper orchestrator with enhanced processing
**Key Features**:
- Integrates with enhanced EV calculator
- Session tracking
- Data cleanup functionality
- Summary report generation

**Usage**: Part of morning workflow

---

### **12. enhanced_ev_calculator.py**
**Purpose**: Mathematical expected value calculation
**Key Features**:
- True EV calculation from projections and odds
- Multiple odds format support (American, Decimal)
- Probability estimation based on projections
- EV tier classification

**Usage**: Integrated into scraping workflow

---

## 🔄 **Daily Workflow Usage**

### **Complete Automated Run**:
```bash
python3 daily_automation.py
```

### **Individual Steps**:
```bash
# 1. Scrape today's props
python3 working_scraper.py

# 2. Create best odds table
python3 create_best_odds_table.py

# 3. Create +EV bets
python3 update_plus_ev_workflow.py

# 4. Collect box scores
python3 box_score_scraper.py

# 5. Resolve bets
python3 enhanced_bet_resolver.py

# 6. Calculate ROI
python3 calculate_tiered_betting_roi.py
```

---

## 📊 **Key Performance Metrics**

### **System Performance** (as of 06/27/2025):
- **Scraping Speed**: ~116 seconds for 1,250 records (optimized)
- **Resolution Rate**: 97.3% (108/111 bets resolved automatically)
- **Data Sources**: MLB Stats API (primary), ESPN (fallback)
- **Supported Markets**: 20+ different prop types

### **Recent Performance Example** (06/26/2025):
- **Total Bets**: 111
- **Resolved**: 108 (97.3%)
- **Win Rate**: 38.0% overall
- **Net Profit**: +$370.50
- **Best Tier**: Tier D (50% win rate, +$970 profit)

---

## 🗄️ **Database Schema**

### **Primary Tables**:
1. **props**: Raw scraped data (30+ columns)
2. **best_odds**: Optimized betting data 
3. **box_scores**: MLB player statistics
4. **bet_results**: Resolved outcomes with ROI
5. **games**: Game reference data
6. **players**: Player reference data

### **Key Indexes**:
- Player+Market combinations
- Expected Value tiers
- Scrape dates
- Game dates

---

## 📈 **Expected Value Tiers**

**Tier Hierarchy** (E = Best, A = Lowest):
- **Tier E**: Excellent Expected Value (Best bets)
- **Tier D**: Very Good Expected Value
- **Tier C**: Good Expected Value  
- **Tier B**: Fair Expected Value
- **Tier A**: Lower Expected Value

**Image Detection**: System detects tiers from `plus_e_5.png` (E) down to `plus_a_1.png` (A)

---

## 🎯 **Betting Strategy**

### **Recommended Focus**:
1. **Tier E bets**: Highest priority, excellent value
2. **Tier D bets**: Very good value, strong consideration
3. **Tier C bets**: Good value, moderate consideration

### **Stake Sizing**:
- **High odds (+750+)**: $15
- **Medium-high (+500-750)**: $25  
- **Medium (+250-500)**: $50
- **Low/Negative**: $100 to win $100

---

## 📝 **Log Files**

- **Daily Automation**: `logs/daily_automation_YYYY-MM-DD.log`
- **Individual Scrapers**: Various log outputs to console and files

---

## 🔧 **System Requirements**

- **Python 3.11+**
- **Chrome Browser** + ChromeDriver
- **Required Libraries**: selenium, requests, sqlite3
- **Credentials**: EVAnalytics.com login

---

## 🚀 **Quick Start**

1. **Run Complete System**:
   ```bash
   cd "/Users/doug/06-27 Full Working MLB Prop System"
   python3 daily_automation.py
   ```

2. **Check Results**:
   - View CSV exports for betting decisions
   - Check database for resolved bets and ROI
   - Review log files for any issues

3. **Monitor Performance**:
   - System sends macOS notifications on completion
   - Log files contain detailed execution info
   - Database tracks all historical performance

---

## ✅ **Verification Steps**

**System Health Check**:
1. Scraper completes without timeout
2. Best odds table populated
3. +EV bets identified correctly
4. Box scores collected for previous day
5. Bets resolved automatically
6. ROI calculated and exported

**Expected Outputs**:
- JSON file with scraped data
- CSV files for betting decisions
- Updated database with all workflow data
- Performance analysis and reports

---

This system represents a complete, production-ready MLB props betting automation solution with proven performance and comprehensive data tracking.
