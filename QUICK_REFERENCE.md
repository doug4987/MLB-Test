# MLB Props System - Quick Reference

## 🚀 **Quick Start Commands**

### **Run Complete System**
```bash
python3 daily_automation.py
```

### **Individual Components**
```bash
# 1. Scrape today's props
python3 working_scraper.py

# 2. Create best odds
python3 create_best_odds_table.py

# 3. Generate +EV bets
python3 update_plus_ev_workflow.py

# 4. Get box scores
python3 box_score_scraper.py

# 5. Resolve bets
python3 enhanced_bet_resolver.py

# 6. Calculate ROI
python3 calculate_tiered_betting_roi.py

# 7. Export CSV
python3 export_to_csv.py
```

## 📁 **File Summary**

| File | Purpose | Runtime | Output |
|------|---------|---------|--------|
| `working_scraper.py` | Scrape props from EVAnalytics | ~2-3 min | JSON data |
| `database.py` | Database management | N/A | SQLite DB |
| `daily_automation.py` | Master workflow orchestrator | ~10-15 min | Complete workflow |
| `create_best_odds_table.py` | Optimize betting opportunities | ~30 sec | best_odds table |
| `update_plus_ev_workflow.py` | High-value bet identification | ~30 sec | +EV CSV |
| `box_score_scraper.py` | Collect MLB statistics | ~2-5 min | box_scores table |
| `enhanced_bet_resolver.py` | Auto-resolve bet outcomes | ~1 min | bet_results table |
| `calculate_tiered_betting_roi.py` | ROI analysis & reporting | ~1 min | ROI CSV |
| `export_to_csv.py` | Export daily props | ~10 sec | plays_today.csv |

## 🗄️ **Database Tables**

| Table | Records | Purpose |
|-------|---------|---------|
| `props` | ~1,250/day | Raw scraped data |
| `best_odds` | ~500/day | Optimized betting data |
| `plus_ev_bets` | ~18/day | High-value opportunities |
| `box_scores` | ~248/game day | MLB player statistics |
| `bet_results` | ~111/day | Resolved outcomes + ROI |
| `games` | ~15/day | Game reference data |

## 📊 **Key Metrics**

### **Performance Targets**
- **Scraping**: 1,250 props in ~2-3 minutes
- **Resolution Rate**: >95% automatic bet resolution
- **Processing Time**: Complete workflow in <15 minutes
- **Data Quality**: Tier E/D/C bets for betting focus

### **Expected Outputs**
- **JSON**: `mlb_data_YYYYMMDD_HHMMSS.json`
- **CSV Files**:
  - `plays_today.csv` (all props)
  - `plus_ev_bets_YYYY-MM-DD.csv` (high-value bets)
  - `best_odds_tiered_roi_YYYY-MM-DD.csv` (ROI analysis)

## 🎯 **Betting Strategy**

### **Tier Priority**
1. **Tier E**: Excellent value (highest priority)
2. **Tier D**: Very good value (strong consideration)
3. **Tier C**: Good value (moderate consideration)

### **Stake Amounts**
- **+750+ odds**: $15
- **+500 to +750**: $25
- **+250 to +500**: $50
- **Negative odds**: $100 (to win $100)

## 🔍 **Troubleshooting**

### **Common Issues**
1. **Scraper timeout**: Check internet connection, retry
2. **Login failure**: Verify EVAnalytics.com credentials
3. **No box scores**: Games may not be completed yet
4. **Resolution errors**: Check player name mappings

### **Log Locations**
- **Daily logs**: `logs/daily_automation_YYYY-MM-DD.log`
- **Console output**: Real-time status updates

## 📈 **Monitoring**

### **Success Indicators**
- ✅ Scraper completes without timeout
- ✅ ~500 best odds created
- ✅ ~18 +EV bets identified
- ✅ >95% bet resolution rate
- ✅ CSV files generated

### **Performance Tracking**
- **Win Rate**: Monitor by tier (Tier D target: ~50%)
- **ROI**: Track profitability by stake amount
- **Resolution**: Ensure >95% auto-resolution

## 🛠️ **System Requirements**

### **Dependencies**
- Python 3.11+
- Chrome Browser + ChromeDriver
- SQLite3
- Required packages: selenium, requests

### **Credentials**
- EVAnalytics.com login credentials in `working_scraper.py`

## 📱 **Notifications**

System sends macOS notifications for:
- Workflow start
- Workflow completion
- Critical failures

## 🔄 **Maintenance**

### **Daily Tasks**
- Review CSV exports for betting decisions
- Monitor log files for errors
- Check database for data quality

### **Weekly Tasks**
- Review performance metrics
- Update player name mappings if needed
- Clean old log files

This quick reference provides everything needed for daily operation of the MLB Props System.
