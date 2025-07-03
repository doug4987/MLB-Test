#!/usr/bin/env python3
"""
Daily +EV Bets Workflow Integration
This script should be run after best_odds are created to populate +EV Bets table
"""
import sqlite3
import sys
from datetime import date
from create_plus_ev_bets_table import (
    create_plus_ev_bets_table,
    populate_plus_ev_bets_from_best_odds, 
    calculate_plus_ev_roi,
    analyze_plus_ev_performance,
    create_plus_ev_csv_export
)

def update_plus_ev_bets_for_date(scrape_date=None):
    """Update +EV Bets table for a specific date"""
    
    if scrape_date is None:
        scrape_date = date.today().strftime('%Y-%m-%d')
    
    print(f"🎯 UPDATING +EV BETS FOR {scrape_date}")
    print("=" * 60)
    
    # Ensure table exists
    create_plus_ev_bets_table()
    
    # Check if best_odds exist for this date
    with sqlite3.connect('mlb_props.db') as conn:
        cursor = conn.execute("""
            SELECT COUNT(*) FROM best_odds 
            WHERE scrape_date = ?
              AND expected_value_tier IN ('A', 'B', 'C')
        """, (scrape_date,))
        
        available_bets = cursor.fetchone()[0]
        
        if available_bets == 0:
            print(f"❌ No E/D/C tier bets found in best_odds for {scrape_date}")
            return False
        
        print(f"📊 Found {available_bets} high-value bets available")
    
    # Populate +EV Bets from best_odds
    populate_plus_ev_bets_from_best_odds(scrape_date)
    
    # Calculate ROI (will only work if bet results exist)
    try:
        calculate_plus_ev_roi(scrape_date)
        analyze_plus_ev_performance(scrape_date)
    except Exception as e:
        print(f"⚠️  ROI calculation skipped (likely no bet results yet): {e}")
    
    # Create CSV export
    create_plus_ev_csv_export(scrape_date)
    
    print(f"✅ +EV Bets updated successfully for {scrape_date}")
    return True

def main():
    """Main execution"""
    scrape_date = sys.argv[1] if len(sys.argv) > 1 else None
    update_plus_ev_bets_for_date(scrape_date)

if __name__ == "__main__":
    main()
