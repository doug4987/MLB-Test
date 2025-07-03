#!/usr/bin/env python3
"""
+EV Bets Table Creation and Management
Creates a focused table for only the highest value bets (Tier A, B, C)
"""
import sqlite3
import re
from database import MLBPropsDatabase

def create_plus_ev_bets_table():
    """Create the +EV Bets table for high-value tiers only"""
    
    print("🎯 CREATING +EV BETS TABLE")
    print("=" * 50)
    
    with sqlite3.connect('mlb_props.db') as conn:
        # Create +EV Bets table (similar to best_odds but filtered for E, D, C tiers)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS plus_ev_bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scrape_date DATE NOT NULL,
                scrape_timestamp DATETIME NOT NULL,
                
                -- Game Info
                game_id TEXT,
                date TEXT NOT NULL,
                time TEXT NOT NULL,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                
                -- Player Info
                player_name TEXT NOT NULL,
                team TEXT NOT NULL,
                position TEXT,
                batting_order TEXT,
                
                -- Betting Info
                market TEXT NOT NULL,
                best_site TEXT NOT NULL,
                over_line TEXT,
                under_line TEXT,
                line_move TEXT,
                
                -- Projections & Analysis
                implied_projection REAL,
                batx_projection REAL,
                implied_vs_batx_diff TEXT,
                suggested_bet TEXT NOT NULL,
                
                -- Expected Value (Only A, B, C tiers - highest value)
                expected_value_tier TEXT NOT NULL CHECK (expected_value_tier IN ('A', 'B', 'C')),
                expected_value_raw TEXT,
                expected_value_description TEXT,
                
                -- Status Info
                status TEXT,
                official_lineup INTEGER,
                pitch_count_checked INTEGER,
                batx_pitch_count TEXT,
                
                -- Metadata
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                original_prop_id INTEGER NOT NULL,
                
                -- Foreign Keys
                FOREIGN KEY (original_prop_id) REFERENCES props (id),
                
                -- Constraints
                UNIQUE(scrape_date, player_name, team, market, best_site)
            )
        """)
        
        # Create indexes for performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_plus_ev_bets_scrape_date ON plus_ev_bets(scrape_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_plus_ev_bets_tier ON plus_ev_bets(expected_value_tier)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_plus_ev_bets_player ON plus_ev_bets(player_name, team)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_plus_ev_bets_original_id ON plus_ev_bets(original_prop_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_plus_ev_bets_market ON plus_ev_bets(market)")
        
        conn.commit()
        print("✅ Created plus_ev_bets table with indexes")

def populate_plus_ev_bets_from_best_odds(scrape_date='2025-06-26'):
    """Populate +EV Bets table from existing best_odds for A, B, C tiers"""
    
    print(f"\n📊 POPULATING +EV BETS FROM BEST ODDS ({scrape_date})")
    print("=" * 50)
    
    with sqlite3.connect('mlb_props.db') as conn:
        # Insert A, B, C tier bets from best_odds into plus_ev_bets
        cursor = conn.execute("""
            INSERT OR REPLACE INTO plus_ev_bets (
                scrape_date, scrape_timestamp, game_id, date, time, home_team, away_team,
                player_name, team, position, batting_order, market, best_site,
                over_line, under_line, line_move, implied_projection, batx_projection,
                implied_vs_batx_diff, suggested_bet, expected_value_tier,
                expected_value_raw, expected_value_description, status,
                official_lineup, pitch_count_checked, batx_pitch_count,
                created_at, original_prop_id
            )
            SELECT 
                scrape_date, scrape_timestamp, game_id, date, time, home_team, away_team,
                player_name, team, position, batting_order, market, best_site,
                over_line, under_line, line_move, implied_projection, batx_projection,
                implied_vs_batx_diff, suggested_bet, expected_value_tier,
                expected_value_raw, expected_value_description, status,
                official_lineup, pitch_count_checked, batx_pitch_count,
                created_at, original_prop_id
            FROM best_odds
            WHERE scrape_date = ?
              AND expected_value_tier IN ('A', 'B', 'C')
              AND suggested_bet IS NOT NULL
              AND suggested_bet != 'NEUTRAL'
        """, (scrape_date,))
        
        inserted_count = cursor.rowcount
        conn.commit()
        
        print(f"✅ Inserted {inserted_count} +EV bets from best_odds")
        
        # Show breakdown by tier
        cursor = conn.execute("""
            SELECT 
                expected_value_tier,
                COUNT(*) as count,
                COUNT(CASE WHEN suggested_bet = 'OVER' THEN 1 END) as over_bets,
                COUNT(CASE WHEN suggested_bet = 'UNDER' THEN 1 END) as under_bets
            FROM plus_ev_bets
            WHERE scrape_date = ?
            GROUP BY expected_value_tier
            ORDER BY expected_value_tier DESC
        """, (scrape_date,))
        
        print("\nBreakdown by EV Tier:")
        print("Tier | Total | OVER | UNDER")
        print("-" * 30)
        
        for row in cursor.fetchall():
            tier, total, over, under = row
            print(f"{tier:4} | {total:5} | {over:4} | {under:5}")

def calculate_plus_ev_roi(scrape_date='2025-06-26'):
    """Calculate ROI for +EV bets using tiered betting amounts"""
    
    print(f"\n💰 CALCULATING +EV BETS ROI ({scrape_date})")
    print("=" * 50)
    
    def parse_odds(odds_string):
        """Parse American odds string to numeric value"""
        if not odds_string or odds_string.strip() == '':
            return None
        match = re.search(r'\(([+-]\d+)\)', odds_string)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        return None

    def calculate_suggested_stake_tiered(odds_value):
        """Calculate suggested stake based on tiered odds ranges"""
        if odds_value is None:
            return None
        
        if odds_value > 0:
            if 100 <= odds_value <= 250:
                return 100.0
            elif 250 < odds_value <= 500:
                return 50.0
            elif 500 < odds_value <= 750:
                return 25.0
            elif odds_value > 750:
                return 15.0
            else:
                return 100.0
        elif odds_value < 0:
            return abs(odds_value)
        else:
            return None

    def calculate_profit_loss(odds_value, stake, bet_won):
        """Calculate profit or loss for a bet"""
        if odds_value is None or stake is None or bet_won is None:
            return None
        
        if bet_won:
            if odds_value > 0:
                return (odds_value / 100) * stake
            else:
                return (100 / abs(odds_value)) * stake
        else:
            return -stake

    db = MLBPropsDatabase()
    updated_count = 0
    
    with db.get_connection() as conn:
        # Get +EV bets with their bet results
        cursor = conn.execute("""
            SELECT 
                pev.id, pev.suggested_bet, pev.over_line, pev.under_line,
                br.over_result, br.under_result, br.id as bet_result_id
            FROM plus_ev_bets pev
            LEFT JOIN bet_results br ON pev.original_prop_id = br.prop_id
            WHERE pev.scrape_date = ?
            ORDER BY pev.id
        """, (scrape_date,))
        
        plus_ev_bets = cursor.fetchall()
        total_bets = len(plus_ev_bets)
        
        print(f"Found {total_bets} +EV bets to analyze")
        
        # Add ROI columns to plus_ev_bets table if they don't exist
        try:
            conn.execute("ALTER TABLE plus_ev_bets ADD COLUMN suggested_stake REAL")
            conn.execute("ALTER TABLE plus_ev_bets ADD COLUMN profit_loss REAL") 
            conn.execute("ALTER TABLE plus_ev_bets ADD COLUMN roi_percentage REAL")
            conn.execute("ALTER TABLE plus_ev_bets ADD COLUMN bet_result TEXT")
            print("✅ Added ROI columns to plus_ev_bets table")
        except sqlite3.OperationalError:
            print("ℹ️  ROI columns already exist in plus_ev_bets table")
        
        resolved_count = 0
        unresolved_count = 0
        
        for bet in plus_ev_bets:
            pev_id, suggested_bet, over_line, under_line, over_result, under_result, bet_result_id = bet
            
            if bet_result_id is None:
                # No bet result found
                unresolved_count += 1
                continue
            
            # Determine bet result and odds
            if suggested_bet == 'OVER':
                odds_string = over_line
                bet_won = (over_result == 'win')
                bet_result = 'WIN' if over_result == 'win' else 'LOSS' if over_result == 'loss' else 'PUSH'
            elif suggested_bet == 'UNDER':
                odds_string = under_line
                bet_won = (under_result == 'win')
                bet_result = 'WIN' if under_result == 'win' else 'LOSS' if under_result == 'loss' else 'PUSH'
            else:
                continue
            
            # Calculate ROI
            odds_value = parse_odds(odds_string)
            if odds_value is None:
                continue
            
            suggested_stake = calculate_suggested_stake_tiered(odds_value)
            if suggested_stake is None:
                continue
            
            profit_loss = calculate_profit_loss(odds_value, suggested_stake, bet_won)
            if profit_loss is None:
                continue
            
            roi_percentage = (profit_loss / suggested_stake) * 100
            
            # Update plus_ev_bets record
            conn.execute("""
                UPDATE plus_ev_bets 
                SET suggested_stake = ?, profit_loss = ?, roi_percentage = ?, bet_result = ?
                WHERE id = ?
            """, (suggested_stake, profit_loss, roi_percentage, bet_result, pev_id))
            
            resolved_count += 1
            updated_count += 1
        
        conn.commit()
        
        print(f"✅ Updated {updated_count} +EV bets with ROI data")
        print(f"   Resolved: {resolved_count}")
        print(f"   Unresolved: {unresolved_count}")

def analyze_plus_ev_performance(scrape_date='2025-06-26'):
    """Analyze +EV bets performance by tier"""
    
    print(f"\n📈 +EV BETS PERFORMANCE ANALYSIS ({scrape_date})")
    print("=" * 60)
    
    with sqlite3.connect('mlb_props.db') as conn:
        conn.row_factory = sqlite3.Row
        
        # Overall +EV performance
        cursor = conn.execute("""
            SELECT 
                COUNT(*) as total_bets,
                COUNT(CASE WHEN bet_result IS NOT NULL THEN 1 END) as resolved_bets,
                SUM(suggested_stake) as total_staked,
                SUM(profit_loss) as total_profit_loss,
                ROUND(SUM(profit_loss) / SUM(suggested_stake) * 100, 1) as overall_roi,
                COUNT(CASE WHEN bet_result = 'WIN' THEN 1 END) as wins,
                COUNT(CASE WHEN bet_result = 'LOSS' THEN 1 END) as losses,
                ROUND(COUNT(CASE WHEN bet_result = 'WIN' THEN 1 END) * 100.0 / 
                      COUNT(CASE WHEN bet_result IS NOT NULL THEN 1 END), 1) as win_rate
            FROM plus_ev_bets
            WHERE scrape_date = ?
              AND suggested_stake IS NOT NULL
        """, (scrape_date,))
        
        overall = cursor.fetchone()
        
        print("OVERALL +EV BETS PERFORMANCE:")
        print(f"Total Bets: {overall['total_bets']:,}")
        print(f"Resolved Bets: {overall['resolved_bets']:,}")
        print(f"Total Staked: ${overall['total_staked']:,.0f}")
        print(f"Total P/L: ${overall['total_profit_loss']:+,.0f}")
        print(f"Overall ROI: {overall['overall_roi']:+.1f}%")
        print(f"Win Rate: {overall['win_rate']:.1f}% ({overall['wins']}W / {overall['losses']}L)")
        
        # Performance by EV tier
        cursor = conn.execute("""
            SELECT 
                expected_value_tier,
                COUNT(*) as total_bets,
                SUM(suggested_stake) as total_staked,
                SUM(profit_loss) as total_profit_loss,
                ROUND(SUM(profit_loss) / SUM(suggested_stake) * 100, 1) as overall_roi,
                COUNT(CASE WHEN bet_result = 'WIN' THEN 1 END) as wins,
                COUNT(CASE WHEN bet_result = 'LOSS' THEN 1 END) as losses,
                ROUND(COUNT(CASE WHEN bet_result = 'WIN' THEN 1 END) * 100.0 / 
                      COUNT(CASE WHEN bet_result IS NOT NULL THEN 1 END), 1) as win_rate,
                MAX(profit_loss) as best_win,
                MIN(profit_loss) as worst_loss
            FROM plus_ev_bets
            WHERE scrape_date = ?
              AND suggested_stake IS NOT NULL
              AND bet_result IS NOT NULL
            GROUP BY expected_value_tier
            ORDER BY expected_value_tier DESC
        """, (scrape_date,))
        
        print(f"\nPERFORMANCE BY EV TIER:")
        print("Tier | Bets | Total Staked | Total P/L | ROI    | Win Rate | Best Win | Worst Loss")
        print("-" * 80)
        
        for row in cursor.fetchall():
            tier = row['expected_value_tier']
            bets = row['total_bets']
            staked = row['total_staked']
            pl = row['total_profit_loss']
            roi = row['overall_roi']
            win_rate = row['win_rate']
            best_win = row['best_win'] or 0
            worst_loss = row['worst_loss'] or 0
            
            print(f"{tier:4} | {bets:4} | ${staked:10,.0f} | ${pl:+8,.0f} | {roi:+5.1f}% | {win_rate:7.1f}% | ${best_win:+7.0f} | ${worst_loss:+7.0f}")

def create_plus_ev_csv_export(scrape_date='2025-06-26'):
    """Create CSV export for +EV bets"""
    
    print(f"\n📄 CREATING +EV BETS CSV EXPORT")
    print("=" * 50)
    
    with sqlite3.connect('mlb_props.db') as conn:
        cursor = conn.execute("""
            SELECT 
                pev.*,
                pnm.mlb_name as mapped_mlb_name,
                pnm.mapping_type
            FROM plus_ev_bets pev
            LEFT JOIN player_name_mapping pnm ON pev.player_name = pnm.betting_name
            WHERE pev.scrape_date = ?
            ORDER BY 
                pev.expected_value_tier DESC,
                pev.roi_percentage DESC,
                pev.player_name,
                pev.market
        """, (scrape_date,))
        
        import csv
        filename = f'/Users/doug/new_scraper/plus_ev_bets_{scrape_date}.csv'
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            columns = [desc[0] for desc in cursor.description]
            writer.writerow(columns)
            
            # Write data
            for row in cursor.fetchall():
                writer.writerow(row)
        
        print(f"✅ +EV Bets CSV exported to: {filename}")

def main():
    """Main execution"""
    scrape_date = '2025-06-26'
    
    # Create the +EV Bets table
    create_plus_ev_bets_table()
    
    # Populate from existing best_odds
    populate_plus_ev_bets_from_best_odds(scrape_date)
    
    # Calculate ROI for +EV bets
    calculate_plus_ev_roi(scrape_date)
    
    # Analyze performance
    analyze_plus_ev_performance(scrape_date)
    
    # Create CSV export
    create_plus_ev_csv_export(scrape_date)
    
    print(f"\n✅ +EV BETS SYSTEM COMPLETE!")
    print(f"Focus table created with {108} high-value bets (Tiers A, B, C only)")
    print(f"All ROI analysis and CSV export completed!")

if __name__ == "__main__":
    main()
