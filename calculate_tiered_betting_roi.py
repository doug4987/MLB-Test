#!/usr/bin/env python3
"""
Tiered Betting ROI Calculator
Enhanced version with tiered betting amounts based on odds ranges
"""
import sqlite3
import re
from database import MLBPropsDatabase

def parse_odds(odds_string):
    """Parse American odds string to numeric value"""
    if not odds_string or odds_string.strip() == '':
        return None
        
    # Look for American odds format in parentheses (+150, -110, etc.)
    # Format is typically "1.5 (+150)" or "2.5 (-110)"
    match = re.search(r'\(([+-]\d+)\)', odds_string)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    
    # Fallback: look for standalone odds format
    match = re.search(r'^([+-]\d+)$', odds_string.strip())
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    
    return None

def calculate_suggested_stake_tiered(odds_value):
    """Calculate suggested stake based on tiered odds ranges
    +100 to +250: bet $100
    +250 to +500: bet $50  
    +500 to +750: bet $25
    +750+: bet $15
    Negative odds: bet to win $100
    """
    if odds_value is None:
        return None
    
    if odds_value > 0:
        # Positive odds - tiered betting
        if 100 <= odds_value <= 250:
            return 100.0
        elif 250 < odds_value <= 500:
            return 50.0
        elif 500 < odds_value <= 750:
            return 25.0
        elif odds_value > 750:
            return 15.0
        else:
            # For odds below +100, bet $100
            return 100.0
    elif odds_value < 0:
        # Negative odds: bet enough to win $100
        return abs(odds_value)
    else:
        # Odds of 0 (shouldn't happen but handle it)
        return None

def calculate_profit_loss(odds_value, stake, bet_won):
    """Calculate profit or loss for a bet
    Args:
        odds_value: American odds (e.g., +150, -110)
        stake: Amount bet
        bet_won: True if bet won, False if lost
    """
    if odds_value is None or stake is None or bet_won is None:
        return None
    
    if bet_won:
        if odds_value > 0:
            # Positive odds: win the odds amount for every $100 bet
            return (odds_value / 100) * stake
        else:
            # Negative odds: win $100 for every |odds| bet
            return (100 / abs(odds_value)) * stake
    else:
        # Bet lost: lose the stake
        return -stake

def calculate_roi(profit_loss, stake):
    """Calculate ROI percentage"""
    if profit_loss is None or stake is None or stake == 0:
        return None
    
    return (profit_loss / stake) * 100

def add_roi_columns_to_bet_results():
    """Add ROI tracking columns to bet_results table"""
    
    print("🔧 ADDING TIERED ROI TRACKING COLUMNS")
    print("=" * 50)
    
    with sqlite3.connect('mlb_props.db') as conn:
        try:
            # Add suggested_stake column
            conn.execute("""
                ALTER TABLE bet_results 
                ADD COLUMN suggested_stake REAL
            """)
            print("✅ Added suggested_stake column")
        except sqlite3.OperationalError:
            print("ℹ️  suggested_stake column already exists")
        
        try:
            # Add profit_loss column
            conn.execute("""
                ALTER TABLE bet_results 
                ADD COLUMN profit_loss REAL
            """)
            print("✅ Added profit_loss column")
        except sqlite3.OperationalError:
            print("ℹ️  profit_loss column already exists")
        
        try:
            # Add roi_percentage column
            conn.execute("""
                ALTER TABLE bet_results 
                ADD COLUMN roi_percentage REAL
            """)
            print("✅ Added roi_percentage column")
        except sqlite3.OperationalError:
            print("ℹ️  roi_percentage column already exists")
        
        conn.commit()

def calculate_tiered_roi_for_all_bets():
    """Calculate ROI for all resolved bets using tiered betting amounts"""
    
    print("\n💰 CALCULATING TIERED ROI FOR ALL RESOLVED BETS")
    print("=" * 50)
    
    db = MLBPropsDatabase()
    updated_count = 0
    error_count = 0
    neutral_count = 0
    missing_odds_count = 0
    odds_parse_errors = 0
    
    with db.get_connection() as conn:
        # Get all resolved bets with their props data
        cursor = conn.execute("""
            SELECT br.id, br.prop_id, p.suggested_bet, p.over_line, p.under_line,
                   br.over_result, br.under_result, br.suggested_stake, br.profit_loss
            FROM bet_results br
            JOIN props p ON br.prop_id = p.id
            WHERE p.scrape_date = '2025-06-26'
            ORDER BY br.id
        """)
        
        bets_to_process = cursor.fetchall()
        total_bets = len(bets_to_process)
        
        print(f"Found {total_bets:,} resolved bets to process")
        print()
        
        for i, bet in enumerate(bets_to_process):
            try:
                bet_result_id = bet[0]
                suggested_bet = bet[2]
                over_line = bet[3]
                under_line = bet[4]
                over_result = bet[5]
                under_result = bet[6]
                existing_stake = bet[7]
                existing_profit = bet[8]
                
                # Skip if already calculated (and we're not recalculating)
                # For this run, let's recalculate to update with new tiered amounts
                
                # Determine which odds to use based on suggested bet
                if suggested_bet == 'OVER':
                    odds_string = over_line
                    bet_won = (over_result == 'win')
                elif suggested_bet == 'UNDER':
                    odds_string = under_line
                    bet_won = (under_result == 'win')
                elif suggested_bet == 'NEUTRAL':
                    # Skip neutral bets - no clear betting recommendation
                    neutral_count += 1
                    continue
                else:
                    # Unknown suggested bet type
                    error_count += 1
                    continue
                
                # Check if odds string exists
                if not odds_string or odds_string.strip() == '':
                    missing_odds_count += 1
                    continue
                
                # Parse odds and calculate stake
                odds_value = parse_odds(odds_string)
                if odds_value is None:
                    odds_parse_errors += 1
                    if i < 3:  # Show first few parsing errors
                        print(f"  ⚠️  Failed to parse odds: '{odds_string}'")
                    continue
                
                suggested_stake = calculate_suggested_stake_tiered(odds_value)
                if suggested_stake is None:
                    error_count += 1
                    continue
                
                # Calculate profit/loss
                profit_loss = calculate_profit_loss(odds_value, suggested_stake, bet_won)
                if profit_loss is None:
                    error_count += 1
                    continue
                
                # Calculate ROI
                roi = calculate_roi(profit_loss, suggested_stake)
                
                # Update the bet_results record
                cursor = conn.execute("""
                    UPDATE bet_results 
                    SET suggested_stake = ?, profit_loss = ?, roi_percentage = ?
                    WHERE id = ?
                """, (suggested_stake, profit_loss, roi, bet_result_id))
                
                updated_count += 1
                
                # Show progress for different stake tiers
                if i < 5 or (i + 1) % 1000 == 0:
                    result_emoji = "✅" if bet_won else "❌"
                    print(f"  {i+1:,}/{total_bets:,} {result_emoji} {suggested_bet} bet")
                    print(f"    Odds: {odds_value:+d} | Stake: ${suggested_stake:.0f} | P/L: ${profit_loss:+.0f} | ROI: {roi:+.1f}%")
                    if i >= 4 and (i + 1) % 1000 != 0:
                        print("    ... (showing every 1000th update)")
                    print()
                
            except Exception as e:
                error_count += 1
                if error_count <= 3:  # Show first few general errors
                    print(f"  Error processing bet {bet[0]}: {e}")
        
        # Commit all changes
        conn.commit()
    
    print(f"✅ TIERED ROI CALCULATION COMPLETE!")
    print(f"  Bets updated: {updated_count:,}")
    print(f"  Skipped - NEUTRAL bets: {neutral_count:,}")
    print(f"  Skipped - Missing odds: {missing_odds_count:,}")
    print(f"  Errors - Odds parsing: {odds_parse_errors:,}")
    print(f"  Errors - Other: {error_count:,}")
    print(f"  Total errors: {neutral_count + missing_odds_count + odds_parse_errors + error_count:,}")
    
    return updated_count, error_count

def analyze_tiered_betting_performance():
    """Analyze performance by betting tier"""
    
    print(f"\n📊 TIERED BETTING PERFORMANCE ANALYSIS")
    print("=" * 60)
    
    with sqlite3.connect('mlb_props.db') as conn:
        conn.row_factory = sqlite3.Row
        
        # Analyze by stake amount (which corresponds to odds tiers)
        cursor = conn.execute('''
            SELECT 
                br.suggested_stake,
                COUNT(*) as total_bets,
                SUM(br.suggested_stake) as total_staked,
                SUM(br.profit_loss) as total_profit_loss,
                ROUND(AVG(br.roi_percentage), 1) as avg_roi,
                ROUND(SUM(br.profit_loss) / SUM(br.suggested_stake) * 100, 1) as overall_roi,
                SUM(CASE WHEN br.profit_loss > 0 THEN 1 ELSE 0 END) as winning_bets,
                ROUND(SUM(CASE WHEN br.profit_loss > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as win_rate
            FROM bet_results br
            JOIN props p ON br.prop_id = p.id
            WHERE p.scrape_date = '2025-06-26'
              AND br.suggested_stake IS NOT NULL
              AND br.profit_loss IS NOT NULL
            GROUP BY br.suggested_stake
            ORDER BY br.suggested_stake DESC
        ''')
        
        print("PERFORMANCE BY BETTING TIER:")
        print("Stake   | Bets  | Total Staked | Total P/L | Overall ROI | Win Rate")
        print("-" * 70)
        
        stake_tier_names = {
            15: "$15 (odds > +750)",
            25: "$25 (odds +500 to +750)", 
            50: "$50 (odds +250 to +500)",
            100: "$100 (odds +100 to +250 or negative odds to win $100)"
        }
        
        for row in cursor.fetchall():
            stake = row['suggested_stake']
            total_bets = row['total_bets']
            total_staked = row['total_staked']
            total_profit_loss = row['total_profit_loss']
            overall_roi = row['overall_roi']
            win_rate = row['win_rate']
            
            tier_name = stake_tier_names.get(int(stake), f"${stake:.0f} (other)")
            
            print(f"${stake:3.0f}    | {total_bets:5,} | ${total_staked:10,.0f} | ${total_profit_loss:+8,.0f} | {overall_roi:+7.1f}% | {win_rate:6.1f}%")
        
        print(f"\nTier Descriptions:")
        for stake, desc in stake_tier_names.items():
            print(f"  ${stake}: {desc}")

def create_enhanced_csv_export():
    """Create CSV export with bet results and ROI data"""
    
    print(f"\n📄 CREATING ENHANCED CSV EXPORT")
    print("=" * 50)
    
    with sqlite3.connect('mlb_props.db') as conn:
        cursor = conn.execute('''
            SELECT 
                bo.id,
                bo.scrape_date,
                bo.date as game_date,
                bo.time as game_time,
                bo.home_team,
                bo.away_team,
                bo.player_name,
                bo.team,
                bo.market,
                bo.best_site,
                bo.over_line,
                bo.under_line,
                bo.suggested_bet,
                bo.expected_value_tier,
                bo.implied_projection,
                bo.batx_projection,
                bo.implied_vs_batx_diff,
                -- Bet resolution data
                br.actual_result,
                br.over_result,
                br.under_result,
                -- Bet result tied to suggested bet
                CASE 
                    WHEN bo.suggested_bet = 'OVER' AND br.over_result = 'win' THEN 'WIN'
                    WHEN bo.suggested_bet = 'OVER' AND br.over_result = 'loss' THEN 'LOSS'
                    WHEN bo.suggested_bet = 'OVER' AND br.over_result = 'push' THEN 'PUSH'
                    WHEN bo.suggested_bet = 'UNDER' AND br.under_result = 'win' THEN 'WIN'
                    WHEN bo.suggested_bet = 'UNDER' AND br.under_result = 'loss' THEN 'LOSS'
                    WHEN bo.suggested_bet = 'UNDER' AND br.under_result = 'push' THEN 'PUSH'
                    WHEN bo.suggested_bet = 'NEUTRAL' OR bo.suggested_bet IS NULL OR bo.suggested_bet = '' THEN 'NO_BET'
                    WHEN br.id IS NULL THEN 'UNRESOLVED'
                    ELSE 'UNKNOWN'
                END as bet_result,
                -- ROI data
                br.suggested_stake,
                br.profit_loss,
                br.roi_percentage,
                br.resolved_at,
                -- Name mapping
                pnm.mlb_name as mapped_mlb_name
            FROM best_odds bo
            LEFT JOIN bet_results br ON bo.original_prop_id = br.prop_id
            LEFT JOIN player_name_mapping pnm ON bo.player_name = pnm.betting_name
            WHERE bo.scrape_date = '2025-06-26'
            ORDER BY 
                bo.expected_value_tier DESC,
                br.roi_percentage DESC,
                bo.player_name,
                bo.market
        ''')
        
        # Write to CSV
        import csv
        
        filename = '/Users/doug/new_scraper/best_odds_tiered_roi_2025-06-26.csv'
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            columns = [desc[0] for desc in cursor.description]
            writer.writerow(columns)
            
            # Write data
            for row in cursor.fetchall():
                writer.writerow(row)
        
        print(f"✅ Enhanced CSV exported to: {filename}")

def main():
    """Main execution"""
    # Add ROI columns to database
    add_roi_columns_to_bet_results()
    
    # Calculate tiered ROI for all bets
    updated_count, error_count = calculate_tiered_roi_for_all_bets()
    
    # Analyze tiered betting performance
    analyze_tiered_betting_performance()
    
    # Create enhanced CSV export
    create_enhanced_csv_export()
    
    print(f"\n✅ TIERED BETTING ROI ANALYSIS COMPLETE!")
    print(f"Updated {updated_count:,} bets with tiered betting amounts.")
    print(f"Enhanced CSV export includes bet results and ROI data!")

if __name__ == "__main__":
    main()
