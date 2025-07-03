import sqlite3
from datetime import datetime

def create_best_odds_table():
    """Create a table to store only the best odds for each unique bet"""
    conn = sqlite3.connect('mlb_props.db')
    cursor = conn.cursor()
    
    # Create the best_odds table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS best_odds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scrape_date DATE,
        scrape_timestamp DATETIME,
        game_id TEXT,
        date TEXT,
        time TEXT,
        home_team TEXT,
        away_team TEXT,
        player_name TEXT,
        team TEXT,
        position TEXT,
        batting_order TEXT,
        market TEXT,
        best_site TEXT,
        over_line TEXT,
        under_line TEXT,
        line_move TEXT,
        implied_projection REAL,
        batx_projection REAL,
        implied_vs_batx_diff TEXT,
        suggested_bet TEXT,
        expected_value_raw TEXT,
        expected_value_tier TEXT,
        expected_value_description TEXT,
        status TEXT,
        official_lineup INTEGER,
        pitch_count_checked INTEGER,
        batx_pitch_count TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        original_prop_id INTEGER,
        FOREIGN KEY (original_prop_id) REFERENCES props(id)
    )
    ''')
    
    # Create indexes for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_best_odds_player_market ON best_odds(player_name, market)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_best_odds_scrape_date ON best_odds(scrape_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_best_odds_tier ON best_odds(expected_value_tier)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_best_odds_site ON best_odds(best_site)')
    
    conn.commit()
    conn.close()
    print("✅ Created best_odds table with indexes")

def get_numeric_value_from_tier(tier):
    """Convert tier to numeric value for comparison (higher is better)
    
    CORRECTED TIER MAPPING:
    A Tier (Highest): plus_e_5.png
    B Tier: plus_d_4.png  
    C Tier: plus_c_3.png
    D Tier: plus_b_2.png
    E Tier (Lowest): plus_a_1.png
    """
    tier_values = {
        'A': 5,  # Highest
        'B': 4,  # Second best
        'C': 3,  # Third
        'D': 2,  # Fourth
        'E': 1,  # Lowest
        '': 0,   # No tier
        None: 0  # No tier
    }
    return tier_values.get(tier, 0)

def populate_best_odds_table(scrape_date=None):
    """Populate the best_odds table with the best odds for each unique bet"""
    conn = sqlite3.connect('mlb_props.db')
    cursor = conn.cursor()
    
    # Use today's date if no date specified
    if scrape_date is None:
        scrape_date = datetime.now().strftime('%Y-%m-%d')
    
    # Clear existing data for this date
    cursor.execute('DELETE FROM best_odds WHERE scrape_date = ?', (scrape_date,))
    
    # Get all unique player+market combinations for the date
    cursor.execute('''
    SELECT DISTINCT player_name, market 
    FROM props 
    WHERE DATE(scrape_date) = ?
    ''', (scrape_date,))
    
    unique_bets = cursor.fetchall()
    inserted_count = 0
    
    print(f"📊 Processing {len(unique_bets)} unique player+market combinations...")
    
    for player_name, market in unique_bets:
        # Get all props for this player+market combination
        cursor.execute('''
        SELECT * FROM props 
        WHERE DATE(scrape_date) = ? 
        AND player_name = ? 
        AND market = ?
        ORDER BY expected_value_tier ASC, id ASC
        ''', (scrape_date, player_name, market))
        
        all_props = cursor.fetchall()
        
        if not all_props:
            continue
            
        # Find the best prop based on expected value tier
        best_prop = None
        best_tier_value = -1
        
        for prop in all_props:
            # Get the column index for expected_value_tier (should be index 23)
            tier = prop[23]  # expected_value_tier column
            tier_value = get_numeric_value_from_tier(tier)
            
            if tier_value > best_tier_value:
                best_tier_value = tier_value
                best_prop = prop
        
        # If no tier found, use the first one
        if best_prop is None:
            best_prop = all_props[0]
        
        # Insert the best prop into best_odds table
        cursor.execute('''
        INSERT INTO best_odds (
            scrape_date, scrape_timestamp, game_id, date, time, home_team, away_team,
            player_name, team, position, batting_order, market, best_site,
            over_line, under_line, line_move, implied_projection, batx_projection,
            implied_vs_batx_diff, suggested_bet, expected_value_raw, expected_value_tier,
            expected_value_description, status, official_lineup, pitch_count_checked,
            batx_pitch_count, original_prop_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            best_prop[1],   # scrape_date
            best_prop[2],   # scrape_timestamp
            best_prop[4],   # game_id
            best_prop[5],   # date
            best_prop[6],   # time
            best_prop[7],   # home_team
            best_prop[8],   # away_team
            best_prop[9],   # player_name
            best_prop[10],  # team
            best_prop[11],  # position
            best_prop[12],  # batting_order
            best_prop[14],  # market
            best_prop[13],  # site (renamed to best_site)
            best_prop[15],  # over_line
            best_prop[16],  # under_line
            best_prop[17],  # line_move
            best_prop[18],  # implied_projection
            best_prop[19],  # batx_projection
            best_prop[20],  # implied_vs_batx_diff
            best_prop[21],  # suggested_bet
            best_prop[22],  # expected_value_raw
            best_prop[23],  # expected_value_tier
            best_prop[24],  # expected_value_description
            best_prop[25],  # status
            best_prop[26],  # official_lineup
            best_prop[27],  # pitch_count_checked
            best_prop[28],  # batx_pitch_count
            best_prop[0]    # original_prop_id (original id)
        ))
        
        inserted_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"✅ Inserted {inserted_count} best odds entries for {scrape_date}")
    return inserted_count

def analyze_best_odds(scrape_date=None):
    """Analyze the best odds data"""
    conn = sqlite3.connect('mlb_props.db')
    cursor = conn.cursor()
    
    if scrape_date is None:
        scrape_date = datetime.now().strftime('%Y-%m-%d')
    
    # Get summary statistics
    cursor.execute('''
    SELECT 
        COUNT(*) as total_best_odds,
        COUNT(DISTINCT player_name) as unique_players,
        COUNT(DISTINCT market) as unique_markets,
        COUNT(DISTINCT best_site) as unique_sites
    FROM best_odds 
    WHERE scrape_date = ?
    ''', (scrape_date,))
    
    summary = cursor.fetchone()
    
    # Get tier breakdown
    cursor.execute('''
    SELECT 
        expected_value_tier,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM best_odds WHERE scrape_date = ?), 2) as percentage
    FROM best_odds 
    WHERE scrape_date = ?
    GROUP BY expected_value_tier
    ORDER BY expected_value_tier
    ''', (scrape_date, scrape_date))
    
    tier_breakdown = cursor.fetchall()
    
    # Get site distribution
    cursor.execute('''
    SELECT 
        best_site,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM best_odds WHERE scrape_date = ?), 2) as percentage
    FROM best_odds 
    WHERE scrape_date = ?
    GROUP BY best_site
    ORDER BY count DESC
    ''', (scrape_date, scrape_date))
    
    site_distribution = cursor.fetchall()
    
    conn.close()
    
    # Print analysis
    print(f"\n📈 BEST ODDS ANALYSIS for {scrape_date}")
    print("=" * 50)
    print(f"Total Best Odds: {summary[0]:,}")
    print(f"Unique Players: {summary[1]:,}")
    print(f"Unique Markets: {summary[2]:,}")
    print(f"Unique Sites: {summary[3]:,}")
    
    print(f"\n🎯 EXPECTED VALUE TIER BREAKDOWN:")
    for tier, count, percentage in tier_breakdown:
        tier_name = tier if tier else "No Tier"
        print(f"  Tier {tier_name}: {count:,} ({percentage}%)")
    
    print(f"\n🏆 SPORTSBOOK DISTRIBUTION (Best Odds):")
    for site, count, percentage in site_distribution:
        print(f"  {site}: {count:,} ({percentage}%)")

if __name__ == "__main__":
    # Create the table
    create_best_odds_table()
    
    # Populate with today's data
    scrape_date = datetime.now().strftime('%Y-%m-%d')
    populate_best_odds_table(scrape_date)
    
    # Analyze the results
    analyze_best_odds(scrape_date)
