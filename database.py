#!/usr/bin/env python3
"""
MLB Props Database System
Handles daily data storage and bet result tracking
"""
import sqlite3
import json
import logging
import re
from datetime import datetime, date
from typing import List, Dict, Optional, Any
import os
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class MLBPropsDatabase:
    """Database manager for MLB Props data"""
    
    def __init__(self, db_path: str = "mlb_props.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database with all required tables"""
        with self.get_connection() as conn:
            # Create tables
            self._create_props_table(conn)
            self._create_games_table(conn)
            self._create_players_table(conn)
            self._create_scrape_sessions_table(conn)
            self._create_bet_results_table(conn)
            self._create_box_scores_table(conn)
            self._create_player_name_mapping_table(conn)
            self._create_indexes(conn)
            logger.info("Database initialized successfully")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
    
    def _create_props_table(self, conn):
        """Create main props data table"""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS props (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scrape_date DATE NOT NULL,
                scrape_timestamp DATETIME NOT NULL,
                scrape_session_id TEXT NOT NULL,
                
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
                batting_order INTEGER,
                
                -- Betting Info
                site TEXT NOT NULL,
                market TEXT NOT NULL,
                over_line TEXT,
                under_line TEXT,
                line_move TEXT,
                
                -- Projections & Analysis
                implied_projection REAL,
                batx_projection REAL,
                implied_vs_batx_diff TEXT,
                suggested_bet TEXT,
                
                -- Expected Value
                expected_value_raw TEXT,  -- Raw JSON from images
                expected_value_tier TEXT, -- Parsed tier (A, B, C, D)
                expected_value_description TEXT,
                
                -- Status Info
                status TEXT,
                official_lineup BOOLEAN,
                pitch_count_checked BOOLEAN,
                batx_pitch_count INTEGER,
                
                -- Metadata
                page_number INTEGER,
                row_number INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                
                -- Constraints
                UNIQUE(scrape_date, player_name, team, market, site)
            )
        """)
    
    def _create_games_table(self, conn):
        """Create games reference table"""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT UNIQUE NOT NULL,
                game_date DATE NOT NULL,
                game_time TEXT NOT NULL,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                
                -- Game Status
                status TEXT DEFAULT 'scheduled', -- scheduled, in_progress, completed, postponed
                
                -- Final Scores
                home_score INTEGER,
                away_score INTEGER,
                innings_played INTEGER,
                
                -- Game Stats (for bet resolution)
                game_stats_json TEXT, -- JSON blob for detailed stats
                
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def _create_players_table(self, conn):
        """Create players reference table"""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name TEXT NOT NULL,
                team TEXT NOT NULL,
                position TEXT,
                
                -- Player Stats Cache
                season_stats_json TEXT,
                last_updated DATETIME,
                
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(player_name, team)
            )
        """)
    
    def _create_scrape_sessions_table(self, conn):
        """Create scrape sessions tracking table"""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scrape_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT UNIQUE NOT NULL,
                scrape_date DATE NOT NULL,
                start_time DATETIME NOT NULL,
                end_time DATETIME,
                
                status TEXT NOT NULL, -- running, completed, failed
                records_scraped INTEGER DEFAULT 0,
                pages_processed INTEGER DEFAULT 0,
                
                error_message TEXT,
                notes TEXT,
                
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def _create_bet_results_table(self, conn):
        """Create bet results tracking table"""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bet_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prop_id INTEGER NOT NULL,
                
                -- Bet Details
                market TEXT NOT NULL,
                player_name TEXT NOT NULL,
                team TEXT NOT NULL,
                game_id TEXT NOT NULL,
                
                -- Bet Lines
                over_line TEXT,
                under_line TEXT,
                suggested_bet TEXT,
                expected_value_tier TEXT,
                
                -- Actual Results
                actual_result REAL, -- The actual statistical outcome
                over_result TEXT,   -- 'win', 'loss', 'push'
                under_result TEXT,  -- 'win', 'loss', 'push'
                
                -- Result Source
                result_source TEXT, -- 'espn', 'mlb_api', 'manual', etc.
                result_confidence REAL, -- 0.0 to 1.0
                
                -- Metadata
                resolved_at DATETIME,
                notes TEXT,
                
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (prop_id) REFERENCES props (id),
                UNIQUE(prop_id)
            )
        """)
    
    def _create_box_scores_table(self, conn):
        """Create box scores table for daily player statistics"""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS box_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Game and Date Info
                game_id TEXT NOT NULL,
                game_date DATE NOT NULL,
                player_name TEXT NOT NULL,
                team TEXT NOT NULL,
                
                -- Game Status
                game_status TEXT NOT NULL, -- 'completed', 'in_progress', 'postponed'
                game_completed BOOLEAN DEFAULT FALSE,
                
                -- Batting Statistics
                at_bats INTEGER DEFAULT 0,
                hits INTEGER DEFAULT 0,
                runs INTEGER DEFAULT 0,
                rbi INTEGER DEFAULT 0,
                home_runs INTEGER DEFAULT 0,
                doubles INTEGER DEFAULT 0,
                triples INTEGER DEFAULT 0,
                singles INTEGER DEFAULT 0,
                walks INTEGER DEFAULT 0,
                strikeouts INTEGER DEFAULT 0,
                stolen_bases INTEGER DEFAULT 0,
                caught_stealing INTEGER DEFAULT 0,
                total_bases INTEGER DEFAULT 0,
                
                -- Pitching Statistics
                innings_pitched REAL DEFAULT 0.0,
                pitching_outs INTEGER DEFAULT 0,
                hits_allowed INTEGER DEFAULT 0,
                earned_runs INTEGER DEFAULT 0,
                walks_allowed INTEGER DEFAULT 0,
                strikeouts_pitched INTEGER DEFAULT 0,
                home_runs_allowed INTEGER DEFAULT 0,
                
                -- Fielding Statistics
                fielding_assists INTEGER DEFAULT 0,
                fielding_putouts INTEGER DEFAULT 0,
                fielding_errors INTEGER DEFAULT 0,
                
                -- Additional Metrics
                position TEXT,
                batting_order INTEGER,
                
                -- Data Source
                data_source TEXT NOT NULL, -- 'mlb_api', 'espn', 'manual'
                data_confidence REAL DEFAULT 1.0,
                
                -- Metadata
                collected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                
                -- Constraints
                UNIQUE(game_id, player_name, team, game_date)
            )
        """)

    def _create_player_name_mapping_table(self, conn):
        """Create player name mapping table"""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS player_name_mapping (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                betting_name TEXT NOT NULL,
                mlb_name TEXT NOT NULL,
                team TEXT,
                mapping_type TEXT DEFAULT 'manual',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(betting_name, mlb_name, team)
            )
        """)
    
    def _create_indexes(self, conn):
        """Create database indexes for performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_props_scrape_date ON props(scrape_date)",
            "CREATE INDEX IF NOT EXISTS idx_props_player_team ON props(player_name, team)",
            "CREATE INDEX IF NOT EXISTS idx_props_market ON props(market)",
            "CREATE INDEX IF NOT EXISTS idx_props_ev_tier ON props(expected_value_tier)",
            "CREATE INDEX IF NOT EXISTS idx_props_game_date ON props(date)",
            "CREATE INDEX IF NOT EXISTS idx_games_date ON games(game_date)",
            "CREATE INDEX IF NOT EXISTS idx_games_teams ON games(home_team, away_team)",
            "CREATE INDEX IF NOT EXISTS idx_bet_results_prop ON bet_results(prop_id)",
            "CREATE INDEX IF NOT EXISTS idx_bet_results_resolved ON bet_results(resolved_at)",
        ]
        
        for index_sql in indexes:
            conn.execute(index_sql)
    
    def start_scrape_session(self, scrape_date: date) -> str:
        """Start a new scrape session"""
        session_id = f"scrape_{scrape_date.isoformat()}_{datetime.now().strftime('%H%M%S')}"
        
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO scrape_sessions 
                (session_id, scrape_date, start_time, status)
                VALUES (?, ?, ?, 'running')
            """, (session_id, scrape_date, datetime.now()))
        
        logger.info(f"Started scrape session: {session_id}")
        return session_id
    
    def end_scrape_session(self, session_id: str, status: str, 
                          records_scraped: int = 0, pages_processed: int = 0,
                          error_message: str = None):
        """End a scrape session"""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE scrape_sessions 
                SET end_time = ?, status = ?, records_scraped = ?, 
                    pages_processed = ?, error_message = ?
                WHERE session_id = ?
            """, (datetime.now(), status, records_scraped, pages_processed, 
                  error_message, session_id))
        
        logger.info(f"Ended scrape session {session_id}: {status}")
    
    def insert_props_data(self, props_data: List[Dict], session_id: str) -> int:
        """Insert props data into database"""
        if not props_data:
            return 0
        
        inserted_count = 0
        
        with self.get_connection() as conn:
            for prop in props_data:
                try:
                    # Parse game info
                    game_info = self._parse_game_info(prop.get('GAME', ''))
                    
                    # Parse expected value
                    ev_info = self._parse_expected_value_data(prop.get('EXPECTED VALUE', ''))
                    
                    # Parse numeric fields
                    batting_order = self._safe_int(prop.get('BATTING\nORDER', ''))
                    implied_proj = self._safe_float(prop.get('IMPLIED\nPROJECTION', ''))
                    batx_proj = self._safe_float(prop.get('THE BAT X\nPROJECTION', ''))
                    pitch_count = self._safe_int(prop.get('THE BAT X\nPITCH COUNT', ''))
                    
                    # Insert prop record
                    conn.execute("""
                        INSERT OR REPLACE INTO props (
                            scrape_date, scrape_timestamp, scrape_session_id,
                            game_id, date, time, home_team, away_team,
                            player_name, team, position, batting_order,
                            site, market, over_line, under_line, line_move,
                            implied_projection, batx_projection, implied_vs_batx_diff, suggested_bet,
                            expected_value_raw, expected_value_tier, expected_value_description,
                            status, official_lineup, pitch_count_checked, batx_pitch_count,
                            page_number, row_number
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        prop.get('scrape_timestamp', '')[:10],  # Extract date
                        prop.get('scrape_timestamp', ''),
                        session_id,
                        game_info['game_id'],
                        prop.get('DATE', ''),
                        prop.get('TIME', ''),
                        game_info['home_team'],
                        game_info['away_team'],
                        prop.get('PLAYER', ''),
                        prop.get('TM', ''),
                        prop.get('POSITION', ''),
                        batting_order,
                        prop.get('SITE', ''),
                        prop.get('MARKET', ''),
                        prop.get('OVER', ''),
                        prop.get('UNDER', ''),
                        prop.get('LINE MOVE', ''),
                        implied_proj,
                        batx_proj,
                        prop.get('IMPLIED VS BATX\n% DIFFERENCE', ''),
                        prop.get('SUGGESTED\nBET', ''),
                        prop.get('EXPECTED VALUE', ''),
                        ev_info['tier'],
                        ev_info['description'],
                        prop.get('STATUS', ''),
                        prop.get('OFFICAL\nLINEUP', '') != '',
                        prop.get('PITCH COUNT\nCHECKED', '') != '',
                        pitch_count,
                        prop.get('page_number', 0),
                        prop.get('row_number', 0)
                    ))
                    
                    # Insert game if not exists
                    if game_info['game_id']:
                        self._insert_game_if_not_exists(conn, game_info, prop.get('DATE', ''), prop.get('TIME', ''))
                    
                    # Insert player if not exists
                    self._insert_player_if_not_exists(conn, prop.get('PLAYER', ''), prop.get('TM', ''), prop.get('POSITION', ''))
                    
                    inserted_count += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting prop: {e}")
                    logger.debug(f"Prop data: {prop}")
                    continue
        
        logger.info(f"Inserted {inserted_count} props records")
        return inserted_count
    
    def _parse_game_info(self, game_str: str) -> Dict[str, str]:
        """Parse game string like 'LAD@COL' into components"""
        if '@' in game_str:
            parts = game_str.split('@')
            away_team = parts[0].strip()
            home_team = parts[1].strip()
            game_id = f"{away_team}@{home_team}"
        else:
            away_team = ''
            home_team = ''
            game_id = game_str
        
        return {
            'game_id': game_id,
            'away_team': away_team,
            'home_team': home_team
        }
    
    def _parse_expected_value_data(self, ev_raw: str) -> Dict[str, str]:
        """Parse expected value raw data into tier and description
        
        CORRECTED TIER MAPPING:
        A Tier (Highest): plus_e_5.png
        B Tier: plus_d_4.png  
        C Tier: plus_c_3.png
        D Tier: plus_b_2.png
        E Tier (Lowest): plus_a_1.png
        F Tier: No image (empty)
        """
        if not ev_raw:
            return {'tier': 'F', 'description': 'No Expected Value'}
        
        try:
            ev_data = json.loads(ev_raw)
            images = ev_data.get('images', [])
            
            if not images:
                return {'tier': 'F', 'description': 'No Expected Value'}
            
            src = images[0].get('src', '')
            
            # CORRECTED: Fixed tier mapping based on actual scraped data
            if 'plus_e' in src:  # plus_e_5.png
                return {'tier': 'A', 'description': 'Excellent Expected Value'}
            elif 'plus_d' in src:  # plus_d_4.png
                return {'tier': 'B', 'description': 'Very Good Expected Value'}
            elif 'plus_c' in src:  # plus_c_3.png
                return {'tier': 'C', 'description': 'Good Expected Value'}
            elif 'plus_b' in src:  # plus_b_2.png
                return {'tier': 'D', 'description': 'Fair Expected Value'}
            elif 'plus_a' in src:  # plus_a_1.png
                return {'tier': 'E', 'description': 'Lower Expected Value'}
            
            return {'tier': 'F', 'description': 'Unknown Expected Value'}
            
        except Exception:
            return {'tier': 'F', 'description': 'Parse Error'}
    
    def _safe_int(self, value: str) -> Optional[int]:
        """Safely convert string to int"""
        try:
            return int(value) if value and value.strip() else None
        except ValueError:
            return None
    
    def _safe_float(self, value: str) -> Optional[float]:
        """Safely convert string to float"""
        try:
            return float(value) if value and value.strip() else None
        except ValueError:
            return None
    
    def _insert_game_if_not_exists(self, conn, game_info: Dict, date_str: str, time_str: str):
        """Insert game record if it doesn't exist"""
        conn.execute("""
            INSERT OR IGNORE INTO games (game_id, game_date, game_time, home_team, away_team)
            VALUES (?, ?, ?, ?, ?)
        """, (
            game_info['game_id'],
            date_str,
            time_str,
            game_info['home_team'],
            game_info['away_team']
        ))
    
    def _insert_player_if_not_exists(self, conn, player_name: str, team: str, position: str):
        """Insert player record if it doesn't exist"""
        if player_name and team:
            conn.execute("""
                INSERT OR IGNORE INTO players (player_name, team, position)
                VALUES (?, ?, ?)
            """, (player_name, team, position))
    
    def get_daily_summary(self, scrape_date: date) -> Dict[str, Any]:
        """Get summary of daily scrape data"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_props,
                    COUNT(DISTINCT player_name) as unique_players,
                    COUNT(DISTINCT game_id) as unique_games,
                    COUNT(DISTINCT market) as unique_markets,
                    COUNT(CASE WHEN expected_value_tier = 'A' THEN 1 END) as tier_a_count,
                    COUNT(CASE WHEN expected_value_tier = 'B' THEN 1 END) as tier_b_count,
                    COUNT(CASE WHEN expected_value_tier = 'C' THEN 1 END) as tier_c_count,
                    COUNT(CASE WHEN expected_value_tier = 'D' THEN 1 END) as tier_d_count
                FROM props 
                WHERE scrape_date = ?
            """, (scrape_date,))
            
            result = cursor.fetchone()
            return dict(result) if result else {}
    
    def get_props_by_tier(self, scrape_date: date, tier: str) -> List[Dict]:
        """Get props filtered by expected value tier"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM props 
                WHERE scrape_date = ? AND expected_value_tier = ?
                ORDER BY player_name, market
            """, (scrape_date, tier))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_unresolved_bets(self, before_date: date = None) -> List[Dict]:
        """Get bets that need result resolution"""
        query = """
            SELECT p.*, g.status as game_status
            FROM props p
            LEFT JOIN games g ON p.game_id = g.game_id
            LEFT JOIN bet_results br ON p.id = br.prop_id
            WHERE br.id IS NULL
        """
        params = []
        
        if before_date:
            query += " AND p.scrape_date <= ?"
            params.append(before_date)
        
        query += " ORDER BY p.scrape_date DESC, p.player_name"
        
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def insert_bet_result(self, prop_id: int, actual_result: float, 
                         result_source: str = 'manual',
                         result_confidence: float = 1.0,
                         notes: str = None):
        """Insert bet result and calculate win/loss"""
        with self.get_connection() as conn:
            # Get prop details
            cursor = conn.execute("""
                SELECT over_line, under_line, suggested_bet, expected_value_tier,
                       market, player_name, team, game_id
                FROM props WHERE id = ?
            """, (prop_id,))
            
            prop = cursor.fetchone()
            if not prop:
                raise ValueError(f"Prop {prop_id} not found")
            
            # Calculate over/under results
            over_result, under_result = self._calculate_bet_results(
                actual_result, prop['over_line'], prop['under_line']
            )
            
            # Insert result
            conn.execute("""
                INSERT OR REPLACE INTO bet_results (
                    prop_id, market, player_name, team, game_id,
                    over_line, under_line, suggested_bet, expected_value_tier,
                    actual_result, over_result, under_result,
                    result_source, result_confidence, resolved_at, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                prop_id, prop['market'], prop['player_name'], prop['team'], prop['game_id'],
                prop['over_line'], prop['under_line'], prop['suggested_bet'], prop['expected_value_tier'],
                actual_result, over_result, under_result,
                result_source, result_confidence, datetime.now(), notes
            ))
            
            logger.info(f"Inserted bet result for prop {prop_id}: {actual_result} ({over_result}/{under_result})")
    
    def _calculate_bet_results(self, actual_result: float, over_line: str, under_line: str) -> tuple:
        """Calculate whether over/under bets won, lost, or pushed"""
        try:
            def parse_line(value: str) -> Optional[float]:
                if not value:
                    return None
                match = re.search(r"-?\d+(?:\.\d+)?", value)
                return float(match.group()) if match else None

            over_num = parse_line(over_line)
            under_num = parse_line(under_line)

            line_value = over_num if over_num is not None else under_num

            if line_value is None:
                return 'unknown', 'unknown'

            if actual_result > line_value:
                return 'win', 'loss'
            elif actual_result < line_value:
                return 'loss', 'win'
            else:
                return 'push', 'push'

        except Exception as e:
            logger.error(f"Error calculating bet results: {e}")
            return 'error', 'error'
    
    def get_performance_analysis(self, start_date: date = None, end_date: date = None) -> Dict[str, Any]:
        """Get betting performance analysis"""
        query = """
            SELECT 
                br.expected_value_tier,
                br.suggested_bet,
                COUNT(*) as total_bets,
                SUM(CASE WHEN 
                    (br.suggested_bet = 'OVER' AND br.over_result = 'win') OR
                    (br.suggested_bet = 'UNDER' AND br.under_result = 'win')
                    THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN 
                    (br.suggested_bet = 'OVER' AND br.over_result = 'loss') OR
                    (br.suggested_bet = 'UNDER' AND br.under_result = 'loss')
                    THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN 
                    (br.suggested_bet = 'OVER' AND br.over_result = 'push') OR
                    (br.suggested_bet = 'UNDER' AND br.under_result = 'push')
                    THEN 1 ELSE 0 END) as pushes
            FROM bet_results br
            JOIN props p ON br.prop_id = p.id
        """
        
        params = []
        if start_date:
            query += " WHERE p.scrape_date >= ?"
            params.append(start_date)
        if end_date:
            if start_date:
                query += " AND p.scrape_date <= ?"
            else:
                query += " WHERE p.scrape_date <= ?"
            params.append(end_date)
        
        query += " GROUP BY br.expected_value_tier, br.suggested_bet ORDER BY br.expected_value_tier"
        
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_daily_bet_results_summary(self, target_date: date) -> Dict[str, Any]:
        """Return summary of bet results for a specific date including P/L"""

        def parse_odds(odds_string: str) -> Optional[int]:
            if not odds_string or odds_string.strip() == "":
                return None
            match = re.search(r"\(([+-]\d+)\)", odds_string)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    return None
            match = re.search(r"^([+-]\d+)$", odds_string.strip())
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    return None
            return None

        def calculate_suggested_stake(odds_value: int) -> Optional[float]:
            if odds_value is None:
                return None
            if odds_value > 0:
                if 100 <= odds_value <= 250:
                    return 100.0
                if 250 < odds_value <= 500:
                    return 50.0
                if 500 < odds_value <= 750:
                    return 25.0
                if odds_value > 750:
                    return 15.0
                return 100.0
            if odds_value < 0:
                return float(abs(odds_value))
            return None

        def calculate_profit_loss(odds_value: int, stake: float, bet_won: bool) -> Optional[float]:
            if odds_value is None or stake is None or bet_won is None:
                return None
            if bet_won:
                if odds_value > 0:
                    return (odds_value / 100) * stake
                return (100 / abs(odds_value)) * stake
            return -stake

        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT
                    br.expected_value_tier,
                    br.suggested_bet,
                    br.over_line,
                    br.under_line,
                    br.over_result,
                    br.under_result
                FROM bet_results br
                JOIN props p ON br.prop_id = p.id
                WHERE p.scrape_date = ?
            """,
                (target_date,),
            )

            rows = [dict(row) for row in cursor.fetchall()]

        total_bets = 0
        wins = 0
        losses = 0
        pushes = 0
        total_staked = 0.0
        total_profit_loss = 0.0
        tier_data: Dict[str, Dict[str, Any]] = {}

        for row in rows:
            suggested_bet = row.get("suggested_bet")
            if suggested_bet not in ("OVER", "UNDER"):
                continue
            odds_str = row["over_line"] if suggested_bet == "OVER" else row["under_line"]
            result = row["over_result"] if suggested_bet == "OVER" else row["under_result"]
            odds_value = parse_odds(odds_str)
            stake = calculate_suggested_stake(odds_value)
            if stake is None:
                continue
            bet_push = result == "push"
            bet_won = result == "win"
            profit_loss = 0.0 if bet_push else calculate_profit_loss(odds_value, stake, bet_won)

            tier = row.get("expected_value_tier", "?")
            data = tier_data.setdefault(
                tier,
                {
                    "tier": tier,
                    "total_bets": 0,
                    "wins": 0,
                    "losses": 0,
                    "pushes": 0,
                    "total_staked": 0.0,
                    "total_profit_loss": 0.0,
                },
            )

            data["total_bets"] += 1
            if bet_won:
                wins += 1
                data["wins"] += 1
            elif bet_push:
                pushes += 1
                data["pushes"] += 1
            else:
                losses += 1
                data["losses"] += 1

            total_bets += 1
            total_staked += stake
            total_profit_loss += profit_loss
            data["total_staked"] += stake
            data["total_profit_loss"] += profit_loss

        summary = {
            "total_bets": total_bets,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "win_rate": round((wins / total_bets) * 100, 1) if total_bets else 0,
            "push_rate": round((pushes / total_bets) * 100, 1) if total_bets else 0,
            "total_staked": round(total_staked, 2),
            "total_profit_loss": round(total_profit_loss, 2),
            "overall_roi": round((total_profit_loss / total_staked) * 100, 1) if total_staked else 0,
        }

        tier_breakdown = []
        for tier in sorted(tier_data.keys()):
            t = tier_data[tier]
            t["win_rate"] = round((t["wins"] / t["total_bets"]) * 100, 1) if t["total_bets"] else 0
            t["roi"] = round((t["total_profit_loss"] / t["total_staked"]) * 100, 1) if t["total_staked"] else 0
            t["total_staked"] = round(t["total_staked"], 2)
            t["total_profit_loss"] = round(t["total_profit_loss"], 2)
            tier_breakdown.append(t)

        summary["tier_breakdown"] = tier_breakdown

        return summary
    
    def insert_box_score_data(self, box_scores: List[Dict]) -> int:
        """Insert box score data into database"""
        if not box_scores:
            return 0
        
        inserted_count = 0
        
        with self.get_connection() as conn:
            for score in box_scores:
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO box_scores (
                            game_id, game_date, player_name, team,
                            game_status, game_completed,
                            at_bats, hits, runs, rbi, home_runs, doubles, triples, singles,
                            walks, strikeouts, stolen_bases, caught_stealing, total_bases,
                            innings_pitched, pitching_outs, hits_allowed, earned_runs,
                            walks_allowed, strikeouts_pitched, home_runs_allowed,
                            fielding_assists, fielding_putouts, fielding_errors,
                            position, batting_order, data_source, data_confidence,
                            collected_at, updated_at
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                        )
                    """, (
                        score.get('game_id'),
                        score.get('game_date'),
                        score.get('player_name'),
                        score.get('team'),
                        score.get('game_status'),
                        score.get('game_completed', False),
                        score.get('at_bats', 0),
                        score.get('hits', 0),
                        score.get('runs', 0),
                        score.get('rbi', 0),
                        score.get('home_runs', 0),
                        score.get('doubles', 0),
                        score.get('triples', 0),
                        score.get('singles', 0),
                        score.get('walks', 0),
                        score.get('strikeouts', 0),
                        score.get('stolen_bases', 0),
                        score.get('caught_stealing', 0),
                        score.get('total_bases', 0),
                        score.get('innings_pitched', 0.0),
                        score.get('pitching_outs', 0),
                        score.get('hits_allowed', 0),
                        score.get('earned_runs', 0),
                        score.get('walks_allowed', 0),
                        score.get('strikeouts_pitched', 0),
                        score.get('home_runs_allowed', 0),
                        score.get('fielding_assists', 0),
                        score.get('fielding_putouts', 0),
                        score.get('fielding_errors', 0),
                        score.get('position'),
                        score.get('batting_order'),
                        score.get('data_source', 'unknown'),
                        score.get('data_confidence', 1.0)
                    ))
                    
                    inserted_count += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting box score: {e}")
                    logger.debug(f"Box score data: {score}")
                    continue
        
        logger.info(f"Inserted {inserted_count} box score records")
        return inserted_count
    
    def get_box_scores_for_date(self, target_date: date) -> List[Dict]:
        """Get all box scores for a specific date"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM box_scores 
                WHERE game_date = ?
                ORDER BY game_id, team, player_name
            """, (target_date,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_player_box_score(self, player_name: str, game_date: date, team: str = None) -> Optional[Dict]:
        """Get box score for a specific player on a specific date"""
        query = """
            SELECT * FROM box_scores
            WHERE player_name = ? AND game_date = ?
        """
        params = [player_name, game_date]
        
        if team:
            query += " AND team = ?"
            params.append(team)
        
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            result = cursor.fetchone()
            return dict(result) if result else None

    def add_player_name_mapping(self, betting_name: str, mlb_name: str,
                                team: str = None, mapping_type: str = 'manual'):
        """Insert or update a player name mapping"""
        with self.get_connection() as conn:
            conn.execute(
                """
                    INSERT OR REPLACE INTO player_name_mapping
                    (betting_name, mlb_name, team, mapping_type)
                    VALUES (?, ?, ?, ?)
                """,
                (betting_name, mlb_name, team, mapping_type),
            )

    def get_mlb_name_for_betting_name(self, betting_name: str, team: str = None) -> Optional[str]:
        """Lookup MLB name for a betting name"""
        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                    SELECT mlb_name
                    FROM player_name_mapping
                    WHERE lower(betting_name) = lower(?)
                      AND (team = ? OR team IS NULL)
                    ORDER BY team DESC
                    LIMIT 1
                """,
                (betting_name, team),
            )
            row = cursor.fetchone()
            return row[0] if row else None
    
    def get_completed_games_without_box_scores(self, target_date: date) -> List[str]:
        """Get list of completed games that don't have box scores yet"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT DISTINCT g.game_id
                FROM games g
                LEFT JOIN box_scores bs ON g.game_id = bs.game_id AND bs.game_date = ?
                WHERE g.game_date = ? 
                  AND g.status IN ('completed', 'final')
                  AND bs.game_id IS NULL
            """, (target_date, target_date))
            
            return [row[0] for row in cursor.fetchall()]


if __name__ == "__main__":
    # Test database creation
    logging.basicConfig(level=logging.INFO)
    db = MLBPropsDatabase("test_mlb_props.db")
    print("Database created successfully!")
    
    # Test session
    session_id = db.start_scrape_session(date.today())
    db.end_scrape_session(session_id, "completed", 100, 2)
    
    summary = db.get_daily_summary(date.today())
    print(f"Daily summary: {summary}")
