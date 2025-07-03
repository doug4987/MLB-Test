#!/usr/bin/env python3
"""
Bet Result Scraper
Fetches actual game statistics to resolve bet outcomes
"""
import requests
import json
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
import time
from database import MLBPropsDatabase

logger = logging.getLogger(__name__)


class MLBResultScraper:
    """Scrapes MLB game results and player statistics for bet resolution"""
    
    def __init__(self, db: MLBPropsDatabase):
        self.db = db
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def resolve_bets_for_date(self, target_date: date) -> Dict[str, Any]:
        """Resolve all bets for a specific date"""
        logger.info(f"Resolving bets for {target_date}")
        
        # Get unresolved bets for the target date
        unresolved_bets = self.db.get_unresolved_bets(target_date)
        target_date_bets = [bet for bet in unresolved_bets if bet['scrape_date'] == target_date.isoformat()]
        
        if not target_date_bets:
            logger.info(f"No unresolved bets found for {target_date}")
            return {'resolved': 0, 'errors': 0, 'games_processed': 0}
        
        logger.info(f"Found {len(target_date_bets)} unresolved bets for {target_date}")
        
        # Group bets by game for efficient processing
        games_bets = {}
        for bet in target_date_bets:
            game_id = bet['game_id']
            if game_id not in games_bets:
                games_bets[game_id] = []
            games_bets[game_id].append(bet)
        
        resolved_count = 0
        error_count = 0
        games_processed = 0
        
        for game_id, game_bets in games_bets.items():
            try:
                logger.info(f"Processing game {game_id} with {len(game_bets)} bets")
                
                # Get game statistics
                game_stats = self.get_game_statistics(game_id, target_date)
                
                if not game_stats:
                    logger.warning(f"No statistics found for game {game_id}")
                    continue
                
                # Resolve each bet in this game
                for bet in game_bets:
                    try:
                        result = self.resolve_single_bet(bet, game_stats)
                        if result:
                            resolved_count += 1
                        else:
                            error_count += 1
                    except Exception as e:
                        logger.error(f"Error resolving bet {bet['id']}: {e}")
                        error_count += 1
                
                games_processed += 1
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error processing game {game_id}: {e}")
                error_count += len(game_bets)
        
        logger.info(f"Resolution complete: {resolved_count} resolved, {error_count} errors, {games_processed} games processed")
        
        return {
            'resolved': resolved_count,
            'errors': error_count,
            'games_processed': games_processed
        }
    
    def get_game_statistics(self, game_id: str, game_date: date) -> Optional[Dict]:
        """Get comprehensive game statistics from multiple sources"""
        # Try ESPN first (most reliable)
        stats = self.get_espn_game_stats(game_id, game_date)
        if stats:
            return stats
        
        # Fallback to MLB Stats API
        stats = self.get_mlb_api_stats(game_id, game_date)
        if stats:
            return stats
        
        # Fallback to Baseball Reference
        stats = self.get_baseball_reference_stats(game_id, game_date)
        if stats:
            return stats
        
        logger.warning(f"No statistics source available for game {game_id}")
        return None
    
    def get_espn_game_stats(self, game_id: str, game_date: date) -> Optional[Dict]:
        """Get game statistics from ESPN"""
        try:
            # Parse team codes from game_id (e.g., "LAD@COL")
            if '@' not in game_id:
                return None
            
            away_team, home_team = game_id.split('@')
            
            # ESPN team code mapping
            espn_teams = {
                'LAD': 'lad', 'COL': 'col', 'NYY': 'nyy', 'BOS': 'bos',
                'TB': 'tb', 'KC': 'kc', 'CLE': 'cle', 'TOR': 'tor',
                'ATL': 'atl', 'NYM': 'nym', 'CHC': 'chc', 'STL': 'stl',
                'MIA': 'mia', 'SF': 'sf', 'ATH': 'oak', 'DET': 'det'
                # Add more team mappings as needed
            }
            
            espn_away = espn_teams.get(away_team)
            espn_home = espn_teams.get(home_team)
            
            if not espn_away or not espn_home:
                logger.debug(f"Team mapping not found for {game_id}")
                return None
            
            # Format date for ESPN API
            date_str = game_date.strftime("%Y%m%d")
            
            # ESPN MLB API endpoint
            url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_str}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Find the specific game
            for game in data.get('events', []):
                competitors = game.get('competitions', [{}])[0].get('competitors', [])
                if len(competitors) != 2:
                    continue
                
                game_teams = [comp.get('team', {}).get('abbreviation', '').lower() for comp in competitors]
                
                if espn_away.lower() in game_teams and espn_home.lower() in game_teams:
                    # Found our game - extract detailed stats
                    return self.parse_espn_game_data(game)
            
            logger.debug(f"Game {game_id} not found in ESPN data for {game_date}")
            return None
            
        except Exception as e:
            logger.debug(f"ESPN API error for {game_id}: {e}")
            return None
    
    def parse_espn_game_data(self, game_data: Dict) -> Dict:
        """Parse ESPN game data into standardized format"""
        try:
            competition = game_data.get('competitions', [{}])[0]
            competitors = competition.get('competitors', [])
            
            # Get basic game info
            game_stats = {
                'status': game_data.get('status', {}).get('type', {}).get('name', 'unknown'),
                'completed': game_data.get('status', {}).get('type', {}).get('completed', False),
                'players': {},
                'source': 'espn'
            }
            
            # Get team lineups and player stats
            for competitor in competitors:
                team_abbr = competitor.get('team', {}).get('abbreviation', '')
                roster = competitor.get('roster', {}).get('athletes', [])
                
                for player in roster:
                    player_name = player.get('displayName', '')
                    position = player.get('position', {}).get('abbreviation', '')
                    
                    # Get player statistics if available
                    stats = player.get('statistics', [])
                    player_stats = {}
                    
                    for stat_category in stats:
                        category_name = stat_category.get('name', '')
                        category_stats = stat_category.get('stats', {})
                        
                        if category_name == 'batting':
                            player_stats.update({
                                'hits': self._safe_int(category_stats.get('hits')),
                                'runs': self._safe_int(category_stats.get('runs')),
                                'rbi': self._safe_int(category_stats.get('rbi')),
                                'home_runs': self._safe_int(category_stats.get('homeRuns')),
                                'doubles': self._safe_int(category_stats.get('doubles')),
                                'triples': self._safe_int(category_stats.get('triples')),
                                'singles': self._safe_int(category_stats.get('singles')),
                                'stolen_bases': self._safe_int(category_stats.get('stolenBases')),
                                'total_bases': self._safe_int(category_stats.get('totalBases'))
                            })
                        elif category_name == 'pitching':
                            player_stats.update({
                                'innings_pitched': self._safe_float(category_stats.get('inningsPitched')),
                                'pitching_outs': self._safe_int(category_stats.get('outs')),
                                'strikeouts': self._safe_int(category_stats.get('strikeouts')),
                                'hits_allowed': self._safe_int(category_stats.get('hits')),
                                'earned_runs': self._safe_int(category_stats.get('earnedRuns'))
                            })
                    
                    if player_stats:
                        game_stats['players'][player_name] = {
                            'team': team_abbr,
                            'position': position,
                            'stats': player_stats
                        }
            
            return game_stats
            
        except Exception as e:
            logger.error(f"Error parsing ESPN game data: {e}")
            return {}
    
    def get_mlb_api_stats(self, game_id: str, game_date: date) -> Optional[Dict]:
        """Get game statistics from MLB Stats API"""
        try:
            # MLB Stats API is free but requires game ID lookup
            date_str = game_date.strftime("%Y-%m-%d")
            url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_str}&hydrate=boxscore"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Find matching game and extract player stats
            for game_date_info in data.get('dates', []):
                for game in game_date_info.get('games', []):
                    away_team = game.get('teams', {}).get('away', {}).get('team', {}).get('abbreviation', '')
                    home_team = game.get('teams', {}).get('home', {}).get('team', {}).get('abbreviation', '')
                    
                    if f"{away_team}@{home_team}" == game_id:
                        return self.parse_mlb_api_game_data(game)
            
            return None
            
        except Exception as e:
            logger.debug(f"MLB API error for {game_id}: {e}")
            return None
    
    def parse_mlb_api_game_data(self, game_data: Dict) -> Dict:
        """Parse MLB API game data into standardized format"""
        try:
            game_stats = {
                'status': game_data.get('status', {}).get('detailedState', 'unknown'),
                'completed': game_data.get('status', {}).get('statusCode', '') == 'F',
                'players': {},
                'source': 'mlb_api'
            }
            
            # Extract player statistics from boxscore
            boxscore = game_data.get('boxscore', {})
            teams = boxscore.get('teams', {})
            
            for team_side in ['away', 'home']:
                team_data = teams.get(team_side, {})
                players = team_data.get('players', {})
                
                for player_id, player_info in players.items():
                    person = player_info.get('person', {})
                    player_name = person.get('fullName', '')
                    
                    stats = player_info.get('stats', {})
                    batting = stats.get('batting', {})
                    pitching = stats.get('pitching', {})
                    
                    player_stats = {}
                    
                    # Batting stats
                    if batting:
                        player_stats.update({
                            'hits': self._safe_int(batting.get('hits')),
                            'runs': self._safe_int(batting.get('runs')),
                            'rbi': self._safe_int(batting.get('rbi')),
                            'home_runs': self._safe_int(batting.get('homeRuns')),
                            'doubles': self._safe_int(batting.get('doubles')),
                            'triples': self._safe_int(batting.get('triples')),
                            'stolen_bases': self._safe_int(batting.get('stolenBases')),
                            'total_bases': self._safe_int(batting.get('totalBases'))
                        })
                        
                        # Calculate singles
                        hits = player_stats.get('hits', 0)
                        doubles = player_stats.get('doubles', 0)
                        triples = player_stats.get('triples', 0)
                        home_runs = player_stats.get('home_runs', 0)
                        player_stats['singles'] = max(0, hits - doubles - triples - home_runs)
                    
                    # Pitching stats
                    if pitching:
                        player_stats.update({
                            'innings_pitched': self._safe_float(pitching.get('inningsPitched')),
                            'pitching_outs': self._safe_int(pitching.get('outs')),
                            'strikeouts': self._safe_int(pitching.get('strikeOuts')),
                            'hits_allowed': self._safe_int(pitching.get('hits')),
                            'earned_runs': self._safe_int(pitching.get('earnedRuns'))
                        })
                    
                    if player_stats:
                        game_stats['players'][player_name] = {
                            'team': team_data.get('team', {}).get('abbreviation', ''),
                            'stats': player_stats
                        }
            
            return game_stats
            
        except Exception as e:
            logger.error(f"Error parsing MLB API game data: {e}")
            return {}
    
    def get_baseball_reference_stats(self, game_id: str, game_date: date) -> Optional[Dict]:
        """Get game statistics from Baseball Reference (backup source)"""
        try:
            # Baseball Reference scraping would go here
            # This is more complex due to HTML parsing requirements
            # For now, return None to indicate unavailable
            logger.debug("Baseball Reference scraping not implemented yet")
            return None
            
        except Exception as e:
            logger.debug(f"Baseball Reference error for {game_id}: {e}")
            return None
    
    def resolve_single_bet(self, bet: Dict, game_stats: Dict) -> bool:
        """Resolve a single bet using game statistics"""
        try:
            player_name = bet['player_name']
            market = bet['market']
            
            # Find player in game stats
            player_stats = None
            for name, stats in game_stats.get('players', {}).items():
                if self._names_match(player_name, name):
                    player_stats = stats.get('stats', {})
                    break
            
            if not player_stats:
                logger.warning(f"Player {player_name} not found in game stats for {bet['game_id']}")
                return False
            
            # Get actual result based on market
            actual_result = self._get_market_result(market, player_stats)
            
            if actual_result is None:
                logger.warning(f"Could not determine result for {player_name} {market}")
                return False
            
            # Insert bet result
            self.db.insert_bet_result(
                prop_id=bet['id'],
                actual_result=actual_result,
                result_source=game_stats.get('source', 'unknown'),
                result_confidence=0.95,  # High confidence for API data
                notes=f"Auto-resolved from {game_stats.get('source', 'unknown')}"
            )
            
            logger.info(f"Resolved bet: {player_name} {market} = {actual_result}")
            return True
            
        except Exception as e:
            logger.error(f"Error resolving bet {bet['id']}: {e}")
            return False
    
    def _get_market_result(self, market: str, player_stats: Dict) -> Optional[float]:
        """Get actual statistical result for a specific market"""
        market_lower = market.lower()
        
        # Batting markets
        if 'hits' in market_lower and 'runs' in market_lower and 'rbi' in market_lower:
            hits = player_stats.get('hits', 0) or 0
            runs = player_stats.get('runs', 0) or 0
            rbi = player_stats.get('rbi', 0) or 0
            return float(hits + runs + rbi)
        elif market_lower == 'hits':
            return float(player_stats.get('hits', 0) or 0)
        elif market_lower == 'runs':
            return float(player_stats.get('runs', 0) or 0)
        elif market_lower == 'rbis':
            return float(player_stats.get('rbi', 0) or 0)
        elif 'home runs' in market_lower:
            return float(player_stats.get('home_runs', 0) or 0)
        elif market_lower == 'doubles':
            return float(player_stats.get('doubles', 0) or 0)
        elif market_lower == 'triples':
            return float(player_stats.get('triples', 0) or 0)
        elif market_lower == 'singles':
            return float(player_stats.get('singles', 0) or 0)
        elif 'stolen bases' in market_lower:
            return float(player_stats.get('stolen_bases', 0) or 0)
        elif 'total bases' in market_lower:
            return float(player_stats.get('total_bases', 0) or 0)
        
        # Pitching markets
        elif 'strikeouts' in market_lower:
            return float(player_stats.get('strikeouts', 0) or 0)
        elif 'pitching outs' in market_lower:
            return float(player_stats.get('pitching_outs', 0) or 0)
        elif 'hits allowed' in market_lower:
            return float(player_stats.get('hits_allowed', 0) or 0)
        elif 'earned runs' in market_lower:
            return float(player_stats.get('earned_runs', 0) or 0)
        
        logger.warning(f"Unknown market type: {market}")
        return None
    
    def _names_match(self, name1: str, name2: str) -> bool:
        """Check if two player names match (handles variations)"""
        # Simple matching - can be enhanced for nickname handling
        name1_clean = name1.lower().replace('.', '').replace(' jr', '').replace(' sr', '').strip()
        name2_clean = name2.lower().replace('.', '').replace(' jr', '').replace(' sr', '').strip()
        
        return name1_clean == name2_clean
    
    def _safe_int(self, value) -> int:
        """Safely convert value to int"""
        try:
            return int(value) if value is not None else 0
        except (ValueError, TypeError):
            return 0
    
    def _safe_float(self, value) -> float:
        """Safely convert value to float"""
        try:
            return float(value) if value is not None else 0.0
        except (ValueError, TypeError):
            return 0.0


if __name__ == "__main__":
    # Test result scraper
    logging.basicConfig(level=logging.INFO)
    
    db = MLBPropsDatabase()
    scraper = MLBResultScraper(db)
    
    # Test resolving bets for yesterday
    yesterday = date.today() - timedelta(days=1)
    results = scraper.resolve_bets_for_date(yesterday)
    
    print(f"Resolution results: {results}")
