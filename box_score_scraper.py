#!/usr/bin/env python3
"""
Box Score Scraper
Fetches comprehensive MLB player statistics for completed games only
"""
import requests
import json
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
import time
from database import MLBPropsDatabase

logger = logging.getLogger(__name__)


class MLBBoxScoreScraper:
    """Scrapes MLB box scores and player statistics for completed games"""
    
    def __init__(self, db: MLBPropsDatabase):
        self.db = db
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def collect_box_scores_for_date(self, target_date: date, force_update: bool = False) -> Dict[str, Any]:
        """Collect box scores for all completed games on a specific date"""
        logger.info(f"Collecting box scores for {target_date}")
        
        # Get all completed games from MLB API (independent of props database)
        completed_games = self._get_completed_games_from_mlb_api(target_date)
        
        if not completed_games:
            logger.info(f"No completed games found for {target_date}")
            return {'games_processed': 0, 'players_collected': 0, 'errors': 0}
        
        logger.info(f"Found {len(completed_games)} completed games from MLB API")
        
        # Filter out games we already have box scores for (unless force_update)
        if not force_update:
            games_to_process = self._filter_games_without_box_scores(completed_games, target_date)
        else:
            games_to_process = completed_games
        
        if not games_to_process:
            logger.info(f"All games already have box scores for {target_date}")
            return {'games_processed': 0, 'players_collected': 0, 'errors': 0}
        
        logger.info(f"Found {len(games_to_process)} games to process")
        
        total_players = 0
        error_count = 0
        games_processed = 0
        
        # Try MLB API first, then ESPN as fallback
        for game_id in games_to_process:
            try:
                logger.info(f"Processing game {game_id}")
                
                # First check if game is actually completed
                game_status = self._check_game_completion_status(game_id, target_date)
                
                if not game_status['completed']:
                    logger.info(f"Game {game_id} is not completed yet ({game_status['status']}), skipping")
                    continue
                
                # Try to get box score data
                box_score_data = self._get_game_box_score(game_id, target_date)
                
                if box_score_data:
                    # Insert box score data into database
                    inserted_count = self.db.insert_box_score_data(box_score_data)
                    total_players += inserted_count
                    games_processed += 1
                    
                    logger.info(f"Collected {inserted_count} player stats for game {game_id}")
                else:
                    logger.warning(f"No box score data found for game {game_id}")
                    error_count += 1
                
                # Rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing game {game_id}: {e}")
                error_count += 1
                continue
        
        logger.info(f"Box score collection complete: {games_processed} games, {total_players} players, {error_count} errors")
        
        return {
            'games_processed': games_processed,
            'players_collected': total_players,
            'errors': error_count
        }
    
    def _check_game_completion_status(self, game_id: str, game_date: date) -> Dict[str, Any]:
        """Check if a game is completed using multiple sources"""
        
        # Try MLB API first
        mlb_status = self._check_mlb_api_game_status(game_id, game_date)
        if mlb_status['found']:
            return mlb_status
        
        # Fallback to ESPN
        espn_status = self._check_espn_game_status(game_id, game_date)
        if espn_status['found']:
            return espn_status
        
        # If not found in either, assume not completed
        return {
            'found': False,
            'completed': False,
            'status': 'unknown',
            'source': 'none'
        }
    
    def _check_mlb_api_game_status(self, game_id: str, game_date: date) -> Dict[str, Any]:
        """Check game completion status via MLB API"""
        try:
            date_str = game_date.strftime("%Y-%m-%d")
            url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_str}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Find the specific game
            for date_info in data.get('dates', []):
                for game in date_info.get('games', []):
                    # Get team IDs to fetch abbreviations
                    away_team_id = game.get('teams', {}).get('away', {}).get('team', {}).get('id')
                    home_team_id = game.get('teams', {}).get('home', {}).get('team', {}).get('id')
                    
                    if away_team_id and home_team_id:
                        # Get team abbreviations
                        away_abbr = self._get_team_abbreviation(away_team_id)
                        home_abbr = self._get_team_abbreviation(home_team_id)
                        
                        if f"{away_abbr}@{home_abbr}" == game_id:
                            game_status = game.get('status', {})
                            status_code = game_status.get('statusCode', '')
                            detailed_state = game_status.get('detailedState', '')
                            
                            # Game is completed if status code is 'F' (Final)
                            completed = status_code == 'F'
                            
                            return {
                                'found': True,
                                'completed': completed,
                                'status': detailed_state,
                                'status_code': status_code,
                                'source': 'mlb_api'
                            }
            
            return {'found': False, 'completed': False, 'status': 'not_found', 'source': 'mlb_api'}
            
        except Exception as e:
            logger.debug(f"MLB API status check failed for {game_id}: {e}")
            return {'found': False, 'completed': False, 'status': 'error', 'source': 'mlb_api'}
    
    def _check_espn_game_status(self, game_id: str, game_date: date) -> Dict[str, Any]:
        """Check game completion status via ESPN API"""
        try:
            if '@' not in game_id:
                return {'found': False, 'completed': False, 'status': 'invalid_format', 'source': 'espn'}
            
            away_team, home_team = game_id.split('@')
            
            # ESPN team mapping (basic set - can be expanded)
            espn_teams = {
                'LAD': 'lad', 'COL': 'col', 'NYY': 'nyy', 'BOS': 'bos',
                'TB': 'tb', 'KC': 'kc', 'CLE': 'cle', 'TOR': 'tor',
                'ATL': 'atl', 'NYM': 'nym', 'CHC': 'chc', 'STL': 'stl',
                'MIA': 'mia', 'SF': 'sf', 'OAK': 'oak', 'DET': 'det',
                'HOU': 'hou', 'TEX': 'tex', 'SEA': 'sea', 'LAA': 'laa',
                'SD': 'sd', 'AZ': 'ari', 'ARI': 'ari', 'MIN': 'min', 'CWS': 'cws',
                'MIL': 'mil', 'CIN': 'cin', 'PIT': 'pit', 'WSH': 'wsh',
                'PHI': 'phi', 'BAL': 'bal'
            }
            
            espn_away = espn_teams.get(away_team)
            espn_home = espn_teams.get(home_team)
            
            if not espn_away or not espn_home:
                return {'found': False, 'completed': False, 'status': 'team_mapping_failed', 'source': 'espn'}
            
            date_str = game_date.strftime("%Y%m%d")
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
                    game_status = game.get('status', {})
                    status_type = game_status.get('type', {})
                    completed = status_type.get('completed', False)
                    status_name = status_type.get('name', 'unknown')
                    
                    return {
                        'found': True,
                        'completed': completed,
                        'status': status_name,
                        'source': 'espn'
                    }
            
            return {'found': False, 'completed': False, 'status': 'not_found', 'source': 'espn'}
            
        except Exception as e:
            logger.debug(f"ESPN status check failed for {game_id}: {e}")
            return {'found': False, 'completed': False, 'status': 'error', 'source': 'espn'}
    
    def _get_game_box_score(self, game_id: str, game_date: date) -> Optional[List[Dict]]:
        """Get comprehensive box score data for a game"""
        
        # Try MLB API first (most detailed)
        box_score_data = self._get_mlb_api_box_score(game_id, game_date)
        if box_score_data:
            logger.debug(f"Got box score from MLB API for {game_id}")
            return box_score_data
        
        # Fallback to ESPN
        box_score_data = self._get_espn_box_score(game_id, game_date)
        if box_score_data:
            logger.debug(f"Got box score from ESPN for {game_id}")
            return box_score_data
        
        logger.warning(f"No box score data available for {game_id}")
        return None
    
    def _get_mlb_api_box_score(self, game_id: str, game_date: date) -> Optional[List[Dict]]:
        """Get box score data from MLB Stats API"""
        try:
            # First, get the schedule to find the gamePk
            date_str = game_date.strftime("%Y-%m-%d")
            schedule_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_str}"
            
            response = self.session.get(schedule_url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Find the specific game and get its gamePk
            game_pk = None
            game_info = None
            
            for date_info in data.get('dates', []):
                for game in date_info.get('games', []):
                    # Get team IDs to fetch abbreviations
                    away_team_id = game.get('teams', {}).get('away', {}).get('team', {}).get('id')
                    home_team_id = game.get('teams', {}).get('home', {}).get('team', {}).get('id')
                    
                    if away_team_id and home_team_id:
                        # Get team abbreviations
                        away_abbr = self._get_team_abbreviation(away_team_id)
                        home_abbr = self._get_team_abbreviation(home_team_id)
                        
                        if f"{away_abbr}@{home_abbr}" == game_id:
                            game_pk = game.get('gamePk')
                            game_info = game
                            break
                            
                if game_pk:
                    break
            
            if not game_pk:
                logger.debug(f"Game {game_id} not found in schedule for {game_date}")
                return None
            
            # Now get the detailed boxscore using the gamePk
            boxscore_url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"
            
            box_response = self.session.get(boxscore_url, timeout=15)
            box_response.raise_for_status()
            
            boxscore_data = box_response.json()
            
            # Parse the boxscore data with game info
            return self._parse_mlb_api_box_score_direct(boxscore_data, game_info, game_date)
            
        except Exception as e:
            logger.debug(f"MLB API box score failed for {game_id}: {e}")
            return None
    
    def _parse_mlb_api_box_score_direct(self, boxscore_data: Dict, game_info: Dict, game_date: date) -> List[Dict]:
        """Parse direct MLB API boxscore data into standardized format"""
        try:
            box_scores = []
            
            # Get basic game info with team abbreviations
            away_team_id = game_info.get('teams', {}).get('away', {}).get('team', {}).get('id')
            home_team_id = game_info.get('teams', {}).get('home', {}).get('team', {}).get('id')
            
            away_abbr = self._get_team_abbreviation(away_team_id) if away_team_id else ''
            home_abbr = self._get_team_abbreviation(home_team_id) if home_team_id else ''
            
            game_id = f"{away_abbr}@{home_abbr}"
            game_status = game_info.get('status', {}).get('detailedState', 'unknown')
            game_completed = game_info.get('status', {}).get('statusCode', '') == 'F'
            
            # Extract player statistics from boxscore
            teams = boxscore_data.get('teams', {})
            
            for team_side in ['away', 'home']:
                team_data = teams.get(team_side, {})
                # Use the abbreviation we determined from team IDs
                team_abbr = away_abbr if team_side == 'away' else home_abbr
                players = team_data.get('players', {})
                
                for player_id, player_info in players.items():
                    person = player_info.get('person', {})
                    player_name = person.get('fullName', '')
                    position = player_info.get('position', {}).get('abbreviation', '')
                    
                    stats = player_info.get('stats', {})
                    batting = stats.get('batting', {})
                    pitching = stats.get('pitching', {})
                    fielding = stats.get('fielding', {})
                    
                    # Create box score record
                    box_score_record = {
                        'game_id': game_id,
                        'game_date': game_date,
                        'player_name': player_name,
                        'team': team_abbr,
                        'game_status': game_status,
                        'game_completed': game_completed,
                        'position': position,
                        'data_source': 'mlb_api',
                        'data_confidence': 0.95
                    }
                    
                    # Add batting stats
                    if batting:
                        hits = self._safe_int(batting.get('hits', 0))
                        doubles = self._safe_int(batting.get('doubles', 0))
                        triples = self._safe_int(batting.get('triples', 0))
                        home_runs = self._safe_int(batting.get('homeRuns', 0))
                        
                        box_score_record.update({
                            'at_bats': self._safe_int(batting.get('atBats', 0)),
                            'hits': hits,
                            'runs': self._safe_int(batting.get('runs', 0)),
                            'rbi': self._safe_int(batting.get('rbi', 0)),
                            'home_runs': home_runs,
                            'doubles': doubles,
                            'triples': triples,
                            'singles': max(0, hits - doubles - triples - home_runs),
                            'walks': self._safe_int(batting.get('baseOnBalls', 0)),
                            'strikeouts': self._safe_int(batting.get('strikeOuts', 0)),
                            'stolen_bases': self._safe_int(batting.get('stolenBases', 0)),
                            'caught_stealing': self._safe_int(batting.get('caughtStealing', 0)),
                            'total_bases': self._safe_int(batting.get('totalBases', 0))
                        })
                    
                    # Add pitching stats
                    if pitching:
                        box_score_record.update({
                            'innings_pitched': self._safe_float(pitching.get('inningsPitched', 0.0)),
                            'pitching_outs': self._safe_int(pitching.get('outs', 0)),
                            'hits_allowed': self._safe_int(pitching.get('hits', 0)),
                            'earned_runs': self._safe_int(pitching.get('earnedRuns', 0)),
                            'walks_allowed': self._safe_int(pitching.get('baseOnBalls', 0)),
                            'strikeouts_pitched': self._safe_int(pitching.get('strikeOuts', 0)),
                            'home_runs_allowed': self._safe_int(pitching.get('homeRuns', 0))
                        })
                    
                    # Add fielding stats
                    if fielding:
                        box_score_record.update({
                            'fielding_assists': self._safe_int(fielding.get('assists', 0)),
                            'fielding_putouts': self._safe_int(fielding.get('putOuts', 0)),
                            'fielding_errors': self._safe_int(fielding.get('errors', 0))
                        })
                    
                    # Only add if player has some stats (played in the game)
                    if (box_score_record.get('at_bats', 0) > 0 or 
                        box_score_record.get('innings_pitched', 0.0) > 0.0 or
                        box_score_record.get('fielding_putouts', 0) > 0):
                        box_scores.append(box_score_record)
            
            return box_scores
            
        except Exception as e:
            logger.error(f"Error parsing direct MLB API box score: {e}")
            return []
    
    def _parse_mlb_api_box_score(self, game_data: Dict, game_date: date) -> List[Dict]:
        """Parse MLB API box score data into standardized format"""
        try:
            box_scores = []
            
            # Get basic game info with team abbreviations
            away_team_id = game_data.get('teams', {}).get('away', {}).get('team', {}).get('id')
            home_team_id = game_data.get('teams', {}).get('home', {}).get('team', {}).get('id')
            
            away_abbr = self._get_team_abbreviation(away_team_id) if away_team_id else ''
            home_abbr = self._get_team_abbreviation(home_team_id) if home_team_id else ''
            
            game_id = f"{away_abbr}@{home_abbr}"
            game_status = game_data.get('status', {}).get('detailedState', 'unknown')
            game_completed = game_data.get('status', {}).get('statusCode', '') == 'F'
            
            # Extract player statistics from boxscore
            boxscore = game_data.get('boxscore', {})
            teams = boxscore.get('teams', {})
            
            for team_side in ['away', 'home']:
                team_data = teams.get(team_side, {})
                # Use the abbreviation we already determined from team IDs
                team_abbr = away_abbr if team_side == 'away' else home_abbr
                players = team_data.get('players', {})
                
                for player_id, player_info in players.items():
                    person = player_info.get('person', {})
                    player_name = person.get('fullName', '')
                    position = player_info.get('position', {}).get('abbreviation', '')
                    
                    stats = player_info.get('stats', {})
                    batting = stats.get('batting', {})
                    pitching = stats.get('pitching', {})
                    fielding = stats.get('fielding', {})
                    
                    # Create box score record
                    box_score_record = {
                        'game_id': game_id,
                        'game_date': game_date,
                        'player_name': player_name,
                        'team': team_abbr,
                        'game_status': game_status,
                        'game_completed': game_completed,
                        'position': position,
                        'data_source': 'mlb_api',
                        'data_confidence': 0.95
                    }
                    
                    # Add batting stats
                    if batting:
                        hits = self._safe_int(batting.get('hits', 0))
                        doubles = self._safe_int(batting.get('doubles', 0))
                        triples = self._safe_int(batting.get('triples', 0))
                        home_runs = self._safe_int(batting.get('homeRuns', 0))
                        
                        box_score_record.update({
                            'at_bats': self._safe_int(batting.get('atBats', 0)),
                            'hits': hits,
                            'runs': self._safe_int(batting.get('runs', 0)),
                            'rbi': self._safe_int(batting.get('rbi', 0)),
                            'home_runs': home_runs,
                            'doubles': doubles,
                            'triples': triples,
                            'singles': max(0, hits - doubles - triples - home_runs),
                            'walks': self._safe_int(batting.get('baseOnBalls', 0)),
                            'strikeouts': self._safe_int(batting.get('strikeOuts', 0)),
                            'stolen_bases': self._safe_int(batting.get('stolenBases', 0)),
                            'caught_stealing': self._safe_int(batting.get('caughtStealing', 0)),
                            'total_bases': self._safe_int(batting.get('totalBases', 0))
                        })
                    
                    # Add pitching stats
                    if pitching:
                        box_score_record.update({
                            'innings_pitched': self._safe_float(pitching.get('inningsPitched', 0.0)),
                            'pitching_outs': self._safe_int(pitching.get('outs', 0)),
                            'hits_allowed': self._safe_int(pitching.get('hits', 0)),
                            'earned_runs': self._safe_int(pitching.get('earnedRuns', 0)),
                            'walks_allowed': self._safe_int(pitching.get('baseOnBalls', 0)),
                            'strikeouts_pitched': self._safe_int(pitching.get('strikeOuts', 0)),
                            'home_runs_allowed': self._safe_int(pitching.get('homeRuns', 0))
                        })
                    
                    # Add fielding stats
                    if fielding:
                        box_score_record.update({
                            'fielding_assists': self._safe_int(fielding.get('assists', 0)),
                            'fielding_putouts': self._safe_int(fielding.get('putOuts', 0)),
                            'fielding_errors': self._safe_int(fielding.get('errors', 0))
                        })
                    
                    # Only add if player has some stats (played in the game)
                    if (box_score_record.get('at_bats', 0) > 0 or 
                        box_score_record.get('innings_pitched', 0.0) > 0.0 or
                        box_score_record.get('fielding_putouts', 0) > 0):
                        box_scores.append(box_score_record)
            
            return box_scores
            
        except Exception as e:
            logger.error(f"Error parsing MLB API box score: {e}")
            return []
    
    def _get_espn_box_score(self, game_id: str, game_date: date) -> Optional[List[Dict]]:
        """Get box score data from ESPN (fallback)"""
        try:
            if '@' not in game_id:
                return None
            
            away_team, home_team = game_id.split('@')
            
            # ESPN team mapping (basic set)
            espn_teams = {
                'LAD': 'lad', 'COL': 'col', 'NYY': 'nyy', 'BOS': 'bos',
                'TB': 'tb', 'KC': 'kc', 'CLE': 'cle', 'TOR': 'tor',
                'ATL': 'atl', 'NYM': 'nym', 'CHC': 'chc', 'STL': 'stl',
                'MIA': 'mia', 'SF': 'sf', 'OAK': 'oak', 'DET': 'det',
                'HOU': 'hou', 'TEX': 'tex', 'SEA': 'sea', 'LAA': 'laa',
                'SD': 'sd', 'ARI': 'ari', 'MIN': 'min', 'CWS': 'cws',
                'MIL': 'mil', 'CIN': 'cin', 'PIT': 'pit', 'WSH': 'wsh',
                'PHI': 'phi', 'BAL': 'bal'
            }
            
            espn_away = espn_teams.get(away_team)
            espn_home = espn_teams.get(home_team)
            
            if not espn_away or not espn_home:
                return None
            
            date_str = game_date.strftime("%Y%m%d")
            url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_str}"
            
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            # Find the specific game
            for game in data.get('events', []):
                competitors = game.get('competitions', [{}])[0].get('competitors', [])
                if len(competitors) != 2:
                    continue
                
                game_teams = [comp.get('team', {}).get('abbreviation', '').lower() for comp in competitors]
                
                if espn_away.lower() in game_teams and espn_home.lower() in game_teams:
                    return self._parse_espn_box_score(game, game_date, game_id)
            
            return None
            
        except Exception as e:
            logger.debug(f"ESPN box score failed for {game_id}: {e}")
            return None
    
    def _parse_espn_box_score(self, game_data: Dict, game_date: date, game_id: str) -> List[Dict]:
        """Parse ESPN box score data into standardized format"""
        try:
            box_scores = []
            
            # Get basic game info
            game_status = game_data.get('status', {}).get('type', {}).get('name', 'unknown')
            game_completed = game_data.get('status', {}).get('type', {}).get('completed', False)
            
            competition = game_data.get('competitions', [{}])[0]
            competitors = competition.get('competitors', [])
            
            # Get team lineups and player stats
            for competitor in competitors:
                team_abbr = competitor.get('team', {}).get('abbreviation', '')
                roster = competitor.get('roster', {}).get('athletes', [])
                
                for player in roster:
                    player_name = player.get('displayName', '')
                    position = player.get('position', {}).get('abbreviation', '')
                    
                    # Create box score record
                    box_score_record = {
                        'game_id': game_id,
                        'game_date': game_date,
                        'player_name': player_name,
                        'team': team_abbr,
                        'game_status': game_status,
                        'game_completed': game_completed,
                        'position': position,
                        'data_source': 'espn',
                        'data_confidence': 0.85
                    }
                    
                    # Get player statistics if available
                    stats = player.get('statistics', [])
                    has_stats = False
                    
                    for stat_category in stats:
                        category_name = stat_category.get('name', '')
                        category_stats = stat_category.get('stats', {})
                        
                        if category_name == 'batting':
                            hits = self._safe_int(category_stats.get('hits', 0))
                            doubles = self._safe_int(category_stats.get('doubles', 0))
                            triples = self._safe_int(category_stats.get('triples', 0))
                            home_runs = self._safe_int(category_stats.get('homeRuns', 0))
                            
                            box_score_record.update({
                                'at_bats': self._safe_int(category_stats.get('atBats', 0)),
                                'hits': hits,
                                'runs': self._safe_int(category_stats.get('runs', 0)),
                                'rbi': self._safe_int(category_stats.get('rbi', 0)),
                                'home_runs': home_runs,
                                'doubles': doubles,
                                'triples': triples,
                                'singles': max(0, hits - doubles - triples - home_runs),
                                'walks': self._safe_int(category_stats.get('walks', 0)),
                                'strikeouts': self._safe_int(category_stats.get('strikeouts', 0)),
                                'stolen_bases': self._safe_int(category_stats.get('stolenBases', 0)),
                                'total_bases': self._safe_int(category_stats.get('totalBases', 0))
                            })
                            has_stats = True
                            
                        elif category_name == 'pitching':
                            box_score_record.update({
                                'innings_pitched': self._safe_float(category_stats.get('inningsPitched', 0.0)),
                                'pitching_outs': self._safe_int(category_stats.get('outs', 0)),
                                'hits_allowed': self._safe_int(category_stats.get('hits', 0)),
                                'earned_runs': self._safe_int(category_stats.get('earnedRuns', 0)),
                                'walks_allowed': self._safe_int(category_stats.get('walks', 0)),
                                'strikeouts_pitched': self._safe_int(category_stats.get('strikeouts', 0)),
                                'home_runs_allowed': self._safe_int(category_stats.get('homeRuns', 0))
                            })
                            has_stats = True
                    
                    # Only add if player has some stats (played in the game)
                    if has_stats and (box_score_record.get('at_bats', 0) > 0 or 
                                     box_score_record.get('innings_pitched', 0.0) > 0.0):
                        box_scores.append(box_score_record)
            
            return box_scores
            
        except Exception as e:
            logger.error(f"Error parsing ESPN box score: {e}")
            return []
    
    def _get_all_games_for_date(self, target_date: date) -> List[str]:
        """Get all games for a specific date (regardless of box score status)"""
        with self.db.get_connection() as conn:
            cursor = conn.execute("""
                SELECT DISTINCT game_id
                FROM games
                WHERE game_date = ?
                ORDER BY game_id
            """, (target_date,))
            
            return [row[0] for row in cursor.fetchall()]
    
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
    
    def _get_completed_games_from_mlb_api(self, target_date: date) -> List[str]:
        """Get all completed games for a date directly from MLB API"""
        try:
            date_str = target_date.strftime("%Y-%m-%d")
            url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_str}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            completed_games = []
            
            for date_info in data.get('dates', []):
                for game in date_info.get('games', []):
                    # Check if game is completed
                    status_code = game.get('status', {}).get('statusCode', '')
                    
                    if status_code == 'F':  # Final
                        # Get team abbreviations
                        away_team_id = game.get('teams', {}).get('away', {}).get('team', {}).get('id')
                        home_team_id = game.get('teams', {}).get('home', {}).get('team', {}).get('id')
                        
                        if away_team_id and home_team_id:
                            away_abbr = self._get_team_abbreviation(away_team_id)
                            home_abbr = self._get_team_abbreviation(home_team_id)
                            
                            if away_abbr and home_abbr:
                                game_id = f"{away_abbr}@{home_abbr}"
                                completed_games.append(game_id)
                                logger.debug(f"Found completed game: {game_id}")
            
            return completed_games
            
        except Exception as e:
            logger.error(f"Error getting completed games from MLB API: {e}")
            return []
    
    def _filter_games_without_box_scores(self, games: List[str], target_date: date) -> List[str]:
        """Filter out games that already have box scores in the database"""
        try:
            games_without_scores = []
            
            for game_id in games:
                # Check if we already have box scores for this game
                with self.db.get_connection() as conn:
                    cursor = conn.execute("""
                        SELECT COUNT(*) FROM box_scores 
                        WHERE game_id = ? AND game_date = ?
                    """, (game_id, target_date))
                    
                    count = cursor.fetchone()[0]
                    
                    if count == 0:
                        games_without_scores.append(game_id)
                    else:
                        logger.debug(f"Game {game_id} already has {count} box score records")
            
            return games_without_scores
            
        except Exception as e:
            logger.error(f"Error filtering games: {e}")
            return games  # Return all games if filtering fails
    
    def _get_team_abbreviation(self, team_id: int) -> str:
        """Get team abbreviation from team ID using MLB API"""
        try:
            url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}"
            response = self.session.get(url, timeout=5)
            response.raise_for_status()
            
            data = response.json()
            teams = data.get('teams', [])
            
            if teams:
                return teams[0].get('abbreviation', '')
            
            return ''
            
        except Exception as e:
            logger.debug(f"Failed to get abbreviation for team {team_id}: {e}")
            return ''


def main():
    """Test box score scraper"""
    logging.basicConfig(level=logging.INFO)
    
    db = MLBPropsDatabase()
    scraper = MLBBoxScoreScraper(db)
    
    # Test with yesterday's games
    yesterday = date.today() - timedelta(days=1)
    results = scraper.collect_box_scores_for_date(yesterday)
    
    print(f"Box score collection results: {results}")
    
    # Show some sample data
    box_scores = db.get_box_scores_for_date(yesterday)
    if box_scores:
        print(f"\nSample box scores collected: {len(box_scores)} total")
        print(f"First record: {box_scores[0]}")


if __name__ == "__main__":
    main()
