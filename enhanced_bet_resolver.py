#!/usr/bin/env python3
"""
Enhanced Bet Resolution System
Automatically matches box scores with bets to resolve outcomes
"""
import logging
from datetime import date, timedelta
from typing import Dict, List, Optional, Any, Tuple
import re
from database import MLBPropsDatabase

logger = logging.getLogger(__name__)


class EnhancedBetResolver:
    """Enhanced bet resolver that uses box scores to automatically resolve bets"""
    
    def __init__(self, db: MLBPropsDatabase):
        self.db = db
    
    def resolve_bets_for_date(self, target_date: date) -> Dict[str, Any]:
        """Resolve all bets for a specific date using box score data"""
        logger.info(f"🎲 Resolving bets for {target_date}")
        
        # Get all box scores for the date
        box_scores = self.db.get_box_scores_for_date(target_date)
        if not box_scores:
            logger.warning(f"No box scores found for {target_date}")
            return {'resolved': 0, 'errors': 0, 'no_box_scores': True}
        
        logger.info(f"Found {len(box_scores)} box score records for {target_date}")
        
        # Get unresolved bets for the date  
        unresolved_bets = self._get_unresolved_bets_for_date(target_date)
        if not unresolved_bets:
            logger.info(f"No unresolved bets found for {target_date}")
            return {'resolved': 0, 'errors': 0, 'no_bets': True}
        
        logger.info(f"Found {len(unresolved_bets)} unresolved bets for {target_date}")
        
        # Create lookup dictionaries for efficient matching
        box_score_lookup = self._create_box_score_lookup(box_scores)
        
        resolved_count = 0
        error_count = 0
        
        for bet in unresolved_bets:
            try:
                success = self._resolve_single_bet(bet, box_score_lookup)
                if success:
                    resolved_count += 1
                else:
                    error_count += 1
                    
            except Exception as e:
                logger.error(f"Error resolving bet {bet.get('id', 'unknown')}: {e}")
                error_count += 1
        
        logger.info(f"✅ Bet resolution completed: {resolved_count} resolved, {error_count} errors")
        
        return {
            'resolved': resolved_count,
            'errors': error_count,
            'total_bets': len(unresolved_bets),
            'box_scores_used': len(box_scores)
        }
    
    def _get_unresolved_bets_for_date(self, target_date: date) -> List[Dict]:
        """Get all unresolved bets for a specific date"""
        with self.db.get_connection() as conn:
            # Convert date to string format to match scrape_date
            date_str = target_date.strftime('%Y-%m-%d')
            
            cursor = conn.execute("""
                SELECT p.*, g.status as game_status
                FROM props p
                LEFT JOIN games g ON p.game_id = g.game_id
                LEFT JOIN bet_results br ON p.id = br.prop_id
                WHERE br.id IS NULL 
                  AND p.scrape_date = ?
                ORDER BY p.player_name, p.market
            """, (date_str,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def _create_box_score_lookup(self, box_scores: List[Dict]) -> Dict[str, Dict]:
        """Create efficient lookup dictionary for box scores"""
        lookup = {}
        
        for score in box_scores:
            player_name = score['player_name']
            team = score['team']
            
            # Create normalized key for original name
            key = self._normalize_player_key(player_name, team)
            
            if key not in lookup:
                lookup[key] = []
            
            lookup[key].append(score)
            
            # Also create entries for any betting names that map to this MLB name
            # This allows reverse lookup from betting name to box score
            with self.db.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT betting_name FROM player_name_mapping 
                    WHERE mlb_name = ?
                """, (player_name,))
                
                for mapping_row in cursor.fetchall():
                    betting_name = mapping_row[0]
                    betting_key = self._normalize_player_key(betting_name, team)
                    
                    if betting_key not in lookup:
                        lookup[betting_key] = []
                    
                    lookup[betting_key].append(score)
        
        return lookup
    
    def _get_mapped_player_name(self, betting_name: str, team: str) -> str:
        """Get the MLB name for a betting name using name mappings"""
        with self.db.get_connection() as conn:
            cursor = conn.execute("""
                SELECT mlb_name FROM player_name_mapping 
                WHERE betting_name = ? AND (team = ? OR team IS NULL)
                ORDER BY team DESC  -- Prefer team-specific mappings
                LIMIT 1
            """, (betting_name, team))
            
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                return betting_name  # Return original if no mapping found
    
    def _normalize_player_key(self, player_name: str, team: str) -> str:
        """Create normalized key for player matching"""
        # Remove common suffixes and normalize
        name = player_name.lower().strip()
        name = re.sub(r'\s+(jr\.?|sr\.?|iii?|iv)$', '', name)
        name = re.sub(r'[^\w\s]', '', name)  # Remove punctuation
        name = re.sub(r'\s+', ' ', name)     # Normalize whitespace
        
        return f"{name}_{team.upper()}"
    
    def _resolve_single_bet(self, bet: Dict, box_score_lookup: Dict[str, Dict]) -> bool:
        """Resolve a single bet using box score data"""
        try:
            player_name = bet['player_name']
            team = bet['team']
            market = bet['market']
            
            # Try to find matching box score - first with original name, then with mapped name
            key = self._normalize_player_key(player_name, team)
            
            if key not in box_score_lookup:
                # Try using name mapping
                mapped_name = self._get_mapped_player_name(player_name, team)
                if mapped_name != player_name:
                    key = self._normalize_player_key(mapped_name, team)
                    logger.debug(f"Using name mapping: '{player_name}' -> '{mapped_name}'")
            
            if key not in box_score_lookup:
                logger.debug(f"No box score found for {player_name} ({team}) or mapped name")
                return False
            
            # Get the player's box score (there should only be one for a given date)
            player_box_scores = box_score_lookup[key]
            if not player_box_scores:
                return False
            
            box_score = player_box_scores[0]  # Use first match
            
            # Calculate actual result based on market
            actual_result = self._calculate_actual_result(market, box_score)
            
            if actual_result is None:
                logger.debug(f"Could not calculate result for market '{market}' for {player_name}")
                return False
            
            # Insert bet result
            self.db.insert_bet_result(
                prop_id=bet['id'],
                actual_result=actual_result,
                result_source='box_score_auto',
                result_confidence=0.95,
                notes=f"Auto-resolved using box score data from {box_score['data_source']}"
            )
            
            logger.debug(f"✅ Resolved {player_name} {market}: {actual_result}")
            return True
            
        except Exception as e:
            logger.error(f"Error resolving bet for {bet.get('player_name', 'unknown')}: {e}")
            return False
    
    def _calculate_actual_result(self, market: str, box_score: Dict) -> Optional[float]:
        """Calculate actual result based on market type and box score data"""
        market_lower = market.lower().strip()
        
        # Batting markets
        if 'hit' in market_lower and 'allowed' not in market_lower:
            return float(box_score.get('hits', 0))
            
        elif 'run' in market_lower and 'earned' not in market_lower:
            return float(box_score.get('runs', 0))
            
        elif 'rbi' in market_lower:
            return float(box_score.get('rbi', 0))
            
        elif 'home run' in market_lower or 'homer' in market_lower:
            return float(box_score.get('home_runs', 0))
            
        elif 'double' in market_lower:
            return float(box_score.get('doubles', 0))
            
        elif 'triple' in market_lower:
            return float(box_score.get('triples', 0))
            
        elif 'single' in market_lower:
            return float(box_score.get('singles', 0))
            
        elif 'walk' in market_lower or 'base on balls' in market_lower:
            return float(box_score.get('walks', 0))
            
        elif 'strikeout' in market_lower and 'pitcher' not in market_lower:
            return float(box_score.get('strikeouts', 0))
            
        elif 'stolen base' in market_lower:
            return float(box_score.get('stolen_bases', 0))
            
        elif 'total base' in market_lower:
            return float(box_score.get('total_bases', 0))
            
        # Combined markets
        elif 'hits + runs + rbis' in market_lower or 'h+r+rbi' in market_lower:
            hits = box_score.get('hits', 0)
            runs = box_score.get('runs', 0)
            rbi = box_score.get('rbi', 0)
            return float(hits + runs + rbi)
            
        elif 'hits and runs' in market_lower or 'h+r' in market_lower:
            hits = box_score.get('hits', 0)
            runs = box_score.get('runs', 0)
            return float(hits + runs)
            
        # Pitching markets
        elif 'strikeout' in market_lower and ('pitcher' in market_lower or 'pitching' in market_lower):
            return float(box_score.get('strikeouts_pitched', 0))
            
        elif 'earned run' in market_lower or 'er' in market_lower:
            return float(box_score.get('earned_runs', 0))
            
        elif 'hits allowed' in market_lower:
            return float(box_score.get('hits_allowed', 0))
            
        elif 'walks allowed' in market_lower:
            return float(box_score.get('walks_allowed', 0))
            
        elif 'innings pitched' in market_lower:
            return float(box_score.get('innings_pitched', 0.0))
            
        elif 'pitching outs' in market_lower or 'outs recorded' in market_lower:
            return float(box_score.get('pitching_outs', 0))
            
        # Fielding markets
        elif 'assist' in market_lower:
            return float(box_score.get('fielding_assists', 0))
            
        elif 'putout' in market_lower:
            return float(box_score.get('fielding_putouts', 0))
            
        elif 'error' in market_lower:
            return float(box_score.get('fielding_errors', 0))
        
        # If no match found
        logger.debug(f"Unknown market type: '{market}'")
        return None
    
    def get_resolution_summary(self, target_date: date) -> Dict[str, Any]:
        """Get summary of bet resolutions for a date"""
        with self.db.get_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_resolved,
                    COUNT(CASE WHEN result_source = 'box_score_auto' THEN 1 END) as auto_resolved,
                    COUNT(CASE WHEN result_source != 'box_score_auto' THEN 1 END) as manual_resolved,
                    AVG(result_confidence) as avg_confidence
                FROM bet_results br
                JOIN props p ON br.prop_id = p.id
                WHERE p.scrape_date = ?
            """, (target_date,))
            
            summary = dict(cursor.fetchone())
            
            # Get breakdown by market type
            cursor = conn.execute("""
                SELECT 
                    br.market,
                    COUNT(*) as count,
                    AVG(result_confidence) as avg_confidence
                FROM bet_results br
                JOIN props p ON br.prop_id = p.id
                WHERE p.scrape_date = ?
                  AND result_source = 'box_score_auto'
                GROUP BY br.market
                ORDER BY count DESC
            """, (target_date,))
            
            summary['market_breakdown'] = [dict(row) for row in cursor.fetchall()]
            
            return summary


def main():
    """Run enhanced bet resolver for yesterday"""
    logging.basicConfig(level=logging.INFO)
    
    db = MLBPropsDatabase()
    resolver = EnhancedBetResolver(db)
    
    # Use yesterday's date for bet resolution
    from datetime import timedelta
    yesterday = date.today() - timedelta(days=1)
    
    print(f"Running enhanced bet resolution for {yesterday}")
    
    results = resolver.resolve_bets_for_date(yesterday)
    print(f"Results: {results}")
    
    summary = resolver.get_resolution_summary(yesterday)
    print(f"Summary: {summary}")


if __name__ == "__main__":
    main()
