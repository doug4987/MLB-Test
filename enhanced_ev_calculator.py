#!/usr/bin/env python3
"""
Enhanced Expected Value Calculator
Calculates actual mathematical EV from projections and odds, not just image detection
"""
import re
import logging
from typing import Optional, Dict, Tuple
import math

logger = logging.getLogger(__name__)


class EnhancedEVCalculator:
    """Calculate proper expected value from projections and odds"""
    
    def __init__(self):
        self.ev_tier_thresholds = {
            'A': 0.08,   # 8%+ EV = Excellent
            'B': 0.04,   # 4-8% EV = Good  
            'C': 0.02,   # 2-4% EV = Fair
            'D': 0.01,   # 1-2% EV = Marginal
        }
    
    def parse_odds(self, odds_string: str) -> Optional[float]:
        """Parse odds string to decimal probability"""
        if not odds_string or odds_string.strip() == '':
            return None
            
        # First check for odds inside parentheses like "2.5 (-110)"
        match = re.search(r'\(([+-]?\d+)\)', odds_string)
        if match:
            odds_clean = match.group(1)
        else:
            # Clean the odds string
            odds_clean = odds_string.strip().replace('(', '').replace(')', '')
        
        # Handle American odds format (+150, -110, etc.)
        if odds_clean.startswith(('+', '-')):
            try:
                odds_value = int(odds_clean)
                if odds_value > 0:
                    # Positive odds: +150 means you win $150 on $100 bet
                    implied_prob = 100 / (odds_value + 100)
                else:
                    # Negative odds: -110 means you bet $110 to win $100
                    implied_prob = abs(odds_value) / (abs(odds_value) + 100)
                return implied_prob
            except ValueError:
                pass
        
        # Handle decimal odds (1.50, 2.00, etc.)
        try:
            decimal_odds = float(odds_clean)
            if decimal_odds > 1:
                implied_prob = 1 / decimal_odds
                return implied_prob
        except ValueError:
            pass
        
        logger.debug(f"Could not parse odds: {odds_string}")
        return None
    
    def calculate_ev_for_over_bet(self, batx_projection: float, line: float, 
                                 over_odds: str) -> Optional[float]:
        """Calculate EV for an OVER bet"""
        odds_prob = self.parse_odds(over_odds)
        if odds_prob is None:
            return None
        
        # Probability that actual result > line based on BatX projection
        # For now, use simple comparison - could be enhanced with distributions
        true_prob_over = self.estimate_prob_over_line(batx_projection, line)
        if true_prob_over is None:
            return None
        
        # Calculate expected value
        # EV = (True_Prob_Win * Payout) - (Prob_Lose * Stake)
        # Payout = (1/odds_prob - 1), Stake = 1
        payout_ratio = (1 / odds_prob) - 1
        ev = (true_prob_over * payout_ratio) - ((1 - true_prob_over) * 1)
        
        return ev
    
    def calculate_ev_for_under_bet(self, batx_projection: float, line: float,
                                  under_odds: str) -> Optional[float]:
        """Calculate EV for an UNDER bet"""
        odds_prob = self.parse_odds(under_odds)
        if odds_prob is None:
            return None
        
        # Probability that actual result < line based on BatX projection  
        true_prob_under = self.estimate_prob_under_line(batx_projection, line)
        if true_prob_under is None:
            return None
        
        # Calculate expected value
        payout_ratio = (1 / odds_prob) - 1
        ev = (true_prob_under * payout_ratio) - ((1 - true_prob_under) * 1)
        
        return ev
    
    def estimate_prob_over_line(self, projection: float, line: float) -> Optional[float]:
        """Estimate probability of going over the line based on projection"""
        if projection is None or line is None:
            return None
        
        # Simple model: if projection is significantly above line, high prob
        diff = projection - line
        
        if diff >= 0.5:
            return 0.75  # High confidence over
        elif diff >= 0.2:
            return 0.65  # Good confidence over
        elif diff >= 0.05:
            return 0.58  # Slight edge over
        elif diff >= -0.05:
            return 0.52  # Very slight edge over
        elif diff >= -0.2:
            return 0.45  # Slight edge under
        else:
            return 0.35  # Strong edge under
    
    def estimate_prob_under_line(self, projection: float, line: float) -> Optional[float]:
        """Estimate probability of going under the line based on projection"""
        prob_over = self.estimate_prob_over_line(projection, line)
        return 1 - prob_over if prob_over is not None else None
    
    def calculate_prop_ev(self, batx_projection: float, implied_projection: float,
                         line_value: float, over_odds: str, under_odds: str,
                         suggested_bet: str) -> Dict:
        """Calculate EV for a prop bet"""
        result = {
            'calculated_ev': None,
            'ev_percentage': None,
            'ev_tier': 'Unknown',
            'ev_description': 'Could not calculate EV',
            'calculation_method': 'mathematical',
            'bet_side': suggested_bet,
            'confidence': 'low'
        }
        
        try:
            # Determine which bet to calculate EV for
            if suggested_bet and 'OVER' in suggested_bet.upper():
                ev = self.calculate_ev_for_over_bet(batx_projection, line_value, over_odds)
                result['bet_side'] = 'OVER'
            elif suggested_bet and 'UNDER' in suggested_bet.upper():
                ev = self.calculate_ev_for_under_bet(batx_projection, line_value, under_odds)
                result['bet_side'] = 'UNDER'
            else:
                # Auto-determine best bet
                ev_over = self.calculate_ev_for_over_bet(batx_projection, line_value, over_odds)
                ev_under = self.calculate_ev_for_under_bet(batx_projection, line_value, under_odds)
                
                if ev_over is not None and ev_under is not None:
                    if ev_over > ev_under:
                        ev = ev_over
                        result['bet_side'] = 'OVER'
                    else:
                        ev = ev_under
                        result['bet_side'] = 'UNDER'
                elif ev_over is not None:
                    ev = ev_over
                    result['bet_side'] = 'OVER'
                elif ev_under is not None:
                    ev = ev_under
                    result['bet_side'] = 'UNDER'
                else:
                    ev = None
            
            if ev is not None:
                result['calculated_ev'] = ev
                result['ev_percentage'] = ev * 100
                
                # Classify into tiers
                abs_ev = abs(ev)
                if abs_ev >= self.ev_tier_thresholds['A']:
                    result['ev_tier'] = 'A'
                    result['ev_description'] = 'Excellent Expected Value'
                    result['confidence'] = 'high'
                elif abs_ev >= self.ev_tier_thresholds['B']:
                    result['ev_tier'] = 'B' 
                    result['ev_description'] = 'Good Expected Value'
                    result['confidence'] = 'medium'
                elif abs_ev >= self.ev_tier_thresholds['C']:
                    result['ev_tier'] = 'C'
                    result['ev_description'] = 'Fair Expected Value'
                    result['confidence'] = 'medium'
                elif abs_ev >= self.ev_tier_thresholds['D']:
                    result['ev_tier'] = 'D'
                    result['ev_description'] = 'Marginal Expected Value'
                    result['confidence'] = 'low'
                else:
                    result['ev_tier'] = 'E'
                    result['ev_description'] = 'Poor Expected Value'
                    result['confidence'] = 'low'
                
                # Negative EV gets downgraded
                if ev < 0:
                    result['ev_tier'] = 'E'
                    result['ev_description'] = 'Negative Expected Value'
                    result['confidence'] = 'low'
        
        except Exception as e:
            logger.debug(f"EV calculation error: {e}")
            result['ev_description'] = f'Calculation error: {e}'
        
        return result
    
    def extract_line_value(self, line_string: str) -> Optional[float]:
        """Extract numeric line value from string like '0.5 (+150)'"""
        if not line_string:
            return None
        
        # Extract the first number before any parentheses or other characters
        match = re.search(r'([0-9]*\.?[0-9]+)', line_string.strip())
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        
        return None


def test_ev_calculator():
    """Test the EV calculator with our examples"""
    calc = EnhancedEVCalculator()

    # Unit-style tests for parse_odds
    assert abs(calc.parse_odds("2.5 (-110)") - (110 / 210)) < 1e-6
    assert abs(calc.parse_odds("(-105)") - (105 / 205)) < 1e-6
    
    print("🧮 TESTING ENHANCED EV CALCULATOR")
    print("=" * 50)
    
    # Test Cole Young Stolen Bases on Fliff
    print("📊 Cole Young Stolen Bases on Fliff:")
    print("  BatX: 0.1, Implied: 0.07")
    print("  Over 0.5 (+1420), Under 0.5 (-4000)")
    
    cole_ev = calc.calculate_prop_ev(
        batx_projection=0.1,
        implied_projection=0.07,
        line_value=0.5,
        over_odds="+1420",
        under_odds="-4000", 
        suggested_bet="OVER"
    )
    
    print(f"  📈 Calculated EV: {cole_ev['ev_percentage']:.2f}%")
    print(f"  📈 EV Tier: {cole_ev['ev_tier']} - {cole_ev['ev_description']}")
    print(f"  📈 Bet Side: {cole_ev['bet_side']}")
    print()
    
    # Test Janson Junk Strikeouts on Fanatics
    print("📊 Janson Junk Strikeouts on Fanatics:")
    print("  BatX: 3.37, Implied: 3.73")
    print("  Over 3.5 (-130), Under 3.5 (+100)")
    
    junk_ev = calc.calculate_prop_ev(
        batx_projection=3.37,
        implied_projection=3.73,
        line_value=3.5,
        over_odds="-130",
        under_odds="+100",
        suggested_bet="UNDER"
    )
    
    print(f"  📈 Calculated EV: {junk_ev['ev_percentage']:.2f}%")
    print(f"  📈 EV Tier: {junk_ev['ev_tier']} - {junk_ev['ev_description']}")
    print(f"  📈 Bet Side: {junk_ev['bet_side']}")
    
    return cole_ev, junk_ev


if __name__ == "__main__":
    test_ev_calculator()
