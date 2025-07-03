#!/usr/bin/env python3
"""
Daily MLB Props Scraper
Main script for daily data collection and bet result resolution
"""
import logging
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Any
from enhanced_ev_calculator import EnhancedEVCalculator
import sys
import os

# Import our modules
from database import MLBPropsDatabase
from working_scraper import parse_expected_value
from result_scraper import MLBResultScraper

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'mlb_scraper_{date.today().isoformat()}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DailyMLBScraper:
    """Main daily scraper orchestrator"""
    
    def __init__(self, db_path: str = "mlb_props.db"):
        self.db = MLBPropsDatabase(db_path)
        self.result_scraper = MLBResultScraper(self.db)
        
    def run_daily_scrape(self, scrape_date: date = None) -> Dict[str, Any]:
        """Run complete daily scraping process"""
        if scrape_date is None:
            scrape_date = date.today()
        
        logger.info(f"Starting daily scrape for {scrape_date}")
        
        # Start scrape session
        session_id = self.db.start_scrape_session(scrape_date)
        
        try:
            # Step 1: Scrape current props data
            props_results = self.scrape_props_data(session_id)
            
            # Step 2: Resolve previous day's bets
            previous_date = scrape_date - timedelta(days=1)
            resolution_results = self.resolve_previous_bets(previous_date)
            
            # Step 3: Generate summary report
            summary = self.generate_daily_summary(scrape_date, props_results, resolution_results)
            
            # Mark session as completed
            self.db.end_scrape_session(
                session_id, 
                "completed",
                props_results.get('total_records', 0),
                props_results.get('pages_processed', 0)
            )
            
            logger.info("Daily scrape completed successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Daily scrape failed: {e}")
            self.db.end_scrape_session(session_id, "failed", error_message=str(e))
            raise
    
    def scrape_props_data(self, session_id: str) -> Dict[str, Any]:
        """Scrape current props data"""
        logger.info("Starting props data scraping...")
        
        try:
            # Initialize the simplified scraper
            scraper = SimplifiedMLBScraper()
            
            # Remove the 2-page limit for production
            scraper.remove_page_limit()
            
            # Run the scraper
            raw_data = scraper.scrape_all_data()
            
            if not raw_data:
                logger.warning("No data scraped")
                return {'total_records': 0, 'pages_processed': 0, 'tier_breakdown': {}}
            
            # Process and enhance the data
            processed_data = self.process_scraped_data(raw_data)
            
            # Insert into database
            records_inserted = self.db.insert_props_data(processed_data, session_id)
            
            # Calculate tier breakdown
            tier_breakdown = self.calculate_tier_breakdown(processed_data)
            
            logger.info(f"Props scraping completed: {records_inserted} records inserted")
            
            return {
                'total_records': records_inserted,
                'pages_processed': len(raw_data) // 250 + 1,
                'tier_breakdown': tier_breakdown,
                'raw_data_count': len(raw_data)
            }
            
        except Exception as e:
            logger.error(f"Props scraping failed: {e}")
            raise
    
    def process_scraped_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Process and enhance scraped data"""
        ev_calculator = EnhancedEVCalculator()
        processed_data = []
        
        for record in raw_data:
            try:
                # Extract projections and odds
                batx_projection = record.get('batx_projection', None)
                implied_projection = record.get('implied_projection', None)
                over_line = record.get('over_line')
                under_line = record.get('under_line')
                suggested_bet = record.get('suggested_bet', '')
                
                if batx_projection and implied_projection and (over_line or under_line):
                    line_value = ev_calculator.extract_line_value(over_line if suggested_bet.upper() == 'OVER' else under_line)
                    
                    # Calculate enhanced EV
                    ev_result = ev_calculator.calculate_prop_ev(
                        batx_projection=batx_projection,
                        implied_projection=implied_projection,
                        line_value=line_value,
                        over_odds=over_line,
                        under_odds=under_line,
                        suggested_bet=suggested_bet
                    )
                    
                    record['ev_calculated'] = ev_result
                    record['ev_tier_parsed'] = ev_result['ev_tier']
                    record['ev_description'] = ev_result['ev_description']

                else:
                    record['ev_tier_parsed'] = 'Unknown'
                    record['ev_description'] = 'Insufficient Data'

                # Add processing timestamp
                record['processing_timestamp'] = datetime.now().isoformat()
                
                processed_data.append(record)
                
            except Exception as e:
                logger.warning(f"Error processing record: {e}")
                continue
        
        return processed_data
    
    def calculate_tier_breakdown(self, data: List[Dict]) -> Dict[str, int]:
        """Calculate Expected Value tier breakdown"""
        tier_counts = {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'None': 0}
        
        for record in data:
            ev_tier = record.get('ev_tier_parsed', 'No data')
            if 'Tier A' in ev_tier:
                tier_counts['A'] += 1
            elif 'Tier B' in ev_tier:
                tier_counts['B'] += 1
            elif 'Tier C' in ev_tier:
                tier_counts['C'] += 1
            elif 'Tier D' in ev_tier:
                tier_counts['D'] += 1
            else:
                tier_counts['None'] += 1
        
        return tier_counts
    
    def resolve_previous_bets(self, previous_date: date) -> Dict[str, Any]:
        """Resolve bets from previous day"""
        logger.info(f"Resolving bets for {previous_date}")
        
        try:
            results = self.result_scraper.resolve_bets_for_date(previous_date)
            logger.info(f"Bet resolution completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Bet resolution failed: {e}")
            return {'resolved': 0, 'errors': 1, 'games_processed': 0}
    
    def generate_daily_summary(self, scrape_date: date, props_results: Dict, 
                              resolution_results: Dict) -> Dict[str, Any]:
        """Generate comprehensive daily summary"""
        
        # Get database summary for the scrape date
        db_summary = self.db.get_daily_summary(scrape_date)
        
        # Get performance analysis
        perf_analysis = self.db.get_performance_analysis(
            start_date=scrape_date - timedelta(days=30),  # Last 30 days
            end_date=scrape_date
        )
        
        summary = {
            'scrape_date': scrape_date.isoformat(),
            'scrape_timestamp': datetime.now().isoformat(),
            
            # Props data summary
            'props_scraped': {
                'total_records': props_results.get('total_records', 0),
                'pages_processed': props_results.get('pages_processed', 0),
                'tier_breakdown': props_results.get('tier_breakdown', {}),
                'unique_players': db_summary.get('unique_players', 0),
                'unique_games': db_summary.get('unique_games', 0),
                'unique_markets': db_summary.get('unique_markets', 0)
            },
            
            # Bet resolution summary
            'bet_resolution': {
                'date_resolved': (scrape_date - timedelta(days=1)).isoformat(),
                'resolved_count': resolution_results.get('resolved', 0),
                'error_count': resolution_results.get('errors', 0),
                'games_processed': resolution_results.get('games_processed', 0)
            },
            
            # Performance analysis
            'performance_analysis': perf_analysis,
            
            # Top tier A bets for today
            'top_tier_a_bets': self.get_top_tier_a_bets(scrape_date)
        }
        
        # Save summary to file
        self.save_daily_summary(summary)
        
        return summary
    
    def get_top_tier_a_bets(self, scrape_date: date, limit: int = 10) -> List[Dict]:
        """Get top Tier A bets for the day"""
        tier_a_bets = self.db.get_props_by_tier(scrape_date, 'A')
        
        # Sort by suggested bet confidence or other criteria
        # For now, just return first 10
        top_bets = []
        for bet in tier_a_bets[:limit]:
            top_bets.append({
                'player_name': bet['player_name'],
                'team': bet['team'],
                'market': bet['market'],
                'over_line': bet['over_line'],
                'under_line': bet['under_line'],
                'suggested_bet': bet['suggested_bet'],
                'implied_projection': bet['implied_projection'],
                'batx_projection': bet['batx_projection'],
                'game': f"{bet['away_team']}@{bet['home_team']}"
            })
        
        return top_bets
    
    def save_daily_summary(self, summary: Dict):
        """Save daily summary to JSON file"""
        filename = f"daily_summary_{summary['scrape_date']}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(summary, f, indent=2)
            logger.info(f"Daily summary saved to {filename}")
            
        except Exception as e:
            logger.error(f"Failed to save daily summary: {e}")
    
    def cleanup_old_data(self, days_to_keep: int = 90):
        """Clean up old data beyond retention period"""
        logger.info(f"Cleaning up data older than {days_to_keep} days")
        
        cutoff_date = date.today() - timedelta(days=days_to_keep)
        
        try:
            with self.db.get_connection() as conn:
                # Clean up old props data
                conn.execute("""
                    DELETE FROM props WHERE scrape_date < ?
                """, (cutoff_date,))
                
                # Clean up old scrape sessions
                conn.execute("""
                    DELETE FROM scrape_sessions WHERE scrape_date < ?
                """, (cutoff_date,))
                
                # Keep bet results (they're valuable for analysis)
                logger.info("Data cleanup completed")
                
        except Exception as e:
            logger.error(f"Data cleanup failed: {e}")


class SimplifiedMLBScraper:
    """Simplified scraper interface for daily use"""
    
    def __init__(self):
        from working_scraper import scrape_basic, init_browser, login_simple
        self.scrape_basic = scrape_basic
        
    def remove_page_limit(self):
        """Remove the 2-page limit for production runs"""
        # This would modify the scraper to run without page limits
        pass
    
    def scrape_all_data(self) -> List[Dict]:
        """Scrape all available data"""
        # Import and run the working scraper
        from working_scraper import scrape_basic
        return scrape_basic()


def main():
    """Main entry point for daily scraper"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Daily MLB Props Scraper')
    parser.add_argument('--date', type=str, help='Scrape date (YYYY-MM-DD)', default=None)
    parser.add_argument('--resolve-only', action='store_true', help='Only resolve bets, skip scraping')
    parser.add_argument('--scrape-only', action='store_true', help='Only scrape props, skip bet resolution')
    parser.add_argument('--cleanup', action='store_true', help='Run data cleanup')
    parser.add_argument('--db-path', type=str, default='mlb_props.db', help='Database path')
    
    args = parser.parse_args()
    
    # Parse date
    if args.date:
        scrape_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        scrape_date = date.today()
    
    # Initialize scraper
    scraper = DailyMLBScraper(args.db_path)
    
    try:
        if args.cleanup:
            scraper.cleanup_old_data()
            return
        
        if args.resolve_only:
            previous_date = scrape_date - timedelta(days=1)
            results = scraper.resolve_previous_bets(previous_date)
            print(f"Bet resolution results: {results}")
            return
        
        if args.scrape_only:
            session_id = scraper.db.start_scrape_session(scrape_date)
            try:
                results = scraper.scrape_props_data(session_id)
                scraper.db.end_scrape_session(session_id, "completed", 
                                            results.get('total_records', 0),
                                            results.get('pages_processed', 0))
                print(f"Scraping results: {results}")
            except Exception as e:
                scraper.db.end_scrape_session(session_id, "failed", error_message=str(e))
                raise
            return
        
        # Run full daily process
        summary = scraper.run_daily_scrape(scrape_date)
        
        print("\n" + "="*60)
        print("DAILY SCRAPE SUMMARY")
        print("="*60)
        print(f"Date: {summary['scrape_date']}")
        print(f"Props Scraped: {summary['props_scraped']['total_records']}")
        print(f"Pages Processed: {summary['props_scraped']['pages_processed']}")
        print(f"Tier A Bets: {summary['props_scraped']['tier_breakdown'].get('A', 0)}")
        print(f"Bets Resolved: {summary['bet_resolution']['resolved_count']}")
        print(f"Resolution Errors: {summary['bet_resolution']['error_count']}")
        print("="*60)
        
        # Show top Tier A bets
        if summary.get('top_tier_a_bets'):
            print("\nTOP TIER A BETS:")
            for i, bet in enumerate(summary['top_tier_a_bets'][:5], 1):
                print(f"{i}. {bet['player_name']} ({bet['team']}) - {bet['market']}")
                print(f"   {bet['game']} | {bet['suggested_bet']} | {bet['over_line']}/{bet['under_line']}")
        
    except KeyboardInterrupt:
        logger.info("Scraper interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
