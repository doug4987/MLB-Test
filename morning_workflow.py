#!/usr/bin/env python3
"""
Daily Morning Workflow
Runs each morning to:
1. Collect today's props → props table
2. Create best props → best_props table  
3. Show today's best props for betting
4. Resolve yesterday's bets using MLB stats
5. Add results to both props and best_props tables
"""
import logging
from datetime import date, timedelta
from typing import Dict, List, Any
import pandas as pd
from database import MLBPropsDatabase
from daily_scraper import DailyMLBScraper
from box_score_scraper import MLBBoxScoreScraper
from enhanced_bet_resolver import EnhancedBetResolver

logger = logging.getLogger(__name__)


class MorningWorkflow:
    """Complete morning workflow for MLB props betting"""
    
    def __init__(self, db_path: str = "mlb_props.db"):
        self.db = MLBPropsDatabase(db_path)
        self.props_scraper = DailyMLBScraper(db_path)
        self.box_score_scraper = MLBBoxScoreScraper(self.db)
        self.bet_resolver = EnhancedBetResolver(self.db)
    
    def run_morning_workflow(self) -> Dict[str, Any]:
        """Run complete morning workflow"""
        today = date.today()
        yesterday = today - timedelta(days=1)
        
        logger.info("🌅 STARTING DAILY MORNING WORKFLOW")
        logger.info("=" * 50)
        
        workflow_results = {
            'date': today.isoformat(),
            'started_at': f"{today} morning workflow"
        }
        
        # STEP 1: Collect Today's Props
        logger.info("📈 STEP 1: Collecting today's props...")
        props_results = self._collect_todays_props(today)
        workflow_results['props_collection'] = props_results
        
        # STEP 2: Create Best Props Table
        logger.info("🏆 STEP 2: Creating best props table...")
        best_props_results = self._create_best_props_table(today)
        workflow_results['best_props_creation'] = best_props_results
        
        # STEP 3: Show Today's Best Props for Betting
        logger.info("💰 STEP 3: Generating today's betting recommendations...")
        betting_recommendations = self._generate_betting_recommendations(today)
        workflow_results['betting_recommendations'] = betting_recommendations
        
        # STEP 4: Collect Yesterday's Box Scores
        logger.info("📊 STEP 4: Collecting yesterday's MLB stats...")
        box_score_results = self._collect_box_scores(yesterday)
        workflow_results['box_score_collection'] = box_score_results
        
        # STEP 5: Resolve Yesterday's Bets
        logger.info("🎲 STEP 5: Resolving yesterday's bet results...")
        resolution_results = self._resolve_yesterday_bets(yesterday)
        workflow_results['bet_resolution'] = resolution_results
        
        # STEP 6: Generate Summary Report
        logger.info("📋 STEP 6: Generating summary report...")
        summary = self._generate_daily_summary(workflow_results)
        workflow_results['summary'] = summary
        
        logger.info("✅ MORNING WORKFLOW COMPLETE!")
        logger.info("=" * 50)
        
        return workflow_results
    
    def _collect_todays_props(self, today: date) -> Dict[str, Any]:
        """Collect today's props and store in props table"""
        try:
            results = self.props_scraper.run_daily_scrape(today)
            
            logger.info(f"✅ Props collected: {results.get('props_scraped', {}).get('total_records', 0)}")
            return results
            
        except Exception as e:
            logger.error(f"❌ Props collection failed: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    def _create_best_props_table(self, today: date) -> Dict[str, Any]:
        """Create best props table from today's props"""
        try:
            from create_best_odds_table import populate_best_odds_table
            
            date_str = today.strftime('%Y-%m-%d')
            best_count = populate_best_odds_table(date_str)
            
            logger.info(f"✅ Best props created: {best_count}")
            
            return {
                'date': date_str,
                'best_props_created': best_count,
                'status': 'completed'
            }
            
        except Exception as e:
            logger.error(f"❌ Best props creation failed: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    def _generate_betting_recommendations(self, today: date) -> Dict[str, Any]:
        """Generate and display today's best betting recommendations"""
        try:
            # Get best props for today
            with self.db.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT 
                        player_name, team, market, best_site,
                        over_line, under_line, suggested_bet,
                        expected_value_tier, expected_value_description,
                        implied_projection, batx_projection
                    FROM best_odds 
                    WHERE scrape_date = ?
                    AND expected_value_tier IN ('A', 'B', 'C')
                    ORDER BY 
                        CASE expected_value_tier 
                            WHEN 'A' THEN 1 
                            WHEN 'B' THEN 2 
                            WHEN 'C' THEN 3
                            ELSE 4 
                        END,
                        player_name
                """, (today,))
                
                recommendations = [dict(row) for row in cursor.fetchall()]
            
            # Organize by tier
            tier_a_bets = [r for r in recommendations if r['expected_value_tier'] == 'A']
            tier_b_bets = [r for r in recommendations if r['expected_value_tier'] == 'B']
            tier_c_bets = [r for r in recommendations if r['expected_value_tier'] == 'C']
            
            logger.info(f"💎 TIER A BETS (Excellent Value): {len(tier_a_bets)}")
            for i, bet in enumerate(tier_a_bets[:10], 1):  # Show top 10
                logger.info(f"  {i:2d}. {bet['player_name']} ({bet['team']}) - {bet['market']}")
                logger.info(f"      {bet['suggested_bet']} {bet['over_line']} @ {bet['best_site']}")
            
            logger.info(f"🥇 TIER B BETS (Very Good Value): {len(tier_b_bets)}")
            for i, bet in enumerate(tier_b_bets[:10], 1):  # Show top 10
                logger.info(f"  {i:2d}. {bet['player_name']} ({bet['team']}) - {bet['market']}")
                logger.info(f"      {bet['suggested_bet']} {bet['over_line']} @ {bet['best_site']}")
            
            logger.info(f"🥈 TIER C BETS (Good Value): {len(tier_c_bets)}")
            for i, bet in enumerate(tier_c_bets[:5], 1):  # Show top 5
                logger.info(f"  {i:2d}. {bet['player_name']} ({bet['team']}) - {bet['market']}")
                logger.info(f"      {bet['suggested_bet']} {bet['over_line']} @ {bet['best_site']}")

                # Export to CSV
            df = pd.DataFrame(recommendations)
            if not df.empty:
                csv_filename = f"betting_recommendations_{today.isoformat()}.csv"
                df.to_csv(csv_filename, index=False)
                logger.info(f"📁 Recommendations exported to CSV: {csv_filename}")
            else:
                csv_filename = None
                logger.info("ℹ️ No recommendations to export; DataFrame is empty.")

            return {
                'tier_a_count': len(tier_a_bets),
                'tier_b_count': len(tier_b_bets),
                'tier_c_count': len(tier_c_bets),
                'total_recommendations': len(recommendations),
                'export_file': csv_filename,
                'status': 'completed'
                }
        except Exception as e:
            logger.error(f"❌ Betting recommendations failed: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    def _collect_box_scores(self, yesterday: date) -> Dict[str, Any]:
        """Collect box scores for yesterday's completed games"""
        try:
            results = self.box_score_scraper.collect_box_scores_for_date(yesterday)
            
            logger.info(f"✅ Box scores: {results.get('games_processed', 0)} games, {results.get('players_collected', 0)} players")
            return results
            
        except Exception as e:
            logger.error(f"❌ Box score collection failed: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    def _resolve_yesterday_bets(self, yesterday: date) -> Dict[str, Any]:
        """Resolve yesterday's bets using box score data"""
        try:
            # Resolve bets in props table
            props_results = self.bet_resolver.resolve_bets_for_date(yesterday)
            
            # Also resolve bets in best_odds table
            best_props_results = self._resolve_best_props_bets(yesterday)
            
            total_resolved = props_results.get('resolved', 0) + best_props_results.get('resolved', 0)
            
            logger.info(f"✅ Bets resolved: {props_results.get('resolved', 0)} props + {best_props_results.get('resolved', 0)} best props = {total_resolved} total")
            
            return {
                'props_resolved': props_results,
                'best_props_resolved': best_props_results,
                'total_resolved': total_resolved
            }
            
        except Exception as e:
            logger.error(f"❌ Bet resolution failed: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    def _resolve_best_props_bets(self, yesterday: date) -> Dict[str, Any]:
        """Resolve bets from best_odds table"""
        try:
            # Get unresolved best props for yesterday
            with self.db.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT bo.*, bo.original_prop_id
                    FROM best_odds bo
                    LEFT JOIN bet_results br ON bo.original_prop_id = br.prop_id
                    WHERE br.id IS NULL 
                      AND bo.scrape_date = ?
                    ORDER BY bo.player_name, bo.market
                """, (yesterday,))
                
                unresolved_best_props = [dict(row) for row in cursor.fetchall()]
            
            if not unresolved_best_props:
                return {'resolved': 0, 'errors': 0, 'message': 'No unresolved best props'}
            
            # Get box scores for matching
            box_scores = self.db.get_box_scores_for_date(yesterday)
            if not box_scores:
                return {'resolved': 0, 'errors': 0, 'message': 'No box scores available'}
            
            # Use the same resolution logic as enhanced bet resolver
            box_score_lookup = self.bet_resolver._create_box_score_lookup(box_scores)
            
            resolved_count = 0
            error_count = 0
            
            for best_prop in unresolved_best_props:
                try:
                    # Use original prop ID if available, otherwise create a dummy record
                    prop_data = {
                        'id': best_prop.get('original_prop_id') or best_prop['id'],
                        'player_name': best_prop['player_name'],
                        'team': best_prop['team'],
                        'market': best_prop['market']
                    }
                    
                    success = self.bet_resolver._resolve_single_bet(prop_data, box_score_lookup)
                    if success:
                        resolved_count += 1
                    else:
                        error_count += 1
                        
                except Exception as e:
                    logger.debug(f"Error resolving best prop {best_prop.get('id', 'unknown')}: {e}")
                    error_count += 1
            
            return {
                'resolved': resolved_count,
                'errors': error_count,
                'total_best_props': len(unresolved_best_props)
            }
            
        except Exception as e:
            logger.error(f"Error resolving best props: {e}")
            return {'resolved': 0, 'errors': 1, 'error': str(e)}
    
    def _generate_daily_summary(self, workflow_results: Dict) -> Dict[str, Any]:
        """Generate comprehensive daily summary"""
        try:
            today = date.today()
            yesterday = today - timedelta(days=1)
            
            # Get counts from workflow results
            props_collected = workflow_results.get('props_collection', {}).get('props_scraped', {}).get('total_records', 0)
            best_props_created = workflow_results.get('best_props_creation', {}).get('best_props_created', 0)
            box_scores_collected = workflow_results.get('box_score_collection', {}).get('players_collected', 0)
            bets_resolved = workflow_results.get('bet_resolution', {}).get('total_resolved', 0)
            
            # Get betting recommendations
            recommendations = workflow_results.get('betting_recommendations', {})
            tier_a_count = recommendations.get('tier_a_count', 0)
            tier_b_count = recommendations.get('tier_b_count', 0)
            tier_c_count = recommendations.get('tier_c_count', 0)
            
            summary = {
                'date': today.isoformat(),
                'workflow_status': 'completed',
                
                # Today's Data
                'todays_props': {
                    'total_collected': props_collected,
                    'best_props_created': best_props_created,
                    'tier_a_recommendations': tier_a_count,
                    'tier_b_recommendations': tier_b_count,
                    'tier_c_recommendations': tier_c_count
                },
                
                # Yesterday's Resolution
                'yesterdays_resolution': {
                    'date': yesterday.isoformat(),
                    'box_scores_collected': box_scores_collected,
                    'bets_resolved': bets_resolved
                },
                
                # Action Items
                'action_items': {
                    'betting_file': recommendations.get('export_file'),
                    'tier_a_bets_to_place': tier_a_count,
                    'tier_b_bets_to_consider': tier_b_count,
                    'tier_c_bets_to_consider': tier_c_count
                }
            }
            
            # Print summary
            logger.info("📊 DAILY SUMMARY")
            logger.info("-" * 30)
            logger.info(f"📅 Date: {today}")
            logger.info(f"📈 Props collected: {props_collected:,}")
            logger.info(f"🏆 Best props created: {best_props_created:,}")
            logger.info(f"💎 Tier A bets (EXCELLENT): {tier_a_count}")
            logger.info(f"🥇 Tier B bets (VERY GOOD): {tier_b_count}")
            logger.info(f"🥈 Tier C bets (GOOD): {tier_c_count}")
            logger.info(f"📊 Box scores from {yesterday}: {box_scores_collected}")
            logger.info(f"🎲 Bets resolved from {yesterday}: {bets_resolved}")
            
            if tier_a_count > 0 or tier_b_count > 0 or tier_c_count > 0:
                logger.info(f"🎯 ACTION: Check {recommendations.get('export_file')} for today's best bets!")
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return {'status': 'error', 'error': str(e)}


def main():
    """Run the morning workflow"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    workflow = MorningWorkflow()
    results = workflow.run_morning_workflow()
    
    print("\n" + "="*60)
    print("🌅 MORNING WORKFLOW COMPLETE!")
    print("="*60)
    
    summary = results.get('summary', {})
    if summary.get('action_items', {}).get('betting_file'):
        print(f"\n🌅 MORNING WORKFLOW DONE. Check {results.get('betting_recommendations',{}).get('export_file')} for bets.")
    return results

if __name__ == "__main__":
    main()
