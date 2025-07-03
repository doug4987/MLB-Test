#!/usr/bin/env python3
"""
Daily MLB Props Automation Master Script
Runs the complete workflow every morning at 9am
"""
import os
import sys
import subprocess
import logging
from datetime import datetime, date
import time

# Setup logging
def setup_logging():
    """Setup logging for the daily automation"""
    log_dir = "/Users/doug/06-27 Full Working MLB Prop System/logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f"daily_automation_{date.today().strftime('%Y-%m-%d')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

def run_command(command, description, logger, timeout=3600):
    """Run a command with logging and error handling"""
    logger.info(f"🔄 Starting: {description}")
    logger.info(f"Command: {command}")
    
    try:
        start_time = time.time()
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True, 
            timeout=timeout,
            cwd="/Users/doug/06-27 Full Working MLB Prop System"
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        if result.returncode == 0:
            logger.info(f"✅ Completed: {description} (took {duration:.1f}s)")
            if result.stdout:
                logger.info(f"Output: {result.stdout.strip()}")
            return True
        else:
            logger.error(f"❌ Failed: {description}")
            logger.error(f"Exit code: {result.returncode}")
            logger.error(f"Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ Timeout: {description} (exceeded {timeout}s)")
        return False
    except Exception as e:
        logger.error(f"💥 Exception in {description}: {e}")
        return False

def send_notification(title, message, logger):
    """Send macOS notification"""
    try:
        cmd = f'osascript -e \'display notification "{message}" with title "{title}"\''
        subprocess.run(cmd, shell=True, capture_output=True)
        logger.info(f"📱 Notification sent: {title}")
    except Exception as e:
        logger.warning(f"Failed to send notification: {e}")

def daily_workflow():
    """Run the complete daily MLB props workflow"""
    
    logger = setup_logging()
    current_date = date.today().strftime('%Y-%m-%d')
    
    logger.info("🚀 STARTING DAILY MLB PROPS AUTOMATION")
    logger.info("=" * 60)
    logger.info(f"Date: {current_date}")
    logger.info(f"Time: {datetime.now().strftime('%H:%M:%S')}")
    logger.info(f"Working Directory: /Users/doug/06-27 Full Working MLB Prop System")
    
    # Send start notification
    send_notification(
        "MLB Props Automation", 
        f"Daily workflow started for {current_date}", 
        logger
    )
    
    workflow_steps = [
        {
            "command": "python3 morning_workflow.py",
            "description": "2. Create Best Props & Odds",
            "timeout": 7200,   # 2 Hours
            "critical": True
        },
        {
            "command": "python3 update_plus_ev_workflow.py",
            "description": "3. Create +EV Bets Table",
            "timeout": 300,   # 5 minutes
            "critical": True
        },
        {
            "command": "python3 box_score_scraper.py",
            "description": "4. Collect Box Scores",
            "timeout": 1800,  # 30 minutes
            "critical": False  # Can fail if games not completed yet
        },
        {
            "command": "python3 enhanced_bet_resolver.py",
            "description": "5. Resolve Bets",
            "timeout": 600,   # 10 minutes
            "critical": False  # Can fail if no box scores available
        },
        {
            "command": "python3 calculate_tiered_betting_roi.py",
            "description": "6. Calculate ROI & Generate Reports",
            "timeout": 300,   # 5 minutes
            "critical": False  # Can fail if no resolved bets
        }
    ]
    
    success_count = 0
    total_steps = len(workflow_steps)
    failed_steps = []
    
    for step in workflow_steps:
        success = run_command(
            step["command"], 
            step["description"], 
            logger, 
            step["timeout"]
        )
        
        if success:
            success_count += 1
        else:
            failed_steps.append(step["description"])
            if step["critical"]:
                logger.error(f"💀 Critical step failed: {step['description']}")
                logger.error("🛑 Stopping workflow due to critical failure")
                break
            else:
                logger.warning(f"⚠️  Non-critical step failed: {step['description']}")
                logger.info("▶️  Continuing with remaining steps...")
    
    # Final summary
    logger.info("📊 DAILY WORKFLOW SUMMARY")
    logger.info("=" * 40)
    logger.info(f"Completed Steps: {success_count}/{total_steps}")
    logger.info(f"Success Rate: {(success_count/total_steps)*100:.1f}%")
    
    if failed_steps:
        logger.warning(f"Failed Steps: {', '.join(failed_steps)}")
    
    # Send completion notification
    if success_count == total_steps:
        status = "✅ All steps completed successfully!"
        notification_msg = f"All {total_steps} steps completed successfully"
    elif success_count >= 3:  # At least core steps completed
        status = f"⚠️  Partial success: {success_count}/{total_steps} steps completed"
        notification_msg = f"{success_count}/{total_steps} steps completed"
    else:
        status = f"❌ Workflow failed: Only {success_count}/{total_steps} steps completed"
        notification_msg = f"Workflow failed - only {success_count} steps completed"
    
    logger.info(status)
    
    send_notification(
        "MLB Props Automation Complete", 
        notification_msg, 
        logger
    )
    
    logger.info(f"🏁 Daily automation completed at {datetime.now().strftime('%H:%M:%S')}")
    logger.info(f"📁 Full log saved to: logs/daily_automation_{current_date}.log")
    
    return success_count == total_steps

if __name__ == "__main__":
    try:
        success = daily_workflow()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n🛑 Daily automation interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"💥 Unexpected error in daily automation: {e}")
        sys.exit(1)
