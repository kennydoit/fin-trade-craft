#!/usr/bin/env python3
"""
Apply the fix for quarterly gap infinite loop issue by modifying the incremental_etl.py file.

This script will update the quarterly gap logic to include a "cooling-off period" that prevents 
symbols from being reprocessed too frequently when they have quarterly gaps.
"""

import os
import re
from pathlib import Path

def apply_quarterly_gap_fix():
    """Apply the quarterly gap infinite loop fix to incremental_etl.py."""
    print("🔧 APPLYING QUARTERLY GAP INFINITE LOOP FIX")
    print("=" * 60)
    
    # Path to the file to fix
    etl_file = Path(__file__).parent / "utils" / "incremental_etl.py"
    
    if not etl_file.exists():
        print(f"❌ File not found: {etl_file}")
        return False
    
    print(f"📂 Modifying file: {etl_file}")
    
    # Read the current file
    with open(etl_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # The problematic logic to find and replace
    old_quarterly_gap_logic = """                    -- Check if there's a quarterly gap or staleness
                    CASE 
                        WHEN ew.last_fiscal_date IS NULL THEN TRUE -- Never processed
                        WHEN ew.last_fiscal_date < (
                            CASE 
                                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL %s
                                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL %s
                                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' + INTERVAL %s
                                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
                                ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '9 months' - INTERVAL '1 day')::date
                            END
                        ) THEN TRUE -- Has quarterly gap
                        WHEN ew.last_successful_run < NOW() - INTERVAL %s THEN TRUE -- Time-based staleness
                        ELSE FALSE
                    END as needs_processing"""
    
    # The improved logic with cooling-off period
    new_quarterly_gap_logic = """                    -- Check if there's a quarterly gap or staleness WITH COOLING-OFF PERIOD
                    CASE 
                        WHEN ew.last_fiscal_date IS NULL THEN TRUE -- Never processed
                        -- Has quarterly gap AND hasn't been processed recently (7-day cooling-off period)
                        WHEN ew.last_fiscal_date < (
                            CASE 
                                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL %s
                                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL %s
                                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' + INTERVAL %s
                                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
                                ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '9 months' - INTERVAL '1 day')::date
                            END
                        ) AND (
                            ew.last_successful_run IS NULL 
                            OR ew.last_successful_run < NOW() - INTERVAL '7 days'
                        ) THEN TRUE -- Has quarterly gap but with 7-day cooling-off period
                        WHEN ew.last_successful_run < NOW() - INTERVAL %s THEN TRUE -- Time-based staleness
                        ELSE FALSE
                    END as needs_processing"""
    
    # Check if the old logic exists
    if old_quarterly_gap_logic not in content:
        print("❌ Could not find the exact quarterly gap logic to replace")
        print("The file may have been modified or the logic has changed")
        return False
    
    # Apply the fix
    new_content = content.replace(old_quarterly_gap_logic, new_quarterly_gap_logic)
    
    # Verify the replacement worked
    if new_content == content:
        print("❌ No changes were made - replacement failed")
        return False
    
    # Create backup
    backup_file = etl_file.with_suffix('.py.backup')
    with open(backup_file, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"📄 Backup created: {backup_file}")
    
    # Write the fixed version
    with open(etl_file, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print("✅ Successfully applied the quarterly gap fix!")
    print()
    print("🎯 WHAT CHANGED:")
    print("• Added 7-day cooling-off period for quarterly gap processing")
    print("• Symbols with quarterly gaps won't reprocess for 7 days after last success")
    print("• This prevents infinite loops when Q3 2025 data isn't available yet")
    print("• Time-based staleness logic remains unchanged")
    print()
    print("📋 NEXT STEPS:")
    print("1. Test with: python data_pipeline/extract/extract_balance_sheet.py --limit 10")
    print("2. Verify the same 524 symbols don't reappear immediately")
    print("3. Run full extraction without infinite loops!")
    
    return True

def verify_fix():
    """Verify that the fix was applied correctly."""
    print(f"\n🔍 VERIFYING FIX")
    print("=" * 60)
    
    etl_file = Path(__file__).parent / "utils" / "incremental_etl.py"
    
    with open(etl_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if the cooling-off period logic exists
    if "7 days" in content and "cooling-off period" in content.lower():
        print("✅ Fix verification successful!")
        print("• Found '7 days' cooling-off period in the code")
        print("• Quarterly gap logic has been updated")
        return True
    else:
        print("❌ Fix verification failed!")
        print("• Could not find expected changes in the file")
        return False

def main():
    """Main function to apply the fix."""
    print("🔄 QUARTERLY GAP INFINITE LOOP FIX APPLICATION")
    print("=" * 60)
    print(f"Timestamp: {datetime.now()}")
    print()
    
    # Apply the fix
    if apply_quarterly_gap_fix():
        # Verify the fix
        if verify_fix():
            print(f"\n🎉 SUCCESSFUL FIX APPLICATION!")
            print("The quarterly gap infinite loop issue has been resolved.")
        else:
            print(f"\n⚠️ FIX APPLIED BUT VERIFICATION FAILED")
            print("Please manually check the changes in utils/incremental_etl.py")
    else:
        print(f"\n❌ FAILED TO APPLY FIX")
        print("Please manually apply the changes or check the file structure")

if __name__ == "__main__":
    from datetime import datetime
    main()
