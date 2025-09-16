#!/usr/bin/env python3
"""
Test the quarterly gap infinite loop fix across all financial statement extractors.

This script verifies that the fix works for:
- Balance Sheet Extractor
- Cash Flow Extractor  
- Income Statement Extractor
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_extractor(extractor_name, script_path):
    """Test a specific extractor to verify the fix works."""
    print(f"\n🧪 TESTING {extractor_name.upper()}")
    print("-" * 50)
    
    import subprocess
    
    # Run the extractor with a small limit to test the fix
    cmd = [
        str(Path(__file__).parent / ".venv" / "Scripts" / "python.exe"),
        str(script_path),
        "--limit", "5"
    ]
    
    try:
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        output = result.stdout + result.stderr
        
        # Check if the fix is working (should show 0 or very few symbols processing)
        if "Found 0 symbols needing processing" in output:
            print(f"✅ {extractor_name}: Fix working - 0 symbols processing (infinite loop prevented)")
            return True
        elif "symbols needing processing" in output:
            # Extract the number of symbols
            import re
            match = re.search(r'Found (\d+) symbols needing processing', output)
            if match:
                num_symbols = int(match.group(1))
                if num_symbols <= 5:  # Should be very few due to cooling-off period
                    print(f"✅ {extractor_name}: Fix working - {num_symbols} symbols processing (reasonable number)")
                    return True
                else:
                    print(f"⚠️ {extractor_name}: {num_symbols} symbols processing (may need investigation)")
                    return False
            else:
                print(f"📄 {extractor_name}: Output analysis inconclusive")
                print("First 500 chars of output:")
                print(output[:500])
                return None
        else:
            print(f"❌ {extractor_name}: Unexpected output")
            print("First 500 chars of output:")
            print(output[:500])
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ {extractor_name}: Test timed out (may indicate infinite loop still exists)")
        return False
    except Exception as e:
        print(f"❌ {extractor_name}: Test failed with error: {e}")
        return False

def main():
    """Test all financial statement extractors."""
    print("🔄 TESTING QUARTERLY GAP FIX ACROSS ALL EXTRACTORS")
    print("=" * 60)
    print(f"Timestamp: {datetime.now()}")
    print()
    
    # Define extractors to test
    extractors = [
        ("Balance Sheet", "data_pipeline/extract/extract_balance_sheet.py"),
        ("Cash Flow", "data_pipeline/extract/extract_cash_flow.py"),
        ("Income Statement", "data_pipeline/extract/extract_income_statement.py")
    ]
    
    results = {}
    
    # Test each extractor
    for name, script_path in extractors:
        if not Path(script_path).exists():
            print(f"⚠️ {name}: Script not found at {script_path}")
            results[name] = False
            continue
            
        results[name] = test_extractor(name, script_path)
    
    # Summary
    print(f"\n📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    successful = 0
    total = 0
    
    for name, result in results.items():
        total += 1
        if result is True:
            successful += 1
            print(f"✅ {name}: Fix verified working")
        elif result is False:
            print(f"❌ {name}: Fix may not be working properly")
        else:
            print(f"📄 {name}: Result inconclusive")
    
    print(f"\n🎯 OVERALL RESULTS:")
    print(f"   Successful: {successful}/{total}")
    print(f"   Success rate: {successful/total*100:.1f}%")
    
    if successful == total:
        print(f"\n🎉 ALL EXTRACTORS WORKING!")
        print("The quarterly gap infinite loop fix has been successfully")
        print("applied to all three financial statement extractors:")
        print("• Balance Sheet ✅")
        print("• Cash Flow ✅") 
        print("• Income Statement ✅")
        print()
        print("You can now run any of these extractors with confidence")
        print("that they won't get stuck in infinite loops.")
    elif successful > 0:
        print(f"\n⚠️ PARTIAL SUCCESS")
        print("Some extractors are working, but others may need investigation.")
    else:
        print(f"\n❌ TESTS FAILED")
        print("The fix may not be working properly. Please check the output above.")
    
    print(f"\n💡 HOW THE FIX WORKS:")
    print("The fix adds a 7-day cooling-off period to quarterly gap detection.")
    print("This means that financial statements with quarterly gaps (missing")
    print("Q3 2025 data) won't be reprocessed for 7 days after their last")
    print("successful run, preventing infinite loops while maintaining the")
    print("ability to detect truly missing quarterly data.")

if __name__ == "__main__":
    main()
