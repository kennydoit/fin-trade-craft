"""
Script to carefully integrate adaptive rate limiter into earnings call transcripts extractor.
This avoids the complex regex replacements that caused corruption.
"""

def update_earnings_call_transcripts():
    """Update earnings call transcripts extractor with targeted changes."""
    
    file_path = r"c:\Users\Kenrm\repositories\fin-trade-craft\data_pipeline\extract\extract_earnings_call_transcripts.py"
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. Update imports - add adaptive rate limiter import
    if "from utils.adaptive_rate_limiter import AdaptiveRateLimiter, ExtractorType" not in content:
        content = content.replace(
            "from utils.incremental_etl import DateUtils, ContentHasher, WatermarkManager, RunIdGenerator",
            "from utils.incremental_etl import DateUtils, ContentHasher, WatermarkManager, RunIdGenerator\nfrom utils.adaptive_rate_limiter import AdaptiveRateLimiter, ExtractorType"
        )
    
    # 2. Remove API_DELAY_SECONDS constant
    content = content.replace("API_DELAY_SECONDS = 0.8  # Alpha Vantage rate limiting\n", "")
    
    # 3. Update class docstring
    content = content.replace(
        "Modern earnings call transcripts extractor with watermarks and incremental processing.",
        "Modern earnings call transcripts extractor with adaptive rate limiting and incremental processing."
    )
    
    # 4. Add rate limiter to constructor
    if "self.rate_limiter = AdaptiveRateLimiter" not in content:
        # Find the end of __init__ method before the closing bracket
        init_end = content.find("self.base_url = \"https://www.alphavantage.co/query\"")
        if init_end != -1:
            insertion_point = content.find("\n", init_end) + 1
            rate_limiter_code = """        
        # Initialize adaptive rate limiter for earnings calls (very heavy text processing)
        self.rate_limiter = AdaptiveRateLimiter(ExtractorType.EARNINGS_CALLS, verbose=True)
"""
            content = content[:insertion_point] + rate_limiter_code + content[insertion_point:]
    
    # 5. Add rate limiting to API call
    api_call_pattern = "url = f'{self.base_url}?function={STOCK_API_FUNCTION}&symbol={symbol}&quarter={quarter}&apikey={self.api_key}'\n        \n        try:"
    if api_call_pattern in content:
        replacement = """url = f'{self.base_url}?function={STOCK_API_FUNCTION}&symbol={symbol}&quarter={quarter}&apikey={self.api_key}'
        
        # Adaptive rate limiting - smart delay based on elapsed time and processing overhead
        self.rate_limiter.pre_api_call()
        
        try:"""
        content = content.replace(api_call_pattern, replacement)
    
    # 6. Replace fixed rate limiting with performance reporting in main loop
    old_rate_limit = "            # Rate limiting\n            time.sleep(API_DELAY_SECONDS)"
    new_performance = """            # Notify rate limiter about API call result
            api_status = 'success' if status == 'success' else ('rate_limited' if 'rate' in str(status).lower() else 'error')
            self.rate_limiter.post_api_call(api_status)"""
    
    if old_rate_limit in content:
        content = content.replace(old_rate_limit, new_performance)
    
    # 7. Fix time estimate calculation that uses API_DELAY_SECONDS
    content = content.replace(
        "estimated_time = (total_api_calls * API_DELAY_SECONDS) / 60",
        "estimated_time = (total_api_calls * 0.5) / 60  # Estimated with adaptive rate limiting"
    )
    
    # 8. Update module docstring
    content = content.replace(
        "Uses source schema, watermarks, content hashing, and deterministic processing.",
        "Uses source schema, watermarks, content hashing, and adaptive rate limiting for optimal performance."
    )
    
    # Write the updated content
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("✅ Updated earnings call transcripts extractor with adaptive rate limiting")

if __name__ == "__main__":
    update_earnings_call_transcripts()
