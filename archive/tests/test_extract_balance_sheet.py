"""
Comprehensive tests for balance sheet extractor.
Tests data processing, API response handling, and new data scenarios.
"""

import pytest
import json
import os
import sys
from datetime import datetime, date
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from data_pipeline.extract.extract_balance_sheet import BalanceSheetExtractor, BALANCE_SHEET_FIELDS
from utils.incremental_etl import DateUtils, ContentHasher, WatermarkManager


class TestBalanceSheetExtractor:
    """Test suite for BalanceSheetExtractor."""
    
    @pytest.fixture
    def sample_api_response(self):
        """Sample API response for testing."""
        return {
            "symbol": "AAPL",
            "annualReports": [
                {
                    "fiscalDateEnding": "2023-12-31",
                    "reportedCurrency": "USD",
                    "totalAssets": "352755000000",
                    "totalCurrentAssets": "143566000000",
                    "cashAndCashEquivalentsAtCarryingValue": "29965000000",
                    "inventory": "6331000000",
                    "totalLiabilities": "290437000000",
                    "totalShareholderEquity": "62146000000",
                    "retainedEarnings": "164038000000"
                },
                {
                    "fiscalDateEnding": "2022-12-31",
                    "reportedCurrency": "USD",
                    "totalAssets": "352583000000",
                    "totalCurrentAssets": "135405000000",
                    "cashAndCashEquivalentsAtCarryingValue": "23646000000",
                    "inventory": "4946000000",
                    "totalLiabilities": "302083000000",
                    "totalShareholderEquity": "50672000000",
                    "retainedEarnings": "5562000000"
                }
            ],
            "quarterlyReports": [
                {
                    "fiscalDateEnding": "2024-03-31",
                    "reportedCurrency": "USD",
                    "totalAssets": "365725000000",
                    "totalCurrentAssets": "147580000000",
                    "cashAndCashEquivalentsAtCarryingValue": "32972000000",
                    "inventory": "7373000000",
                    "totalLiabilities": "298877000000",
                    "totalShareholderEquity": "66848000000",
                    "retainedEarnings": "169148000000"
                }
            ]
        }
    
    @pytest.fixture
    def empty_api_response(self):
        """Empty API response for testing."""
        return {
            "symbol": "NEWCO",
            "annualReports": [],
            "quarterlyReports": []
        }
    
    @pytest.fixture
    def malformed_api_response(self):
        """Malformed API response for testing."""
        return {
            "symbol": "BADCO",
            "annualReports": [
                {
                    "fiscalDateEnding": "invalid-date",
                    "totalAssets": "not-a-number",
                    "reportedCurrency": "USD"
                }
            ],
            "quarterlyReports": []
        }
    
    @pytest.fixture
    def rate_limited_response(self):
        """Rate limited API response for testing."""
        return {
            "Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 25 requests per minute."
        }
    
    @pytest.fixture
    def error_response(self):
        """Error API response for testing."""
        return {
            "Error Message": "Invalid API call. Please retry or visit the documentation."
        }
    
    @pytest.fixture
    def extractor(self):
        """BalanceSheetExtractor instance for testing."""
        with patch.dict(os.environ, {'ALPHAVANTAGE_API_KEY': 'test_key'}):
            return BalanceSheetExtractor()
    
    def test_initialization(self, extractor):
        """Test extractor initialization."""
        assert extractor.api_key == 'test_key'
        assert extractor.table_name == "balance_sheet"
        assert extractor.schema_name == "source"
    
    def test_api_key_missing(self):
        """Test error when API key is missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="ALPHAVANTAGE_API_KEY not found"):
                BalanceSheetExtractor()
    
    @patch('requests.get')
    def test_fetch_api_data_success(self, mock_get, extractor, sample_api_response):
        """Test successful API data fetch."""
        mock_response = Mock()
        mock_response.json.return_value = sample_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        data, status = extractor._fetch_api_data("AAPL")
        
        assert status == "success"
        assert data == sample_api_response
        mock_get.assert_called_once()
    
    @patch('requests.get')
    def test_fetch_api_data_rate_limited(self, mock_get, extractor, rate_limited_response):
        """Test rate limited API response."""
        mock_response = Mock()
        mock_response.json.return_value = rate_limited_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        data, status = extractor._fetch_api_data("AAPL")
        
        assert status == "rate_limited"
        assert "Note" in data
    
    @patch('requests.get')
    def test_fetch_api_data_error(self, mock_get, extractor, error_response):
        """Test error API response."""
        mock_response = Mock()
        mock_response.json.return_value = error_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        data, status = extractor._fetch_api_data("AAPL")
        
        assert status == "error"
        assert "Error Message" in data
    
    @patch('requests.get')
    def test_fetch_api_data_empty(self, mock_get, extractor):
        """Test empty API response."""
        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        data, status = extractor._fetch_api_data("AAPL")
        
        assert status == "empty"
        assert data == {}
    
    @patch('requests.get')
    def test_fetch_api_data_request_exception(self, mock_get, extractor):
        """Test request exception handling."""
        mock_get.side_effect = Exception("Network error")
        
        data, status = extractor._fetch_api_data("AAPL")
        
        assert status == "error"
        assert "error" in data
        assert "Network error" in data["error"]
    
    def test_transform_data_success(self, extractor, sample_api_response):
        """Test successful data transformation."""
        records = extractor._transform_data("AAPL", 1, sample_api_response, "test-run-id")
        
        # Should have 3 records (2 annual + 1 quarterly)
        assert len(records) == 3
        
        # Check annual record
        annual_record = next(r for r in records if r['report_type'] == 'annual' and r['fiscal_date_ending'] == date(2023, 12, 31))
        assert annual_record['symbol'] == "AAPL"
        assert annual_record['symbol_id'] == 1
        assert annual_record['total_assets'] == 352755000000.0
        assert annual_record['reported_currency'] == "USD"
        assert annual_record['content_hash'] is not None
        
        # Check quarterly record
        quarterly_record = next(r for r in records if r['report_type'] == 'quarterly')
        assert quarterly_record['fiscal_date_ending'] == date(2024, 3, 31)
        assert quarterly_record['total_assets'] == 365725000000.0
    
    def test_transform_data_empty(self, extractor, empty_api_response):
        """Test transformation with empty API response."""
        records = extractor._transform_data("NEWCO", 1, empty_api_response, "test-run-id")
        
        assert len(records) == 0
    
    def test_transform_single_report_malformed_date(self, extractor):
        """Test transformation with malformed date."""
        report = {
            "fiscalDateEnding": "invalid-date",
            "totalAssets": "1000000",
            "reportedCurrency": "USD"
        }
        
        record = extractor._transform_single_report("BADCO", 1, report, "annual", "test-run-id")
        
        # Should return None for invalid date
        assert record is None
    
    def test_transform_single_report_valid_conversions(self, extractor):
        """Test value conversion in transformation."""
        report = {
            "fiscalDateEnding": "2023-12-31",
            "totalAssets": "1000000.50",
            "inventory": "None",  # Should convert to None
            "cashAndCashEquivalentsAtCarryingValue": "",  # Should convert to None
            "reportedCurrency": "USD"
        }
        
        record = extractor._transform_single_report("TEST", 1, report, "annual", "test-run-id")
        
        assert record is not None
        assert record['total_assets'] == 1000000.50
        assert record['inventory'] is None
        assert record['cash_and_cash_equivalents_at_carrying_value'] is None
        assert record['fiscal_date_ending'] == date(2023, 12, 31)
    
    def test_content_has_changed_new_content(self, extractor):
        """Test content change detection for new content."""
        mock_db = Mock()
        mock_db.fetch_query.return_value = [(0,)]  # No existing records
        
        result = extractor._content_has_changed(mock_db, 1, "new-hash")
        
        assert result is True
        mock_db.fetch_query.assert_called_once()
    
    def test_content_has_changed_existing_content(self, extractor):
        """Test content change detection for existing content."""
        mock_db = Mock()
        mock_db.fetch_query.return_value = [(1,)]  # Existing record found
        
        result = extractor._content_has_changed(mock_db, 1, "existing-hash")
        
        assert result is False
    
    def test_upsert_records_success(self, extractor):
        """Test successful record upsert."""
        mock_db = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 2
        mock_db.connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        records = [
            {
                'symbol_id': 1,
                'symbol': 'TEST',
                'fiscal_date_ending': date(2023, 12, 31),
                'report_type': 'annual',
                'total_assets': 1000000.0,
                'content_hash': 'test-hash',
                'source_run_id': 'test-run-id',
                'fetched_at': datetime.now(),
                'api_response_status': 'pass'
            }
        ]
        
        rows_affected = extractor._upsert_records(mock_db, records)
        
        assert rows_affected == 2
        mock_cursor.executemany.assert_called_once()
        mock_db.connection.commit.assert_called_once()
    
    def test_upsert_records_empty_list(self, extractor):
        """Test upsert with empty records list."""
        mock_db = Mock()
        
        rows_affected = extractor._upsert_records(mock_db, [])
        
        assert rows_affected == 0
        mock_db.connection.cursor.assert_not_called()


class TestNewDataScenarios:
    """Test scenarios specifically for new data detection and processing."""
    
    @pytest.fixture
    def extractor(self):
        """BalanceSheetExtractor instance for testing."""
        with patch.dict(os.environ, {'ALPHAVANTAGE_API_KEY': 'test_key'}):
            return BalanceSheetExtractor()
    
    def test_new_company_first_extraction(self, extractor):
        """Test extracting data for a completely new company."""
        # Mock database calls
        mock_db = Mock()
        mock_watermark_mgr = Mock()
        
        # Mock API response
        api_response = {
            "symbol": "NEWCO",
            "annualReports": [{
                "fiscalDateEnding": "2023-12-31",
                "totalAssets": "1000000",
                "reportedCurrency": "USD"
            }],
            "quarterlyReports": []
        }
        
        with patch.object(extractor, '_fetch_api_data', return_value=(api_response, "success")), \
             patch.object(extractor, '_store_landing_record', return_value="new-hash"), \
             patch.object(extractor, '_content_has_changed', return_value=True), \
             patch.object(extractor, '_upsert_records', return_value=1), \
             patch.object(extractor, '_initialize_watermark_manager', return_value=mock_watermark_mgr):
            
            result = extractor.extract_symbol("NEWCO", 999, mock_db)
            
            assert result['status'] == 'success'
            assert result['records_processed'] == 1
            assert result['symbol'] == 'NEWCO'
            mock_watermark_mgr.update_watermark.assert_called_once()
    
    def test_existing_company_new_quarterly_data(self, extractor):
        """Test extracting new quarterly data for existing company."""
        mock_db = Mock()
        mock_watermark_mgr = Mock()
        
        # Mock API response with new quarterly data
        api_response = {
            "symbol": "AAPL",
            "annualReports": [],
            "quarterlyReports": [{
                "fiscalDateEnding": "2024-06-30",  # New quarter
                "totalAssets": "370000000000",
                "reportedCurrency": "USD"
            }]
        }
        
        with patch.object(extractor, '_fetch_api_data', return_value=(api_response, "success")), \
             patch.object(extractor, '_store_landing_record', return_value="new-hash"), \
             patch.object(extractor, '_content_has_changed', return_value=True), \
             patch.object(extractor, '_upsert_records', return_value=1), \
             patch.object(extractor, '_initialize_watermark_manager', return_value=mock_watermark_mgr):
            
            result = extractor.extract_symbol("AAPL", 1, mock_db)
            
            assert result['status'] == 'success'
            assert result['records_processed'] == 1
            assert result['latest_fiscal_date'] == date(2024, 6, 30)
    
    def test_no_new_data_scenario(self, extractor):
        """Test scenario where API returns same data (no changes)."""
        mock_db = Mock()
        mock_watermark_mgr = Mock()
        
        api_response = {
            "symbol": "AAPL",
            "annualReports": [{
                "fiscalDateEnding": "2023-12-31",
                "totalAssets": "352755000000",
                "reportedCurrency": "USD"
            }],
            "quarterlyReports": []
        }
        
        with patch.object(extractor, '_fetch_api_data', return_value=(api_response, "success")), \
             patch.object(extractor, '_store_landing_record', return_value="existing-hash"), \
             patch.object(extractor, '_content_has_changed', return_value=False), \
             patch.object(extractor, '_initialize_watermark_manager', return_value=mock_watermark_mgr):
            
            result = extractor.extract_symbol("AAPL", 1, mock_db)
            
            assert result['status'] == 'no_changes'
            assert result['records_processed'] == 0
            mock_watermark_mgr.update_watermark.assert_called_with('balance_sheet', 1, success=True)


class TestEdgeCases:
    """Test edge cases and error scenarios."""
    
    @pytest.fixture
    def extractor(self):
        """BalanceSheetExtractor instance for testing."""
        with patch.dict(os.environ, {'ALPHAVANTAGE_API_KEY': 'test_key'}):
            return BalanceSheetExtractor()
    
    def test_api_timeout(self, extractor):
        """Test API timeout handling."""
        mock_db = Mock()
        mock_watermark_mgr = Mock()
        
        with patch.object(extractor, '_fetch_api_data', return_value=({}, "error")), \
             patch.object(extractor, '_store_landing_record', return_value="error-hash"), \
             patch.object(extractor, '_initialize_watermark_manager', return_value=mock_watermark_mgr):
            
            result = extractor.extract_symbol("TIMEOUT", 1, mock_db)
            
            assert result['status'] == 'api_failure'
            assert result['records_processed'] == 0
            mock_watermark_mgr.update_watermark.assert_called_with('balance_sheet', 1, success=False)
    
    def test_data_with_all_null_values(self, extractor):
        """Test handling data where all financial values are null."""
        api_response = {
            "symbol": "NULLCO",
            "annualReports": [{
                "fiscalDateEnding": "2023-12-31",
                "totalAssets": "None",
                "totalLiabilities": "",
                "totalShareholderEquity": None,
                "reportedCurrency": "USD"
            }],
            "quarterlyReports": []
        }
        
        records = extractor._transform_data("NULLCO", 1, api_response, "test-run-id")
        
        assert len(records) == 1
        record = records[0]
        assert record['total_assets'] is None
        assert record['total_liabilities'] is None
        assert record['total_shareholder_equity'] is None
        assert record['fiscal_date_ending'] == date(2023, 12, 31)
    
    def test_very_large_numbers(self, extractor):
        """Test handling very large financial numbers."""
        api_response = {
            "symbol": "BIGCO",
            "annualReports": [{
                "fiscalDateEnding": "2023-12-31",
                "totalAssets": "99999999999999999999.99",  # Very large number
                "reportedCurrency": "USD"
            }],
            "quarterlyReports": []
        }
        
        records = extractor._transform_data("BIGCO", 1, api_response, "test-run-id")
        
        assert len(records) == 1
        assert records[0]['total_assets'] == 99999999999999999999.99


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
