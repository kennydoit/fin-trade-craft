"""
Comprehensive unit tests for cash flow extractor.
"""

import pytest
import json
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from data_pipeline.extract.extract_cash_flow import CashFlowExtractor, CASH_FLOW_FIELDS


class TestCashFlowExtractor:
    """Test suite for CashFlowExtractor."""
    
    @pytest.fixture
    def mock_env_vars(self, monkeypatch):
        """Mock environment variables."""
        monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test_api_key")
    
    @pytest.fixture
    def extractor(self, mock_env_vars):
        """Create extractor instance with mocked environment."""
        return CashFlowExtractor()
    
    @pytest.fixture
    def mock_api_response(self):
        """Mock successful API response."""
        return {
            "symbol": "AAPL",
            "quarterlyReports": [
                {
                    "fiscalDateEnding": "2024-06-30",
                    "reportedCurrency": "USD",
                    "operatingCashflow": "26000000000",
                    "capitalExpenditures": "-2100000000",
                    "cashflowFromInvestment": "-5500000000",
                    "cashflowFromFinancing": "-21800000000",
                    "changeInCashAndCashEquivalents": "-1300000000",
                    "dividendPayout": "-3900000000",
                    "proceedsFromIssuanceOfCommonStock": "0",
                    "paymentsForRepurchaseOfCommonStock": "-16500000000"
                }
            ],
            "annualReports": [
                {
                    "fiscalDateEnding": "2023-09-30",
                    "reportedCurrency": "USD",
                    "operatingCashflow": "110563000000",
                    "capitalExpenditures": "-10959000000",
                    "cashflowFromInvestment": "-1337000000",
                    "cashflowFromFinancing": "-108488000000",
                    "changeInCashAndCashEquivalents": "738000000",
                    "dividendPayout": "-15025000000",
                    "proceedsFromIssuanceOfCommonStock": "0",
                    "paymentsForRepurchaseOfCommonStock": "-77550000000"
                }
            ]
        }
    
    @pytest.fixture
    def mock_db(self):
        """Mock database manager."""
        db = Mock()
        db.execute_query = Mock()
        db.fetch_query = Mock()
        db.execute_script = Mock()
        db.connection = Mock()
        db.connection.cursor = Mock()
        
        # Mock cursor for upsert operations
        mock_cursor = Mock()
        mock_cursor.executemany = Mock()
        mock_cursor.rowcount = 2
        db.connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        db.connection.cursor.return_value.__exit__ = Mock(return_value=None)
        
        return db
    
    def test_initialization_success(self, mock_env_vars):
        """Test successful extractor initialization."""
        extractor = CashFlowExtractor()
        assert extractor.api_key == "test_api_key"
        assert extractor.table_name == "cash_flow"
        assert extractor.schema_name == "source"
    
    def test_initialization_missing_api_key(self, monkeypatch):
        """Test initialization fails without API key."""
        monkeypatch.delenv("ALPHAVANTAGE_API_KEY", raising=False)
        with pytest.raises(ValueError, match="ALPHAVANTAGE_API_KEY not found"):
            CashFlowExtractor()
    
    @patch('requests.get')
    def test_fetch_api_data_success(self, mock_get, extractor, mock_api_response):
        """Test successful API data fetch."""
        mock_response = Mock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        result, status = extractor._fetch_api_data("AAPL")
        
        assert status == "success"
        assert result == mock_api_response
        mock_get.assert_called_once()
    
    @patch('requests.get')
    def test_fetch_api_data_error(self, mock_get, extractor):
        """Test API error response."""
        mock_response = Mock()
        mock_response.json.return_value = {"Error Message": "Invalid symbol"}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        result, status = extractor._fetch_api_data("INVALID")
        
        assert status == "error"
        assert "Error Message" in result
    
    @patch('requests.get')
    def test_fetch_api_data_rate_limited(self, mock_get, extractor):
        """Test rate limiting response."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 25 calls per minute"
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        result, status = extractor._fetch_api_data("AAPL")
        
        assert status == "rate_limited"
        assert "Note" in result
    
    @patch('requests.get')
    def test_fetch_api_data_network_error(self, mock_get, extractor):
        """Test network error handling."""
        mock_get.side_effect = Exception("Network error")
        
        result, status = extractor._fetch_api_data("AAPL")
        
        assert status == "error"
        assert "error" in result
    
    def test_transform_data_success(self, extractor, mock_api_response):
        """Test successful data transformation."""
        records = extractor._transform_data("AAPL", 1, mock_api_response, "test_run_123")
        
        assert len(records) == 2  # 1 quarterly + 1 annual
        
        # Check quarterly record
        quarterly = next(r for r in records if r["report_type"] == "quarterly")
        assert quarterly["symbol"] == "AAPL"
        assert quarterly["symbol_id"] == 1
        assert quarterly["fiscal_date_ending"].strftime("%Y-%m-%d") == "2024-06-30"
        assert quarterly["operating_cashflow"] == 26000000000.0
        assert quarterly["reported_currency"] == "USD"
        assert quarterly["source_run_id"] == "test_run_123"
        assert quarterly["content_hash"] is not None
        
        # Check annual record
        annual = next(r for r in records if r["report_type"] == "annual")
        assert annual["symbol"] == "AAPL"
        assert annual["fiscal_date_ending"].strftime("%Y-%m-%d") == "2023-09-30"
        assert annual["operating_cashflow"] == 110563000000.0
    
    def test_transform_single_report_quarterly(self, extractor):
        """Test single quarterly report transformation."""
        report_data = {
            "fiscalDateEnding": "2024-06-30",
            "reportedCurrency": "USD",
            "operatingCashflow": "26000000000",
            "capitalExpenditures": "-2100000000",
            "cashflowFromInvestment": "-5500000000"
        }
        
        record = extractor._transform_single_report(
            "AAPL", 1, report_data, "quarterly", "test_run_123"
        )
        
        assert record is not None
        assert record["symbol"] == "AAPL"
        assert record["symbol_id"] == 1
        assert record["report_type"] == "quarterly"
        assert record["fiscal_date_ending"].strftime("%Y-%m-%d") == "2024-06-30"
        assert record["operating_cashflow"] == 26000000000.0
        assert record["capital_expenditures"] == -2100000000.0
        assert record["cashflow_from_investment"] == -5500000000.0
        assert record["reported_currency"] == "USD"
    
    def test_transform_single_report_missing_date(self, extractor):
        """Test transformation with missing fiscal date."""
        report_data = {
            "reportedCurrency": "USD",
            "operatingCashflow": "26000000000"
        }
        
        record = extractor._transform_single_report(
            "AAPL", 1, report_data, "quarterly", "test_run_123"
        )
        
        assert record is None  # Should be None due to missing fiscal date
    
    def test_transform_single_report_value_conversion(self, extractor):
        """Test value conversion in transformation."""
        report_data = {
            "fiscalDateEnding": "2024-06-30",
            "operatingCashflow": "26000000000",  # String number
            "capitalExpenditures": None,  # None value
            "cashflowFromInvestment": "",  # Empty string
            "cashflowFromFinancing": "invalid"  # Invalid number
        }
        
        record = extractor._transform_single_report(
            "AAPL", 1, report_data, "quarterly", "test_run_123"
        )
        
        assert record is not None
        assert record["operating_cashflow"] == 26000000000.0
        assert record["capital_expenditures"] is None
        assert record["cashflow_from_investment"] is None
        assert record["cashflow_from_financing"] is None
    
    def test_content_has_changed_new_content(self, extractor, mock_db):
        """Test content change detection with new content."""
        mock_db.fetch_query.return_value = [(0,)]  # No existing records
        
        result = extractor._content_has_changed(mock_db, 1, "new_hash")
        
        assert result is True
        mock_db.fetch_query.assert_called_once()
    
    def test_content_has_changed_existing_content(self, extractor, mock_db):
        """Test content change detection with existing content."""
        mock_db.fetch_query.return_value = [(1,)]  # Existing record found
        
        result = extractor._content_has_changed(mock_db, 1, "existing_hash")
        
        assert result is False
        mock_db.fetch_query.assert_called_once()
    
    def test_upsert_records_success(self, extractor, mock_db):
        """Test successful record upsert."""
        records = [
            {
                "symbol_id": 1,
                "symbol": "AAPL",
                "fiscal_date_ending": datetime(2024, 6, 30),
                "report_type": "quarterly",
                "operating_cashflow": 26000000000.0,
                "content_hash": "test_hash"
            }
        ]
        
        rows_affected = extractor._upsert_records(mock_db, records)
        
        assert rows_affected == 2  # Mock returns 2
        mock_db.connection.cursor.assert_called_once()
    
    def test_upsert_records_empty_list(self, extractor, mock_db):
        """Test upsert with empty records list."""
        result = extractor._upsert_records(mock_db, [])
        
        assert result == 0
        mock_db.connection.cursor.assert_not_called()
    
    def test_store_landing_record(self, extractor, mock_db):
        """Test storing landing record."""
        api_response = {"test": "data"}
        
        content_hash = extractor._store_landing_record(
            mock_db, "AAPL", 1, api_response, "success", "test_run_123"
        )
        
        assert content_hash is not None
        mock_db.execute_query.assert_called_once()
        
        # Check the query was called with correct parameters
        call_args = mock_db.execute_query.call_args
        assert call_args[0][1][0] == "cash_flow"  # table_name
        assert call_args[0][1][1] == "AAPL"  # symbol
        assert call_args[0][1][2] == 1  # symbol_id
        assert call_args[0][1][3] == "CASH_FLOW"  # api_function
        assert call_args[0][1][7] == "success"  # response_status
    
    def test_field_mapping_completeness(self):
        """Test that all expected cash flow fields are mapped."""
        expected_fields = [
            'operating_cashflow',
            'capital_expenditures', 
            'cashflow_from_investment',
            'cashflow_from_financing',
            'change_in_cash_and_cash_equivalents',
            'dividend_payout'
        ]
        
        for field in expected_fields:
            assert field in CASH_FLOW_FIELDS, f"Missing field mapping: {field}"
    
    @patch.object(CashFlowExtractor, '_fetch_api_data')
    @patch.object(CashFlowExtractor, '_store_landing_record')
    @patch.object(CashFlowExtractor, '_content_has_changed')
    @patch.object(CashFlowExtractor, '_transform_data')
    @patch.object(CashFlowExtractor, '_upsert_records')
    def test_extract_symbol_success_flow(self, mock_upsert, mock_transform, mock_content_changed,
                                        mock_store, mock_fetch, extractor, mock_db, mock_api_response):
        """Test complete symbol extraction success flow."""
        # Setup mocks
        mock_fetch.return_value = (mock_api_response, "success")
        mock_store.return_value = "test_hash"
        mock_content_changed.return_value = True  # Content has changed
        mock_transform.return_value = [{"fiscal_date_ending": datetime(2024, 6, 30)}]
        mock_upsert.return_value = 2
        
        # Mock watermark manager
        with patch.object(extractor, '_initialize_watermark_manager') as mock_watermark_init:
            mock_watermark_mgr = Mock()
            mock_watermark_init.return_value = mock_watermark_mgr
            
            result = extractor.extract_symbol("AAPL", 1, mock_db)
        
        # Verify result
        assert result["symbol"] == "AAPL"
        assert result["status"] == "success"
        assert result["records_processed"] == 1
        assert result["rows_affected"] == 2
        
        # Verify method calls
        mock_fetch.assert_called_once_with("AAPL")
        mock_store.assert_called_once()
        mock_content_changed.assert_called_once()
        mock_transform.assert_called_once()
        mock_upsert.assert_called_once()
        mock_watermark_mgr.update_watermark.assert_called_once()
    
    @patch.object(CashFlowExtractor, '_fetch_api_data')
    @patch.object(CashFlowExtractor, '_store_landing_record')
    def test_extract_symbol_api_failure(self, mock_store, mock_fetch, extractor, mock_db):
        """Test symbol extraction with API failure."""
        # Setup mocks
        mock_fetch.return_value = ({"error": "API error"}, "error")
        mock_store.return_value = "error_hash"
        
        # Mock watermark manager
        with patch.object(extractor, '_initialize_watermark_manager') as mock_watermark_init:
            mock_watermark_mgr = Mock()
            mock_watermark_init.return_value = mock_watermark_mgr
            
            result = extractor.extract_symbol("INVALID", 1, mock_db)
        
        # Verify result
        assert result["symbol"] == "INVALID"
        assert result["status"] == "api_failure"
        assert result["error"] == "error"
        assert result["records_processed"] == 0
        
        # Verify watermark updated with failure
        mock_watermark_mgr.update_watermark.assert_called_once_with(
            "cash_flow", 1, success=False
        )
    
    @patch.object(CashFlowExtractor, '_fetch_api_data')
    @patch.object(CashFlowExtractor, '_store_landing_record')
    @patch.object(CashFlowExtractor, '_content_has_changed')
    def test_extract_symbol_no_changes(self, mock_content_changed, mock_store, 
                                     mock_fetch, extractor, mock_db, mock_api_response):
        """Test symbol extraction with no content changes."""
        # Setup mocks
        mock_fetch.return_value = (mock_api_response, "success")
        mock_store.return_value = "existing_hash"
        mock_content_changed.return_value = False  # No changes
        
        # Mock watermark manager
        with patch.object(extractor, '_initialize_watermark_manager') as mock_watermark_init:
            mock_watermark_mgr = Mock()
            mock_watermark_init.return_value = mock_watermark_mgr
            
            result = extractor.extract_symbol("AAPL", 1, mock_db)
        
        # Verify result
        assert result["symbol"] == "AAPL"
        assert result["status"] == "no_changes"
        assert result["records_processed"] == 0
        
        # Verify watermark updated successfully
        mock_watermark_mgr.update_watermark.assert_called_once_with(
            "cash_flow", 1, success=True
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
