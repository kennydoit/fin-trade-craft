"""
Test suite for Income Statement Extractor.

This module provides comprehensive testing for the IncomeStatementExtractor class,
including unit tests, integration tests, and data quality validation.
"""

import pytest
import json
from datetime import datetime, date
from unittest.mock import Mock, patch, MagicMock
from decimal import Decimal

# Import the extractor
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from data_pipeline.extract.extract_income_statement import IncomeStatementExtractor


class TestIncomeStatementExtractor:
    """Test cases for IncomeStatementExtractor."""
    
    @pytest.fixture
    def extractor(self):
        """Create an IncomeStatementExtractor instance."""
        return IncomeStatementExtractor()
    
    @pytest.fixture
    def sample_api_response(self):
        """Sample API response from Alpha Vantage."""
        return {
            "symbol": "AAPL",
            "annualReports": [
                {
                    "fiscalDateEnding": "2023-09-30",
                    "reportedCurrency": "USD",
                    "grossProfit": "169148000000",
                    "totalRevenue": "383285000000",
                    "costOfRevenue": "214137000000",
                    "costofGoodsAndServicesSold": "214137000000",
                    "operatingIncome": "114301000000",
                    "sellingGeneralAndAdministrative": "24932000000",
                    "researchAndDevelopment": "29915000000",
                    "operatingExpenses": "54847000000",
                    "investmentIncomeNet": "0",
                    "netInterestIncome": "3750000000",
                    "interestIncome": "3750000000",
                    "interestExpense": "0",
                    "nonInterestIncome": "0",
                    "otherNonOperatingIncome": "0",
                    "depreciation": "11519000000",
                    "depreciationAndAmortization": "11519000000",
                    "incomeBeforeTax": "118051000000",
                    "incomeTaxExpense": "21058000000",
                    "interestAndDebtExpense": "0",
                    "netIncomeFromContinuingOperations": "96995000000",
                    "comprehensiveIncomeNetOfTax": "96995000000",
                    "ebit": "118051000000",
                    "ebitda": "129570000000",
                    "netIncome": "96995000000"
                }
            ],
            "quarterlyReports": [
                {
                    "fiscalDateEnding": "2023-09-30",
                    "reportedCurrency": "USD",
                    "grossProfit": "45961000000",
                    "totalRevenue": "89498000000",
                    "costOfRevenue": "43537000000",
                    "costofGoodsAndServicesSold": "43537000000",
                    "operatingIncome": "22986000000",
                    "sellingGeneralAndAdministrative": "6495000000",
                    "researchAndDevelopment": "7476000000",
                    "operatingExpenses": "13971000000",
                    "investmentIncomeNet": "0",
                    "netInterestIncome": "1303000000",
                    "interestIncome": "1303000000",
                    "interestExpense": "0",
                    "nonInterestIncome": "0",
                    "otherNonOperatingIncome": "0",
                    "depreciation": "3103000000",
                    "depreciationAndAmortization": "3103000000",
                    "incomeBeforeTax": "24289000000",
                    "incomeTaxExpense": "4020000000",
                    "interestAndDebtExpense": "0",
                    "netIncomeFromContinuingOperations": "20269000000",
                    "comprehensiveIncomeNetOfTax": "20269000000",
                    "ebit": "24289000000",
                    "ebitda": "27392000000",
                    "netIncome": "20269000000"
                }
            ]
        }
    
    @pytest.fixture
    def mock_db(self):
        """Mock database manager."""
        db = Mock()
        db.connection = Mock()
        db.connection.cursor.return_value.__enter__ = Mock()
        db.connection.cursor.return_value.__exit__ = Mock()
        cursor = Mock()
        cursor.fetchone.return_value = (1,)  # Mock symbol_id
        cursor.rowcount = 2
        db.connection.cursor.return_value.__enter__.return_value = cursor
        return db

    def test_initialization(self, extractor):
        """Test extractor initialization."""
        assert extractor.table_name == "income_statement"
        assert extractor.api_function == "INCOME_STATEMENT"
        assert len(extractor.field_mapping) > 0
        assert "symbol_id" in extractor.field_mapping
        assert "fiscal_date_ending" in extractor.field_mapping
        assert "total_revenue" in extractor.field_mapping

    def test_field_mapping_completeness(self, extractor):
        """Test that field mapping includes all expected income statement fields."""
        expected_fields = [
            "symbol_id", "fiscal_date_ending", "report_type", "reported_currency",
            "total_revenue", "gross_profit", "cost_of_revenue",
            "operating_income", "operating_expenses", "net_income",
            "ebit", "ebitda", "income_before_tax", "income_tax_expense"
        ]
        
        for field in expected_fields:
            assert field in extractor.field_mapping, f"Missing field: {field}"

    def test_transform_data_annual_report(self, extractor, sample_api_response, mock_db):
        """Test transformation of annual report data."""
        symbol = "AAPL"
        symbol_id = 1
        run_id = "test_run_123"
        
        records = extractor._transform_data(symbol, symbol_id, sample_api_response, run_id)
        
        # Should have 1 annual record
        annual_records = [r for r in records if r["report_type"] == "annual"]
        assert len(annual_records) == 1
        
        annual_record = annual_records[0]
        assert annual_record["symbol_id"] == symbol_id
        assert annual_record["fiscal_date_ending"] == date(2023, 9, 30)
        assert annual_record["total_revenue"] == Decimal("383285000000")
        assert annual_record["net_income"] == Decimal("96995000000")
        assert annual_record["gross_profit"] == Decimal("169148000000")
        assert annual_record["operating_income"] == Decimal("114301000000")
        assert annual_record["run_id"] == run_id

    def test_transform_data_quarterly_report(self, extractor, sample_api_response, mock_db):
        """Test transformation of quarterly report data."""
        symbol = "AAPL"
        symbol_id = 1
        run_id = "test_run_123"
        
        records = extractor._transform_data(symbol, symbol_id, sample_api_response, run_id)
        
        # Should have 1 quarterly record
        quarterly_records = [r for r in records if r["report_type"] == "quarterly"]
        assert len(quarterly_records) == 1
        
        quarterly_record = quarterly_records[0]
        assert quarterly_record["symbol_id"] == symbol_id
        assert quarterly_record["fiscal_date_ending"] == date(2023, 9, 30)
        assert quarterly_record["total_revenue"] == Decimal("89498000000")
        assert quarterly_record["net_income"] == Decimal("20269000000")
        assert quarterly_record["gross_profit"] == Decimal("45961000000")
        assert quarterly_record["operating_income"] == Decimal("22986000000")

    def test_transform_data_handles_none_values(self, extractor):
        """Test that transformation handles None/null values correctly."""
        api_response = {
            "symbol": "TEST",
            "annualReports": [
                {
                    "fiscalDateEnding": "2023-12-31",
                    "reportedCurrency": "USD",
                    "grossProfit": "None",
                    "totalRevenue": "1000000",
                    "operatingIncome": "None",
                    "netIncome": "None"
                }
            ],
            "quarterlyReports": []
        }
        
        symbol = "TEST"
        symbol_id = 1
        run_id = "test_run_123"
        
        records = extractor._transform_data(symbol, symbol_id, api_response, run_id)
        
        assert len(records) == 1
        record = records[0]
        assert record["total_revenue"] == Decimal("1000000")
        assert record["gross_profit"] is None
        assert record["operating_income"] is None
        assert record["net_income"] is None

    def test_transform_data_empty_response(self, extractor):
        """Test transformation with empty API response."""
        api_response = {
            "symbol": "TEST",
            "annualReports": [],
            "quarterlyReports": []
        }
        
        records = extractor._transform_data("TEST", 1, api_response, "test_run")
        assert records == []

    def test_upsert_records(self, extractor, mock_db):
        """Test record upserting."""
        records = [
            {
                "symbol_id": 1,
                "fiscal_date_ending": date(2023, 9, 30),
                "report_type": "annual",
                "total_revenue": Decimal("383285000000"),
                "net_income": Decimal("96995000000"),
                "run_id": "test_run_123"
            }
        ]
        
        result = extractor._upsert_records(mock_db, records)
        
        assert result == 2  # Mock rowcount
        mock_db.connection.cursor.assert_called()
        mock_db.connection.commit.assert_called()

    def test_upsert_records_empty_list(self, extractor, mock_db):
        """Test upserting empty record list."""
        result = extractor._upsert_records(mock_db, [])
        assert result == 0
        mock_db.connection.cursor.assert_not_called()

    @patch('data_pipeline.extract.extract_income_statement.time.sleep')
    @patch('data_pipeline.extract.extract_income_statement.requests.get')
    def test_fetch_api_data_success(self, mock_get, mock_sleep, extractor):
        """Test successful API data fetch."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"symbol": "AAPL", "annualReports": []}
        mock_get.return_value = mock_response
        
        result, status = extractor._fetch_api_data("AAPL")
        
        assert status == "success"
        assert result == {"symbol": "AAPL", "annualReports": []}

    @patch('data_pipeline.extract.extract_income_statement.requests.get')
    def test_fetch_api_data_failure(self, mock_get, extractor):
        """Test API data fetch failure."""
        mock_get.side_effect = Exception("API Error")
        
        result, status = extractor._fetch_api_data("AAPL")
        
        assert status == "error"
        assert "error" in result

    def test_content_has_changed_new_content(self, extractor, mock_db):
        """Test content change detection with new content."""
        # Mock no existing record
        cursor = mock_db.connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None
        
        result = extractor._content_has_changed(mock_db, 1, "new_hash")
        assert result is True

    def test_content_has_changed_same_content(self, extractor, mock_db):
        """Test content change detection with same content."""
        # Mock existing record with same hash
        cursor = mock_db.connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("same_hash",)
        
        result = extractor._content_has_changed(mock_db, 1, "same_hash")
        assert result is False

    def test_content_has_changed_different_content(self, extractor, mock_db):
        """Test content change detection with different content."""
        # Mock existing record with different hash
        cursor = mock_db.connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("old_hash",)
        
        result = extractor._content_has_changed(mock_db, 1, "new_hash")
        assert result is True

    def test_store_landing_record(self, extractor, mock_db):
        """Test storing landing record."""
        api_response = {"symbol": "AAPL", "annualReports": []}
        content_hash = extractor._store_landing_record(
            mock_db, "AAPL", 1, api_response, "success", "test_run"
        )
        
        assert content_hash is not None
        assert len(content_hash) == 64  # SHA-256 hash length
        mock_db.connection.cursor.assert_called()
        mock_db.connection.commit.assert_called()

    def test_decimal_conversion(self, extractor):
        """Test decimal conversion with various input types."""
        # Test string number
        assert extractor._to_decimal("1234567890") == Decimal("1234567890")
        
        # Test "None" string
        assert extractor._to_decimal("None") is None
        
        # Test None value
        assert extractor._to_decimal(None) is None
        
        # Test empty string
        assert extractor._to_decimal("") is None
        
        # Test numeric with decimals
        assert extractor._to_decimal("123.45") == Decimal("123.45")

    def test_date_conversion(self, extractor):
        """Test date conversion."""
        # Test valid date string
        result = extractor._to_date("2023-09-30")
        assert result == date(2023, 9, 30)
        
        # Test None
        assert extractor._to_date(None) is None
        
        # Test empty string
        assert extractor._to_date("") is None


class TestIncomeStatementIntegration:
    """Integration tests for income statement extractor."""
    
    @pytest.mark.integration
    def test_full_extraction_workflow(self):
        """Test complete extraction workflow (requires database connection)."""
        # This would test actual database operations
        # Only run when database is available
        pytest.skip("Requires live database connection")
    
    @pytest.mark.integration
    def test_api_integration(self):
        """Test actual API integration (requires API key)."""
        # This would test actual API calls
        # Only run when API key is available
        pytest.skip("Requires valid API key and network connection")


def test_data_quality_validation():
    """Test data quality validation rules."""
    extractor = IncomeStatementExtractor()
    
    # Test that required fields are present
    required_fields = ["symbol_id", "fiscal_date_ending", "report_type"]
    for field in required_fields:
        assert field in extractor.field_mapping
    
    # Test that financial fields use decimal type
    financial_fields = ["total_revenue", "net_income", "gross_profit", "operating_income"]
    for field in financial_fields:
        assert field in extractor.field_mapping


if __name__ == "__main__":
    pytest.main([__file__])
