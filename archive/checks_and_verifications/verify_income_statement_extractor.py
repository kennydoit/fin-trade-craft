"""
Verification script for Income Statement Extractor.

This script provides comprehensive verification of the income statement extraction system,
including database connectivity, API functionality, data quality, and processing capabilities.
"""

import sys
import os
from datetime import datetime, timedelta
from decimal import Decimal

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'db'))

try:
    from data_pipeline.extract.extract_income_statement import IncomeStatementExtractor, INCOME_STATEMENT_FIELDS
    from postgres_database_manager import PostgresDatabaseManager
    print("✅ Successfully imported required modules")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)


class IncomeStatementVerifier:
    """Verification suite for income statement extractor."""
    
    def __init__(self):
        """Initialize the verifier."""
        self.extractor = IncomeStatementExtractor()
        self.db = None
        
    def verify_database_connection(self):
        """Verify database connectivity."""
        print("\n🔍 Testing database connection...")
        try:
            self.db = PostgresDatabaseManager()
            self.db.connect()  # Explicitly connect
            # Test the connection
            with self.db.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    print("✅ Database connection successful")
                    return True
                else:
                    print("❌ Database connection test failed")
                    return False
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def verify_schema_structure(self):
        """Verify income statement table schema."""
        print("\n🔍 Verifying income statement table schema...")
        try:
            with self.db.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'source' 
                    AND table_name = 'income_statement'
                    ORDER BY ordinal_position
                """)
                columns = cursor.fetchall()
                
                if not columns:
                    print("❌ Income statement table does not exist")
                    return False
                
                print(f"✅ Income statement table found with {len(columns)} columns:")
                for col_name, col_type in columns:
                    print(f"   {col_name}: {col_type}")
                
                # Check for critical columns
                column_names = [col[0] for col in columns]
                required_columns = [
                    'income_statement_id', 'symbol_id', 'fiscal_date_ending', 
                    'report_type', 'total_revenue', 'net_income', 'gross_profit'
                ]
                
                missing_columns = [col for col in required_columns if col not in column_names]
                if missing_columns:
                    print(f"❌ Missing required columns: {missing_columns}")
                    return False
                
                print("✅ All required columns present")
                return True
                
        except Exception as e:
            print(f"❌ Schema verification failed: {e}")
            return False
    
    def verify_field_mapping(self):
        """Verify field mapping configuration."""
        print("\n🔍 Verifying field mapping...")
        
        # Import the field mapping from the extractor module
        from data_pipeline.extract.extract_income_statement import INCOME_STATEMENT_FIELDS
        
        mapping = INCOME_STATEMENT_FIELDS
        print(f"✅ Field mapping contains {len(mapping)} fields")
        
        # Check for required fields
        required_fields = [
            'total_revenue', 'gross_profit', 'cost_of_revenue', 'operating_income',
            'operating_expenses', 'net_income', 'ebit', 'ebitda', 'income_before_tax'
        ]
        
        missing_fields = [field for field in required_fields if field not in mapping]
        if missing_fields:
            print(f"❌ Missing required fields: {missing_fields}")
            return False
        
        print("✅ All required fields present in mapping")
        
        # Display field categories
        revenue_fields = [k for k in mapping.keys() if 'revenue' in k.lower()]
        profit_fields = [k for k in mapping.keys() if any(x in k.lower() for x in ['profit', 'income', 'earning'])]
        expense_fields = [k for k in mapping.keys() if any(x in k.lower() for x in ['expense', 'cost'])]
        
        print(f"   Revenue fields: {len(revenue_fields)}")
        print(f"   Profit/Income fields: {len(profit_fields)}")
        print(f"   Expense/Cost fields: {len(expense_fields)}")
        
        return True
    
    def verify_existing_data(self):
        """Verify existing data in the income statement table."""
        print("\n🔍 Analyzing existing income statement data...")
        try:
            with self.db.connection.cursor() as cursor:
                # Count total records
                cursor.execute("SELECT COUNT(*) FROM source.income_statement")
                total_records = cursor.fetchone()[0]
                print(f"✅ Total records: {total_records:,}")
                
                if total_records == 0:
                    print("⚠️  No existing data found")
                    return True
                
                # Count by report type
                cursor.execute("""
                    SELECT report_type, COUNT(*) 
                    FROM source.income_statement 
                    GROUP BY report_type 
                    ORDER BY COUNT(*) DESC
                """)
                report_types = cursor.fetchall()
                print("📊 Records by report type:")
                for report_type, count in report_types:
                    print(f"   {report_type}: {count:,}")
                
                # Count unique symbols
                cursor.execute("SELECT COUNT(DISTINCT symbol_id) FROM source.income_statement")
                unique_symbols = cursor.fetchone()[0]
                print(f"✅ Unique symbols: {unique_symbols:,}")
                
                # Date range
                cursor.execute("""
                    SELECT MIN(fiscal_date_ending), MAX(fiscal_date_ending) 
                    FROM source.income_statement
                """)
                min_date, max_date = cursor.fetchone()
                print(f"✅ Date range: {min_date} to {max_date}")
                
                # Data completeness for key fields
                key_fields = ['total_revenue', 'net_income', 'gross_profit', 'operating_income']
                print("📊 Data completeness:")
                for field in key_fields:
                    cursor.execute(f"""
                        SELECT 
                            COUNT(*) as total,
                            COUNT({field}) as non_null,
                            ROUND(COUNT({field}) * 100.0 / COUNT(*), 1) as completeness_pct
                        FROM source.income_statement
                    """)
                    total, non_null, completeness = cursor.fetchone()
                    print(f"   {field}: {completeness}% ({non_null:,}/{total:,})")
                
                return True
                
        except Exception as e:
            print(f"❌ Data verification failed: {e}")
            return False
    
    def verify_api_functionality(self):
        """Verify API functionality with a test symbol."""
        print("\n🔍 Testing API functionality...")
        
        test_symbols = ['AAPL', 'MSFT', 'GOOGL']
        
        for symbol in test_symbols:
            try:
                print(f"   Testing API call for {symbol}...")
                api_response, status = self.extractor._fetch_api_data(symbol)
                
                if status == "success":
                    print(f"   ✅ {symbol}: API call successful")
                    
                    # Check response structure
                    if 'annualReports' in api_response:
                        annual_count = len(api_response['annualReports'])
                        print(f"      Annual reports: {annual_count}")
                    
                    if 'quarterlyReports' in api_response:
                        quarterly_count = len(api_response['quarterlyReports'])
                        print(f"      Quarterly reports: {quarterly_count}")
                    
                    return True  # Success with first symbol
                else:
                    print(f"   ❌ {symbol}: API call failed - {status}")
                    continue
                    
            except Exception as e:
                print(f"   ❌ {symbol}: Exception - {e}")
                continue
        
        print("❌ All API test calls failed")
        return False
    
    def verify_transformation_logic(self):
        """Verify data transformation logic."""
        print("\n🔍 Testing transformation logic...")
        
        # Sample API response for testing
        sample_response = {
            "symbol": "TEST",
            "annualReports": [
                {
                    "fiscalDateEnding": "2023-12-31",
                    "reportedCurrency": "USD",
                    "grossProfit": "1000000000",
                    "totalRevenue": "2000000000",
                    "costOfRevenue": "1000000000",
                    "operatingIncome": "500000000",
                    "operatingExpenses": "500000000",
                    "netIncome": "400000000",
                    "ebit": "500000000",
                    "ebitda": "600000000",
                    "incomeBeforeTax": "450000000",
                    "incomeTaxExpense": "50000000"
                }
            ],
            "quarterlyReports": [
                {
                    "fiscalDateEnding": "2023-09-30",
                    "reportedCurrency": "USD", 
                    "grossProfit": "250000000",
                    "totalRevenue": "500000000",
                    "costOfRevenue": "250000000",
                    "operatingIncome": "125000000",
                    "operatingExpenses": "125000000",
                    "netIncome": "100000000",
                    "ebit": "125000000",
                    "ebitda": "150000000",
                    "incomeBeforeTax": "112500000",
                    "incomeTaxExpense": "12500000"
                }
            ]
        }
        
        try:
            records = self.extractor._transform_data("TEST", 1, sample_response, "test_run")
            
            if not records:
                print("❌ No records generated from transformation")
                return False
            
            print(f"✅ Generated {len(records)} records")
            
            # Verify record structure
            for i, record in enumerate(records):
                report_type = record.get('report_type')
                fiscal_date = record.get('fiscal_date_ending')
                total_revenue = record.get('total_revenue')
                net_income = record.get('net_income')
                
                print(f"   Record {i+1}: {report_type} - {fiscal_date}")
                print(f"      Total Revenue: {total_revenue}")
                print(f"      Net Income: {net_income}")
                
                # Validate data types
                if not isinstance(total_revenue, (Decimal, type(None))):
                    print(f"❌ Invalid total_revenue type: {type(total_revenue)}")
                    return False
                
                if not isinstance(net_income, (Decimal, type(None))):
                    print(f"❌ Invalid net_income type: {type(net_income)}")
                    return False
            
            print("✅ Transformation logic working correctly")
            return True
            
        except Exception as e:
            print(f"❌ Transformation failed: {e}")
            return False
    
    def verify_watermark_system(self):
        """Verify watermark system functionality."""
        print("\n🔍 Testing watermark system...")
        try:
            from utils.incremental_etl import WatermarkManager
            
            watermark_mgr = WatermarkManager(self.db)
            print("✅ Watermark manager initialized")
            
            # Check if watermark table exists
            with self.db.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = 'source' AND table_name = 'extraction_watermarks'
                """)
                if cursor.fetchone()[0] == 0:
                    print("❌ Watermarks table does not exist")
                    return False
            
            print("✅ Watermarks table exists")
            
            # Check for income statement watermarks
            with self.db.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM source.extraction_watermarks 
                    WHERE table_name = 'income_statement'
                """)
                watermark_count = cursor.fetchone()[0]
                print(f"✅ Income statement watermarks: {watermark_count:,}")
            
            return True
            
        except Exception as e:
            print(f"❌ Watermark verification failed: {e}")
            return False
    
    def verify_content_hashing(self):
        """Verify content hashing system."""
        print("\n🔍 Testing content hashing system...")
        try:
            mapping = INCOME_STATEMENT_FIELDS
            from utils.incremental_etl import RunIdGenerator
            # Test with sample data
            sample_data = {"test": "data", "for": "hashing"}
            # Use proper UUIDs for test run IDs
            test_run_id_1 = RunIdGenerator.generate()
            test_run_id_2 = RunIdGenerator.generate()
            valid_symbol_id = 15348907  # Use a real symbol_id from the database
            hash1 = self.extractor._store_landing_record(
                self.db, "TEST", valid_symbol_id, sample_data, "success", test_run_id_1
            )
            hash2 = self.extractor._store_landing_record(
                self.db, "TEST", valid_symbol_id, sample_data, "success", test_run_id_2
            )
            if hash1 == hash2:
                print("✅ Content hashing produces consistent results")
            else:
                print("❌ Content hashing inconsistent")
                return False
            # Test change detection
            changed = self.extractor._content_has_changed(self.db, valid_symbol_id, hash1)
            if changed is False:
                print("✅ Content change detection working (no change)")
            else:
                print("❌ Content change detection failed (should be no change)")
                return False
            # Clean up test records
            with self.db.connection.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM source.landing_income_statement 
                    WHERE symbol_id = %s AND run_id IN (%s, %s)
                """, (valid_symbol_id, test_run_id_1, test_run_id_2))
                self.db.connection.commit()
            return True
        except Exception as e:
            print(f"❌ Content hashing verification failed: {e}")
            return False
    
    def run_all_verifications(self):
        """Run all verification tests."""
        print("🚀 Starting Income Statement Extractor Verification")
        print("=" * 60)
        
        results = []
        
        # Database connection
        results.append(("Database Connection", self.verify_database_connection()))
        
        if not results[-1][1]:  # If database connection failed, skip rest
            print("\n❌ Cannot continue without database connection")
            return False
        
        # Schema structure
        results.append(("Schema Structure", self.verify_schema_structure()))
        
        # Field mapping
        results.append(("Field Mapping", self.verify_field_mapping()))
        
        # Existing data
        results.append(("Existing Data", self.verify_existing_data()))
        
        # API functionality
        results.append(("API Functionality", self.verify_api_functionality()))
        
        # Transformation logic
        results.append(("Transformation Logic", self.verify_transformation_logic()))
        
        # Watermark system
        results.append(("Watermark System", self.verify_watermark_system()))
        
        # Content hashing
        results.append(("Content Hashing", self.verify_content_hashing()))
        
        # Summary
        print("\n" + "=" * 60)
        print("📋 VERIFICATION SUMMARY")
        print("=" * 60)
        
        passed = 0
        total = len(results)
        
        for test_name, result in results:
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{test_name:<25} {status}")
            if result:
                passed += 1
        
        print("-" * 60)
        print(f"TOTAL: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
        
        if passed == total:
            print("\n🎉 All verifications passed! Income statement extractor is ready for production.")
            return True
        else:
            print(f"\n⚠️  {total-passed} verification(s) failed. Please address issues before production use.")
            return False


def main():
    """Main execution function."""
    verifier = IncomeStatementVerifier()
    success = verifier.run_all_verifications()
    
    if success:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
