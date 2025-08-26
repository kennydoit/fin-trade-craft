create the program /data_pipeline/transformed/transform_income_statement.py

1. Create the below features 
2. normalize them
3. apply the prefix "fis_" to all normalized data
4. remove non-normalized data


# Core Profitability Features

gross_margin = (gross_profit / total_revenue)

operating_margin = (operating_income / total_revenue)

net_margin = (net_income / total_revenue)

ebit_margin = (ebit / total_revenue)

ebitda_margin = (ebitda / total_revenue)

# Expense Control Features

sga_ratio = (selling_general_and_administrative / total_revenue)

r_and_d_ratio = (research_and_development / total_revenue)

operating_expense_ratio = (operating_expenses / total_revenue)

# Leverage & Interest Features

interest_coverage = (ebit / interest_expense)

debt_burden_ratio = (interest_and_debt_expense / total_revenue)

net_interest_ratio = (net_interest_income / total_revenue)

# Tax & Income Quality Features

effective_tax_rate = (income_tax_expense / income_before_tax)

continuing_ops_ratio = (net_income_from_continuing_operations / net_income)

comprehensive_income_ratio = (comprehensive_income_net_of_tax / net_income)

# Growth & Volatility Features

revenue_growth_qoq = (total_revenue / lag(total_revenue,1)) - 1

operating_income_growth_qoq = (operating_income / lag(operating_income,1)) - 1

net_income_growth_qoq = (net_income / lag(net_income,1)) - 1

earnings_volatility_4q = stdev(net_income over last 4 quarters) / mean(net_income over last 4 quarters)

# Cash Proxy Features (from IS itself)

depreciation_ratio = (depreciation_and_amortization / total_revenue)

non_operating_income_ratio = (other_non_operating_income / total_revenue)