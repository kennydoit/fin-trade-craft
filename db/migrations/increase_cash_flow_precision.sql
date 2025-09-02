-- Migration to increase decimal precision for cash flow fields
-- Issue: Companies like Apple have cash flows > $100B, exceeding DECIMAL(20,2) limits
-- Solution: Increase to DECIMAL(28,2) to handle up to $999 trillion

-- Cash flow table precision updates
ALTER TABLE source.cash_flow 
    ALTER COLUMN operating_cashflow TYPE DECIMAL(28,2),
    ALTER COLUMN payments_for_operating_activities TYPE DECIMAL(28,2),
    ALTER COLUMN proceeds_from_operating_activities TYPE DECIMAL(28,2),
    ALTER COLUMN change_in_operating_liabilities TYPE DECIMAL(28,2),
    ALTER COLUMN change_in_operating_assets TYPE DECIMAL(28,2),
    ALTER COLUMN depreciation_depletion_and_amortization TYPE DECIMAL(28,2),
    ALTER COLUMN capital_expenditures TYPE DECIMAL(28,2),
    ALTER COLUMN change_in_receivables TYPE DECIMAL(28,2),
    ALTER COLUMN change_in_inventory TYPE DECIMAL(28,2),
    ALTER COLUMN profit_loss TYPE DECIMAL(28,2),
    ALTER COLUMN cashflow_from_investment TYPE DECIMAL(28,2),
    ALTER COLUMN cashflow_from_financing TYPE DECIMAL(28,2),
    ALTER COLUMN proceeds_from_repayments_of_short_term_debt TYPE DECIMAL(28,2),
    ALTER COLUMN payments_for_repurchase_of_common_stock TYPE DECIMAL(28,2),
    ALTER COLUMN payments_for_repurchase_of_equity TYPE DECIMAL(28,2),
    ALTER COLUMN payments_for_repurchase_of_preferred_stock TYPE DECIMAL(28,2),
    ALTER COLUMN dividend_payout TYPE DECIMAL(28,2),
    ALTER COLUMN dividend_payout_common_stock TYPE DECIMAL(28,2),
    ALTER COLUMN dividend_payout_preferred_stock TYPE DECIMAL(28,2),
    ALTER COLUMN proceeds_from_issuance_of_common_stock TYPE DECIMAL(28,2),
    ALTER COLUMN proceeds_from_issuance_of_long_term_debt_and_capital_securities_net TYPE DECIMAL(28,2),
    ALTER COLUMN proceeds_from_issuance_of_preferred_stock TYPE DECIMAL(28,2),
    ALTER COLUMN proceeds_from_repurchase_of_equity TYPE DECIMAL(28,2),
    ALTER COLUMN proceeds_from_sale_of_treasury_stock TYPE DECIMAL(28,2),
    ALTER COLUMN change_in_cash_and_cash_equivalents TYPE DECIMAL(28,2);

-- Verify the changes
SELECT column_name, data_type, numeric_precision, numeric_scale
FROM information_schema.columns 
WHERE table_schema = 'source' 
  AND table_name = 'cash_flow' 
  AND data_type = 'numeric'
ORDER BY column_name;
