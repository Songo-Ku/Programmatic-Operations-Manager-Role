import pandas as pd
import numpy as np
from datetime import datetime


def extract_data():
    """
    ETL Step 1: EXTRACT - Load raw CSVs into memory (pandas DataFrames) Memory is limited, so for large files required is database staging or other tools.
    Logic: pandas.read_csv() - automatically infers column types and parses YYYY-MM-DD dates.
    Staging: saved data as staging_*.csv for audit and replayability of the pipeline.
    Why: row data stored as is enables debugging and ETL validation.

    """
    df_sales = pd.read_csv('csv_files\sales_data.csv')
    df_campaigns = pd.read_csv('csv_files\marketing_campaigns.csv')

    print(f"✅ Extracted: {len(df_sales)} sales rows, {len(df_campaigns)} campaigns rows")
    print(f"   Sales shape: {df_sales.shape}, Campaigns shape: {df_campaigns.shape}")
    return df_sales, df_campaigns


def transform_fact_sales(df_sales, df_campaigns):
    """
    ETL Step 2: TRANSFORM - Create fact_sales with validation and campaign join

    Cleaning data:
    1. price_per_unit < 0 → DISCARD: Minus prices does not make business sense
    2. product_id IS NULL → DISCARD: Lack foreign key => prevents join with campaigns
    3. customer_id IS NULL → DISCARD: No id client prevents analysis
    4. total_sales = quantity * price_per_unit → calc of basic metric

    JOIN locgic:
    - LEFT JOIN foreign key > product_id
    - Filtr: transaction_date BETWEEN start_date AND end_date
    - Edge case OVERLAP: GROUP BY + MAX(start_date) chooses latest campaign
    - Result: Each transaction has assign to campaign or 'No Campaign'

    Business profit: ROI campaign analysis
    """
    """
    ETL Step 2: TRANSFORM - Creates fact_sales with validation and campaign join
    
    Data Cleaning (WHY and WHAT FOR):
    1 price_per_unit < 0 → DISCARD: Negative prices are data errors, make no business sense
    2 product_id IS NULL → DISCARD: Missing foreign key prevents join with campaigns
    3 customer_id IS NULL → DISCARD: Missing customer identifier prevents analysis
    4 total_sales = quantity * price_per_unit → Calculation of basic metric
    
    JOIN LOGIC (WHAT FOR and HOW):
    - LEFT JOIN on product_id (foreign key)
    - Filter: transaction_date BETWEEN start_date AND end_date
    - Edge case OVERLAP: GROUP BY + MAX(start_date) selects latest campaign
    - Result: Each transaction has assigned campaign or 'No Campaign'
    Business Benefit: Ability to analyze campaign ROI (how much sales each generated)
    """
    # 1. Date validation
    df_sales['transaction_date'] = pd.to_datetime(df_sales['transaction_date'])
    df_campaigns['start_date'] = pd.to_datetime(df_campaigns['start_date'])
    df_campaigns['end_date'] = pd.to_datetime(df_campaigns['end_date'])

    # 2. Cleaning: delete invalid rows
    print("🔧 Cleaning data...")
    initial_rows = len(df_sales)

    # Filtr 1: price_per_unit >= 0
    df_clean = df_sales[df_sales['price_per_unit'] >= 0].copy()
    print(f"   Removed {initial_rows - len(df_clean)} rows with negative prices")

    # Filtr 2: Remove NULL product_id/customer_id
    mask_valid_keys = df_clean['product_id'].notna() & df_clean['customer_id'].notna()
    df_clean = df_clean[mask_valid_keys].copy()
    print(f"   Removed {mask_valid_keys.sum() - len(df_clean)} rows with NULL product/customer_id")

    # 3. Calculate total_sales
    df_clean['total_sales'] = df_clean['quantity'] * df_clean['price_per_unit']

    # 4. JOIN
    df_merged = pd.merge(df_clean, df_campaigns, on='product_id', how='left')

    # 5. Filter for active campaigns (BETWEEN dates)
    df_merged['is_active_campaign'] = (
            (df_merged['transaction_date'] >= df_merged['start_date']) &
            (df_merged['transaction_date'] <= df_merged['end_date'])
    )
    active_campaigns = df_merged[df_merged['is_active_campaign']].copy()

    # 6. SORT + DROP DUPLICATES (without GROUP BY!)
    df_fact = active_campaigns.sort_values('start_date', ascending=False) \
        .drop_duplicates(subset=['transaction_id'], keep='first')

    # for transaction without campaign
    no_campaign_mask = ~df_clean['transaction_id'].isin(df_fact['transaction_id'])
    df_no_campaign = df_clean[no_campaign_mask].copy()
    df_no_campaign['campaign_name'] = 'No Campaign'

    # Join
    df_fact = pd.concat([df_fact, df_no_campaign], ignore_index=True)

    # save
    df_fact.to_csv('df_fact.csv', index=False)
    # optional Excel file
    df_fact.to_excel("df_fact.xlsx", index=False)
    print(f"df_fact created: {len(df_fact):,} rows")

    return df_fact


def load_bi_summary(df_fact):
    """
    ETL Step 3: LOAD - aggregation to bi_sales_summary (reporting table)

    Required Structure (optimal for BI):
    - region (geographic metric )
    - campaign_name (marketing metric )
    - transaction_month (time metrics YYYY-MM)
    - total_sales (metric SUM)
    - sales_count (metric COUNT transactions)

    Usage: Dashboards BI (PowerBI/Tableau) - szybkie zapytania GROUP BY
    """
    df_fact['transaction_month'] = df_fact['transaction_date'].dt.strftime('%Y-%m')

    bi_sales_summary = df_fact.groupby(['region', 'campaign_name', 'transaction_month'], as_index=False).agg({
        'total_sales': 'sum',
        'transaction_id': 'count'
    }).rename(columns={'transaction_id': 'sales_count'})

    # Sort dla czytelności
    bi_sales_summary = bi_sales_summary.sort_values(['region', 'transaction_month', 'total_sales'], ascending=[True, True, False])
    # save to csv
    bi_sales_summary.to_csv('bi_sales_summary.csv', index=False)
    #  save to excel
    bi_sales_summary.to_excel("bi_sales_summary.xlsx", index=False)

    print(f"✅ bi_sales_summary created: {len(bi_sales_summary)} rows")
    print("\n📊 Sample output:")
    print(bi_sales_summary.head(10).round(2))

    return bi_sales_summary


def additional_data_quality_checks(df_sales_raw, df_fact, df_summary):
    """
    ADDITIONAL DATA QUALITY CHECKS (suggestions):
    1. Duplicate transaction_id
    2. Outliers in total_sales (e.g. >99th percentile)
    3. Region and campaign_name validation
    4. Temporal distribution (no gaps in months)
    """
    print("\n🔍 Data Quality Checks:")

    # 1. duplicates in transactions
    dupl = df_sales_raw['transaction_id'].duplicated().sum()
    print(f"   Duplicate transaction_id: {dupl}")

    # 2. Outliers total_sales
    p99 = df_fact['total_sales'].quantile(0.99)
    outliers = len(df_fact[df_fact['total_sales'] > p99])
    print(f"   Potential outliers (>P99 total_sales=${p99:,.0f}): {outliers}")

    # 3. unique values
    print(f"   Unique regions: {df_fact['region'].nunique()}")
    print(f"   Unique campaigns: {df_fact['campaign_name'].nunique()}")

    # 4. monthly distribution
    monthly_trend = df_summary.groupby('transaction_month')['total_sales'].sum()
    print(f"   Monthly sales trend (last 3):")
    print(monthly_trend.tail(3).round(0))


def etl_pipeline():
    """Main ETL pipeline orchestrator"""
    print("🚀 === SALES ETL PIPELINE ===\n")

    # ETL Steps
    df_sales_raw, df_campaigns = extract_data()

    print('df_sales_raw\n', df_sales_raw)
    print('df_campaigns\n', df_campaigns)
    df_fact = transform_fact_sales(df_sales_raw, df_campaigns)
    # df_fact save to csv
    df_fact.to_csv('df_fact.csv', index=False)
    bi_sales_summary = load_bi_summary(df_fact)

    # Quality checks - I did that additionally
    additional_data_quality_checks(df_sales_raw, df_fact, bi_sales_summary)

    print('additionall conclusion:')
    print('there is no scale, amount of data are not enough to prepare scalable analysis and evaluate marketing campaings')
    print('\nI assume this is only sample data for this exercise')
    print('additionally  marketing campaings should be evaluate like we did in this analysis'
          'not after campaign time or bought impressions \nbut after impresion + click in campaign banner and count redirects (ecommerce shop)')
    print('that is efficient way to evaluate marketing campaings')

    print('\nI planned also add auto generating PDF file with whole ready to sale team analysis report but I ran out of time')
    print('in one of project on gihtub I did that example how to use ready library to generate PDF reports to test in python ')
    print('same would be done for this analysis to deliver ready to use report for sales team')

    print("\n✅ ETL COMPLETED!")
    print("📁 Generated files:")
    print("   • df_fact.csv (clean transactions)")
    print("   • bi_sales_summary.csv (required data to power bi, tableau etc.)")


def run_tests():
    """Pytest-style testy weryfikacyjne (TAK - bardzo sensowne!)"""
    df_sales, _ = extract_data()
    df_fact = transform_fact_sales(df_sales, pd.read_csv('csv_files\marketing_campaigns.csv'))

    # Test 1: Walidacja filtrów
    assert df_fact['price_per_unit'].min() >= 0, "Negative prices not filtered"
    assert df_fact['product_id'].notna().all(), "NULL product_id remains"
    assert df_fact['customer_id'].notna().all(), "NULL customer_id remains"

    # Test 2: total_sales calculation
    sample_row = df_fact.iloc[0]
    assert abs(sample_row['total_sales'] - (sample_row['quantity'] * sample_row['price_per_unit'])) < 0.01

    # Test 3: Summary aggregation
    df_summary = load_bi_summary(df_fact)
    assert len(df_summary) > 0, "Summary is empty"

    print("✅ All tests PASSED!")
    return True


if __name__ == "__main__":
    etl_pipeline()
    run_tests()  # I didn't implement as many as tests I wanted. These are basic ones.
    # I generated more code and test but couldnt finish in time and validate everything
    # I also didnt seperate logic(functions) to modules, I assume most importat was show etl pipeline and analysis skills
