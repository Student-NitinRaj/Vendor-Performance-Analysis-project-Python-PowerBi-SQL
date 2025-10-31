import sqlite3
import pandas as pd
import logging

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    """
    This function will merge the different tables to get the overall vendor summary
    and add new columns in the resultant data
    """
    query = """
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC;
    """

    vendor_sales_summary = pd.read_sql_query(query, conn)
    return vendor_sales_summary


def clean_data(df):
    """
    This function will clean the data
    """
    # Changing datatype to float
    df['Volume'] = df['Volume'].astype(float)

    # Filling missing values with 0
    df.fillna(0, inplace=True)

    # Removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # Creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    return df


def ingest_db(df, table_name, conn):
    """Ingests the cleaned dataframe into the SQLite DB"""
    df.to_sql(table_name, conn, if_exists="replace", index=False)


if __name__ == '__main__':
    try:
        # creating database connection
        conn = sqlite3.connect('inventory.db')

        logging.info('Creating Vendor Summary Table.....')
        summary_df = create_vendor_summary(conn)

        logging.info(f"Summary DataFrame shape: {summary_df.shape}")
        logging.debug(f"\n{summary_df.head()}")

        logging.info('Cleaning Data.....')
        clean_df = clean_data(summary_df)

        logging.info(f"Cleaned DataFrame shape: {clean_df.shape}")
        logging.debug(f"\n{clean_df.head()}")

        logging.info('Ingesting data into database.....')
        ingest_db(clean_df, 'vendor_sales_summary', conn)

        logging.info('Pipeline Completed Successfully ✅')

    except Exception as e:
        logging.error(f"Error occurred: {e}")

    finally:
        conn.close()