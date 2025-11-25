import os
import psycopg2
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dotenv import load_dotenv

def plot_candlestick(symbol: str, start_date: str, end_date: str):
    """
    Query PostgreSQL for OHLCV data and plot a candlestick chart using Plotly.
    Args:
        symbol (str): Stock symbol
        start_date (str): Start date (YYYY-MM-DD)
        end_date (str): End date (YYYY-MM-DD)
    """
    load_dotenv()
    db_params = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT'),
        'dbname': os.getenv('POSTGRES_DATABASE'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
    }
    conn = psycopg2.connect(**db_params)
    query = f"""
        SELECT date, open, high, low, close, volume
        FROM raw.time_series_daily_adjusted
        WHERE symbol = %s AND date BETWEEN %s AND %s
        ORDER BY date ASC
    """
    df = pd.read_sql(query, conn, params=(symbol, start_date, end_date))
    conn.close()
    if df.empty:
        print(f"No data found for {symbol} between {start_date} and {end_date}.")
        return
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Determine volume bar colors (green for up days, red for down days)
    df['color'] = ['green' if close >= open else 'red' 
                   for close, open in zip(df['close'], df['open'])]
    
    # Create subplots with shared x-axis
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.7, 0.3],
        subplot_titles=(f"Candlestick Chart for {symbol}", "Volume")
    )
    
    # Add candlestick chart to the first subplot
    fig.add_trace(
        go.Candlestick(
            x=df['date'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name=symbol
        ),
        row=1, col=1
    )
    
    # Add volume bars to the second subplot
    fig.add_trace(
        go.Bar(
            x=df['date'],
            y=df['volume'],
            marker_color=df['color'],
            name='Volume',
            showlegend=False
        ),
        row=2, col=1
    )

    # Add vertical quarter reference lines with shapes and annotations
    quarters = df['date'].dt.to_period('Q')
    quarter_starts = df.groupby(quarters)['date'].first()
    
    shapes = []
    annotations = []
    
    for quarter, qdate in quarter_starts.items():
        qlabel = f"{quarter.year}-Q{quarter.quarter}"
        
        # Add vertical line as a shape (spans both subplots)
        shapes.append(
            dict(
                type="line",
                x0=qdate,
                x1=qdate,
                y0=0,
                y1=1,
                yref="paper",
                line=dict(color="black", width=1, dash="dot")
            )
        )
        
        # Add annotation at the top
        annotations.append(
            dict(
                x=qdate,
                y=1,
                yref="paper",
                text=qlabel,
                showarrow=False,
                yanchor="bottom",
                font=dict(size=10, color="black")
            )
        )
    
    fig.update_layout(
        shapes=shapes,
        annotations=annotations,
        xaxis2_title="Date",
        yaxis_title="Price",
        yaxis2_title="Volume",
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False),
        xaxis2=dict(showgrid=False),
        yaxis2=dict(showgrid=False),
        height=800
    )
    fig.show()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Plot candlestick chart for a stock symbol.")
    parser.add_argument("symbol", type=str, help="Stock symbol")
    parser.add_argument("start_date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", type=str, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()
    plot_candlestick(args.symbol, args.start_date, args.end_date)
