"""
Alpaca API Client Wrapper

Provides a simple interface to interact with Alpaca Trading API for:
- Account management
- Portfolio positions
- Order management
- Market data
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest, GetOrdersRequest
    from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockLatestQuoteRequest
except ImportError:
    print("ERROR: alpaca-py not installed. Run: pip install alpaca-py")
    raise

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class AlpacaClient:
    """Wrapper for Alpaca Trading API."""
    
    def __init__(self):
        """Initialize Alpaca client with credentials from .env file."""
        api_key = os.getenv('ALPACA_API_KEY')
        api_secret = os.getenv('ALPACA_API_SECRET')
        
        if not api_key or not api_secret:
            raise ValueError("ALPACA_API_KEY and ALPACA_API_SECRET must be set in .env file")
        
        # Determine if paper or live trading
        endpoint = os.getenv('ALPACA_ENDPOINT', 'https://paper-api.alpaca.markets')
        self.is_paper = 'paper' in endpoint
        
        # Initialize clients
        self.trading_client = TradingClient(api_key, api_secret, paper=self.is_paper)
        self.data_client = StockHistoricalDataClient(api_key, api_secret)
        
        logger.info(f"Alpaca client initialized ({'PAPER' if self.is_paper else 'LIVE'} trading)")
    
    def get_account(self) -> Dict:
        """Get account information."""
        try:
            account = self.trading_client.get_account()
            return {
                'equity': float(account.equity),
                'cash': float(account.cash),
                'buying_power': float(account.buying_power),
                'portfolio_value': float(account.portfolio_value),
                'pattern_day_trader': account.pattern_day_trader,
                'trading_blocked': account.trading_blocked,
                'account_blocked': account.account_blocked,
            }
        except Exception as e:
            logger.error(f"Error getting account info: {e}")
            raise
    
    def get_positions(self) -> List[Dict]:
        """Get all open positions."""
        try:
            positions = self.trading_client.get_all_positions()
            return [
                {
                    'symbol': pos.symbol,
                    'qty': float(pos.qty),
                    'avg_entry_price': float(pos.avg_entry_price),
                    'current_price': float(pos.current_price),
                    'market_value': float(pos.market_value),
                    'cost_basis': float(pos.cost_basis),
                    'unrealized_pl': float(pos.unrealized_pl),
                    'unrealized_plpc': float(pos.unrealized_plpc),
                    'side': pos.side,
                }
                for pos in positions
            ]
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []
    
    def get_position(self, symbol: str) -> Optional[Dict]:
        """Get position for specific symbol."""
        try:
            pos = self.trading_client.get_open_position(symbol)
            return {
                'symbol': pos.symbol,
                'qty': float(pos.qty),
                'avg_entry_price': float(pos.avg_entry_price),
                'current_price': float(pos.current_price),
                'market_value': float(pos.market_value),
                'cost_basis': float(pos.cost_basis),
                'unrealized_pl': float(pos.unrealized_pl),
                'unrealized_plpc': float(pos.unrealized_plpc),
            }
        except Exception as e:
            # Position doesn't exist
            return None
    
    def get_latest_price(self, symbol: str) -> Optional[float]:
        """Get latest price for a symbol."""
        try:
            request = StockLatestQuoteRequest(symbol_or_symbols=[symbol])
            quotes = self.data_client.get_stock_latest_quote(request)
            if symbol in quotes:
                return float(quotes[symbol].ask_price)
            return None
        except Exception as e:
            logger.error(f"Error getting price for {symbol}: {e}")
            return None
    
    def place_market_order(self, symbol: str, qty: int, side: str) -> Optional[Dict]:
        """
        Place a market order.
        
        Args:
            symbol: Stock symbol
            qty: Quantity to buy/sell
            side: 'buy' or 'sell'
            
        Returns:
            Order details if successful, None otherwise
        """
        try:
            order_side = OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL
            
            order_data = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=order_side,
                time_in_force=TimeInForce.DAY
            )
            
            order = self.trading_client.submit_order(order_data)
            
            logger.info(f"Market order placed: {side.upper()} {qty} shares of {symbol}")
            
            return {
                'id': str(order.id),
                'symbol': order.symbol,
                'qty': float(order.qty),
                'side': order.side.value,
                'type': order.type.value,
                'status': order.status.value,
                'submitted_at': order.submitted_at,
            }
        except Exception as e:
            logger.error(f"Error placing market order for {symbol}: {e}")
            return None
    
    def place_limit_order(self, symbol: str, qty: int, side: str, limit_price: float) -> Optional[Dict]:
        """
        Place a limit order.
        
        Args:
            symbol: Stock symbol
            qty: Quantity to buy/sell
            side: 'buy' or 'sell'
            limit_price: Limit price
            
        Returns:
            Order details if successful, None otherwise
        """
        try:
            order_side = OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL
            
            order_data = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=order_side,
                time_in_force=TimeInForce.DAY,
                limit_price=limit_price
            )
            
            order = self.trading_client.submit_order(order_data)
            
            logger.info(f"Limit order placed: {side.upper()} {qty} shares of {symbol} @ ${limit_price}")
            
            return {
                'id': str(order.id),
                'symbol': order.symbol,
                'qty': float(order.qty),
                'side': order.side.value,
                'type': order.type.value,
                'status': order.status.value,
                'limit_price': float(order.limit_price),
                'submitted_at': order.submitted_at,
            }
        except Exception as e:
            logger.error(f"Error placing limit order for {symbol}: {e}")
            return None
    
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order by ID."""
        try:
            self.trading_client.cancel_order_by_id(order_id)
            logger.info(f"Order {order_id} cancelled")
            return True
        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {e}")
            return False
    
    def get_orders(self, status: str = 'open') -> List[Dict]:
        """
        Get orders by status.
        
        Args:
            status: 'open', 'closed', or 'all'
            
        Returns:
            List of orders
        """
        try:
            if status == 'open':
                order_status = QueryOrderStatus.OPEN
            elif status == 'closed':
                order_status = QueryOrderStatus.CLOSED
            else:
                order_status = QueryOrderStatus.ALL
            
            request = GetOrdersRequest(status=order_status)
            orders = self.trading_client.get_orders(request)
            
            return [
                {
                    'id': str(order.id),
                    'symbol': order.symbol,
                    'qty': float(order.qty),
                    'side': order.side.value,
                    'type': order.type.value,
                    'status': order.status.value,
                    'submitted_at': order.submitted_at,
                    'filled_at': order.filled_at,
                }
                for order in orders
            ]
        except Exception as e:
            logger.error(f"Error getting orders: {e}")
            return []
    
    def close_position(self, symbol: str) -> bool:
        """Close entire position for a symbol."""
        try:
            self.trading_client.close_position(symbol)
            logger.info(f"Position closed for {symbol}")
            return True
        except Exception as e:
            logger.error(f"Error closing position for {symbol}: {e}")
            return False
    
    def is_market_open(self) -> bool:
        """Check if market is currently open."""
        try:
            clock = self.trading_client.get_clock()
            return clock.is_open
        except Exception as e:
            logger.error(f"Error checking market status: {e}")
            return False
