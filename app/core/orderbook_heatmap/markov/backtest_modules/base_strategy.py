"""
Base Strategy Class

Abstract base class for all trading strategies.
All custom strategies should inherit from this class.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime


class BaseStrategy(ABC):
    """
    Abstract base class for trading strategies.

    All strategies must implement the required abstract methods.
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        """
        Initialize the strategy.

        Args:
            params: Dictionary of strategy parameters
        """
        self.params = params or {}
        self.name = self.__class__.__name__
        self.positions = {}
        self.trades = []
        self.current_date = None

    @abstractmethod
    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate trading signals based on the data.

        Args:
            data: DataFrame with OHLCV data

        Returns:
            DataFrame with signals (1 for buy, -1 for sell, 0 for hold)
        """
        pass

    @abstractmethod
    def on_bar(self, bar: pd.Series) -> Optional[Dict[str, Any]]:
        """
        Process a single bar of data.

        Args:
            bar: Series containing OHLCV data for a single period

        Returns:
            Dictionary with order details or None
        """
        pass

    def validate_params(self) -> bool:
        """
        Validate strategy parameters.

        Returns:
            True if parameters are valid, False otherwise
        """
        return True

    def get_parameter(self, key: str, default: Any = None) -> Any:
        """
        Get a strategy parameter.

        Args:
            key: Parameter name
            default: Default value if parameter not found

        Returns:
            Parameter value
        """
        return self.params.get(key, default)

    def set_parameter(self, key: str, value: Any) -> None:
        """
        Set a strategy parameter.

        Args:
            key: Parameter name
            value: Parameter value
        """
        self.params[key] = value

    def reset(self) -> None:
        """Reset strategy state."""
        self.positions = {}
        self.trades = []
        self.current_date = None

    def get_positions(self) -> Dict[str, Any]:
        """
        Get current positions.

        Returns:
            Dictionary of current positions
        """
        return self.positions.copy()

    def get_trades(self) -> List[Dict[str, Any]]:
        """
        Get trade history.

        Returns:
            List of executed trades
        """
        return self.trades.copy()

    def __str__(self) -> str:
        """String representation of the strategy."""
        return f"{self.name}({self.params})"

    def __repr__(self) -> str:
        """Detailed string representation."""
        return self.__str__()
