"""
GROUND TRUTH TESTING FRAMEWORK
For validating iceberg order detection accuracy

This module provides tools to:
1. Collect labeled ground truth data
2. Run backtests against historical data
3. Calculate precision, recall, F1-score
4. Generate validation reports
"""
import json
import asyncio
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from collections import defaultdict
import numpy as np


@dataclass
class GroundTruthLabel:
    """A labeled iceberg order for validation"""
    exchange: str
    symbol: str
    side: str  # 'buy' or 'sell'
    price: float
    timestamp: datetime
    
    # Ground truth values
    is_iceberg: bool
    estimated_hidden_volume: Optional[float] = None
    confidence_level: str = "medium"  # low, medium, high
    
    # Source of label
    label_source: str = "manual"  # manual, whale_alert, exchange_api, known_trader
    notes: str = ""
    
    def to_dict(self) -> Dict:
        return {
            'exchange': self.exchange,
            'symbol': self.symbol,
            'side': self.side,
            'price': self.price,
            'timestamp': self.timestamp.isoformat(),
            'is_iceberg': self.is_iceberg,
            'estimated_hidden_volume': self.estimated_hidden_volume,
            'confidence_level': self.confidence_level,
            'label_source': self.label_source,
            'notes': self.notes
        }


@dataclass
class DetectionResult:
    """A detection from our algorithm"""
    side: str
    price: float
    hidden_volume: float
    confidence: float
    detection_method: str
    timestamp: datetime


@dataclass
class ValidationMetrics:
    """Validation metrics for the detector"""
    true_positives: int = 0
    false_positives: int = 0
    true_negatives: int = 0
    false_negatives: int = 0
    
    @property
    def precision(self) -> float:
        """Precision = TP / (TP + FP)"""
        if self.true_positives + self.false_positives == 0:
            return 0.0
        return self.true_positives / (self.true_positives + self.false_positives)
    
    @property
    def recall(self) -> float:
        """Recall = TP / (TP + FN)"""
        if self.true_positives + self.false_negatives == 0:
            return 0.0
        return self.true_positives / (self.true_positives + self.false_negatives)
    
    @property
    def f1_score(self) -> float:
        """F1 = 2 * (Precision * Recall) / (Precision + Recall)"""
        p = self.precision
        r = self.recall
        if p + r == 0:
            return 0.0
        return 2 * (p * r) / (p + r)
    
    @property
    def accuracy(self) -> float:
        """Accuracy = (TP + TN) / Total"""
        total = self.true_positives + self.false_positives + self.true_negatives + self.false_negatives
        if total == 0:
            return 0.0
        return (self.true_positives + self.true_negatives) / total
    
    def to_dict(self) -> Dict:
        return {
            'true_positives': self.true_positives,
            'false_positives': self.false_positives,
            'true_negatives': self.true_negatives,
            'false_negatives': self.false_negatives,
            'precision': self.precision,
            'recall': self.recall,
            'f1_score': self.f1_score,
            'accuracy': self.accuracy
        }


class GroundTruthCollector:
    """Collect ground truth labels for validation"""
    
    def __init__(self, output_file: str = "ground_truth_labels.json"):
        self.output_file = output_file
        self.labels: List[GroundTruthLabel] = []
    
    def add_manual_label(
        self,
        exchange: str,
        symbol: str,
        side: str,
        price: float,
        timestamp: datetime,
        is_iceberg: bool,
        estimated_hidden_volume: Optional[float] = None,
        notes: str = ""
    ):
        """Manually add a ground truth label"""
        label = GroundTruthLabel(
            exchange=exchange,
            symbol=symbol,
            side=side,
            price=price,
            timestamp=timestamp,
            is_iceberg=is_iceberg,
            estimated_hidden_volume=estimated_hidden_volume,
            label_source="manual",
            notes=notes
        )
        self.labels.append(label)
    
    async def collect_from_whale_alerts(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[GroundTruthLabel]:
        """
        Collect labels from whale alert services
        
        Note: This requires integration with whale alert APIs
        Popular services:
        - Whale Alert (whale-alert.io)
        - CryptoQuant
        - Glassnode
        """
        # Placeholder - implement actual API integration
        print(f"TODO: Implement whale alert API for {symbol}")
        return []
    
    async def collect_from_known_traders(
        self,
        exchange: str,
        known_addresses: List[str]
    ) -> List[GroundTruthLabel]:
        """
        Collect labels from known large trader addresses
        
        Note: Requires blockchain analysis or exchange API access
        """
        # Placeholder
        print(f"TODO: Implement known trader tracking for {exchange}")
        return []
    
    def save_labels(self):
        """Save labels to JSON file"""
        labels_dict = [label.to_dict() for label in self.labels]
        
        with open(self.output_file, 'w') as f:
            json.dump(labels_dict, f, indent=2, default=str)
        
        print(f"Saved {len(self.labels)} labels to {self.output_file}")
    
    def load_labels(self) -> List[GroundTruthLabel]:
        """Load labels from JSON file"""
        try:
            with open(self.output_file, 'r') as f:
                data = json.load(f)
            
            self.labels = []
            for item in data:
                label = GroundTruthLabel(
                    exchange=item['exchange'],
                    symbol=item['symbol'],
                    side=item['side'],
                    price=item['price'],
                    timestamp=datetime.fromisoformat(item['timestamp']),
                    is_iceberg=item['is_iceberg'],
                    estimated_hidden_volume=item.get('estimated_hidden_volume'),
                    confidence_level=item.get('confidence_level', 'medium'),
                    label_source=item.get('label_source', 'manual'),
                    notes=item.get('notes', '')
                )
                self.labels.append(label)
            
            print(f"Loaded {len(self.labels)} labels from {self.output_file}")
            return self.labels
            
        except FileNotFoundError:
            print(f"No labels file found at {self.output_file}")
            return []


class DetectionValidator:
    """Validate detector performance against ground truth"""
    
    def __init__(self, price_tolerance: float = 0.01, time_tolerance_seconds: int = 60):
        """
        Args:
            price_tolerance: Price matching tolerance (1% default)
            time_tolerance_seconds: Time window for matching (60s default)
        """
        self.price_tolerance = price_tolerance
        self.time_tolerance_seconds = time_tolerance_seconds
    
    def validate_detections(
        self,
        detections: List[Dict],
        ground_truth: List[GroundTruthLabel]
    ) -> ValidationMetrics:
        """
        Validate detections against ground truth
        
        Returns ValidationMetrics with TP, FP, TN, FN
        """
        metrics = ValidationMetrics()
        
        # Track which ground truth items were matched
        matched_ground_truth = set()
        
        # Check each detection
        for detection in detections:
            is_match = False
            
            for i, gt in enumerate(ground_truth):
                if self._is_match(detection, gt):
                    if gt.is_iceberg:
                        metrics.true_positives += 1
                        matched_ground_truth.add(i)
                        is_match = True
                        break
            
            if not is_match:
                # Detection but no matching ground truth iceberg
                metrics.false_positives += 1
        
        # Count false negatives (ground truth icebergs not detected)
        for i, gt in enumerate(ground_truth):
            if gt.is_iceberg and i not in matched_ground_truth:
                metrics.false_negatives += 1
        
        # Note: True negatives are hard to count without exhaustive labeling
        # of ALL price levels that are NOT icebergs
        
        return metrics
    
    def _is_match(self, detection: Dict, ground_truth: GroundTruthLabel) -> bool:
        """Check if detection matches ground truth label"""
        # Check side
        if detection['side'] != ground_truth.side:
            return False
        
        # Check price (within tolerance)
        price_diff = abs(detection['price'] - ground_truth.price)
        if price_diff > ground_truth.price * self.price_tolerance:
            return False
        
        # Check timestamp (within tolerance)
        det_time = datetime.fromisoformat(detection['timestamp'].replace('Z', '+00:00'))
        time_diff = abs((det_time - ground_truth.timestamp).total_seconds())
        if time_diff > self.time_tolerance_seconds:
            return False
        
        return True
    
    def generate_report(
        self,
        metrics: ValidationMetrics,
        detections: List[Dict],
        ground_truth: List[GroundTruthLabel]
    ) -> Dict:
        """Generate detailed validation report"""
        
        # Analyze detections by confidence level
        high_conf = [d for d in detections if d['confidence'] > 0.7]
        medium_conf = [d for d in detections if 0.5 < d['confidence'] <= 0.7]
        low_conf = [d for d in detections if d['confidence'] <= 0.5]
        
        # Analyze by detection method
        methods = defaultdict(int)
        for d in detections:
            methods[d.get('detection_method', 'unknown')] += 1
        
        report = {
            'summary': {
                'total_detections': len(detections),
                'total_ground_truth': len(ground_truth),
                'ground_truth_icebergs': len([gt for gt in ground_truth if gt.is_iceberg]),
            },
            'metrics': metrics.to_dict(),
            'detection_breakdown': {
                'high_confidence': len(high_conf),
                'medium_confidence': len(medium_conf),
                'low_confidence': len(low_conf)
            },
            'detection_methods': dict(methods),
            'recommendations': self._generate_recommendations(metrics, detections)
        }
        
        return report
    
    def _generate_recommendations(
        self,
        metrics: ValidationMetrics,
        detections: List[Dict]
    ) -> List[str]:
        """Generate improvement recommendations based on metrics"""
        recommendations = []
        
        if metrics.precision < 0.6:
            recommendations.append(
                f"LOW PRECISION ({metrics.precision:.2%}): "
                "Too many false positives. Consider:"
                "\n  - Increasing confidence threshold"
                "\n  - Tightening detection criteria"
                "\n  - Filtering out market maker activity"
            )
        
        if metrics.recall < 0.6:
            recommendations.append(
                f"LOW RECALL ({metrics.recall:.2%}): "
                "Missing real icebergs. Consider:"
                "\n  - Decreasing confidence threshold"
                "\n  - Adding more detection methods"
                "\n  - Increasing price tolerance"
            )
        
        if metrics.f1_score > 0.7:
            recommendations.append(
                f"GOOD PERFORMANCE (F1={metrics.f1_score:.2%}): "
                "Detector is working well. Fine-tune for specific use cases."
            )
        
        # Analyze confidence distribution
        avg_conf = np.mean([d['confidence'] for d in detections]) if detections else 0
        if avg_conf < 0.5:
            recommendations.append(
                "LOW AVERAGE CONFIDENCE: "
                "Detector is uncertain. May need more training data or better features."
            )
        
        return recommendations


class BacktestRunner:
    """Run backtests on historical data"""
    
    def __init__(self, detector, exchange):
        """
        Args:
            detector: IcebergDetectorImproved instance
            exchange: Exchange instance (e.g., BinanceExchangeImproved)
        """
        self.detector = detector
        self.exchange = exchange
    
    async def run_backtest(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval_minutes: int = 5
    ) -> List[Dict]:
        """
        Run backtest over historical period
        
        Note: This requires historical orderbook + trade data
        Most exchanges don't provide full historical orderbook data via public API
        
        Options:
        1. Use historical data from paid providers (Kaiko, CryptoCompare)
        2. Collect and store your own data over time
        3. Use exchange historical trade data (less accurate)
        """
        print(f"Backtesting {symbol} from {start_time} to {end_time}")
        
        detections = []
        current_time = start_time
        
        while current_time < end_time:
            try:
                # In real implementation, fetch historical data for this timestamp
                # For now, using live data as example
                orderbook = await self.exchange.fetch_orderbook(symbol)
                trades = await self.exchange.fetch_trades(symbol)
                
                # Run detection
                result = await self.detector.detect(
                    orderbook=orderbook,
                    trades=trades,
                    exchange=self.exchange.name,
                    symbol=symbol
                )
                
                detections.extend(result['icebergs'])
                
                # Move to next interval
                current_time += timedelta(minutes=interval_minutes)
                await asyncio.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"Error at {current_time}: {e}")
                current_time += timedelta(minutes=interval_minutes)
        
        return detections


# ============================================================================
# PRACTICAL GROUND TRUTH COLLECTION STRATEGIES
# ============================================================================

class GroundTruthStrategies:
    """
    Practical strategies for collecting ground truth data
    """
    
    @staticmethod
    def strategy_1_manual_observation():
        """
        STRATEGY 1: Manual Observation & Labeling
        
        Best for: Small-scale validation, initial testing
        Effort: High
        Accuracy: High (if done carefully)
        
        Steps:
        1. Watch orderbook in real-time on exchange
        2. Identify suspected icebergs manually:
           - Large orders that keep refilling
           - Price levels with unusual trade volume
        3. Label these in your dataset
        4. Run your detector and compare
        """
        return """
        MANUAL OBSERVATION PROTOCOL:
        
        1. Select high-volume trading pair (e.g., BTC/USDT on Binance)
        2. Watch for 30-60 minutes during active hours
        3. Document any price levels with:
           - Repeated refills after trades
           - Trade volume >> visible volume
           - Consistent presence over time
        
        4. For each suspected iceberg, record:
           - Exact price
           - Side (bid/ask)
           - Timestamp of first observation
           - Approximate hidden volume (estimate)
           - Any supporting evidence
        
        5. Also record NON-icebergs (regular large orders) for negative examples
        
        6. Save to ground_truth_labels.json
        """
    
    @staticmethod
    def strategy_2_exchange_data():
        """
        STRATEGY 2: Use Exchange Order Type Data
        
        Best for: High accuracy if available
        Effort: Low (if API available)
        Accuracy: Very High
        
        Some exchanges expose order types in their API:
        - Binance: Some endpoints show iceberg flag
        - FTX (defunct): Had iceberg order types
        - Coinbase Advanced: Shows partial fills
        """
        return """
        EXCHANGE DATA APPROACH:
        
        1. Check if exchange API provides:
           - Order types in fills
           - Iceberg order flags
           - Partial fill information
        
        2. Binance example:
           - User Data Stream shows icebergQty field
           - Requires authenticated API
           - Only for your own orders
        
        3. Alternative: Contact exchanges for research data
           - Some provide historical data for research
           - Usually requires academic/commercial agreement
        """
    
    @staticmethod
    def strategy_3_whale_tracking():
        """
        STRATEGY 3: Known Whale/Institutional Addresses
        
        Best for: Crypto with on-chain visibility
        Effort: Medium
        Accuracy: Medium-High
        
        Track known large traders and their patterns
        """
        return """
        WHALE TRACKING METHOD:
        
        1. Identify known large traders:
           - Use Whale Alert API
           - Track known institutional addresses
           - Monitor large wallet movements
        
        2. Correlate on-chain to exchange:
           - Large deposits → likely upcoming trades
           - Known trader deposit addresses
        
        3. Watch orderbooks after deposits:
           - Large traders often use icebergs
           - Can label their activity with high confidence
        
        4. Services to use:
           - whale-alert.io API
           - CryptoQuant
           - Glassnode
           - Nansen
        """
    
    @staticmethod
    def strategy_4_statistical_baseline():
        """
        STRATEGY 4: Statistical Baseline Validation
        
        Best for: When ground truth is limited
        Effort: Medium
        Accuracy: Medium
        
        Use statistical properties to validate detector
        """
        return """
        STATISTICAL VALIDATION:
        
        Instead of labeling individual icebergs, validate statistically:
        
        1. Test on different market conditions:
           - High volatility vs low volatility
           - Active hours vs quiet hours
           - Bull market vs bear market
        
        2. Check for expected patterns:
           - More detections during active hours ✓
           - Larger icebergs on major pairs ✓
           - Detection distribution makes sense ✓
        
        3. Cross-exchange validation:
           - Same pair on multiple exchanges
           - Should show similar patterns
        
        4. Volume correlation:
           - Detected icebergs should correlate with:
             * Large trades
             * Price stability at level
             * Repeated refills
        """


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

async def example_usage():
    """Example of how to use the ground truth framework"""
    
    # 1. Collect ground truth labels
    collector = GroundTruthCollector("my_labels.json")
    
    # Manual labeling example
    collector.add_manual_label(
        exchange="binance",
        symbol="BTC/USDT",
        side="buy",
        price=42000.50,
        timestamp=datetime.now(),
        is_iceberg=True,
        estimated_hidden_volume=10.5,
        notes="Observed repeated refills over 30 minutes"
    )
    
    # Save labels
    collector.save_labels()
    
    # 2. Load labels for validation
    labels = collector.load_labels()
    
    # 3. Run your detector and get detections
    # detections = await your_detector.detect(...)
    detections = []  # Placeholder
    
    # 4. Validate
    validator = DetectionValidator(
        price_tolerance=0.01,  # 1%
        time_tolerance_seconds=120  # 2 minutes
    )
    
    metrics = validator.validate_detections(detections, labels)
    
    # 5. Generate report
    report = validator.generate_report(metrics, detections, labels)
    
    print("Validation Report:")
    print(f"Precision: {metrics.precision:.2%}")
    print(f"Recall: {metrics.recall:.2%}")
    print(f"F1 Score: {metrics.f1_score:.2%}")
    print("\nRecommendations:")
    for rec in report['recommendations']:
        print(f"- {rec}")


if __name__ == "__main__":
    # Print strategy guides
    print("=" * 80)
    print("GROUND TRUTH COLLECTION STRATEGIES")
    print("=" * 80)
    
    print("\n" + GroundTruthStrategies.strategy_1_manual_observation())
    print("\n" + GroundTruthStrategies.strategy_2_exchange_data())
    print("\n" + GroundTruthStrategies.strategy_3_whale_tracking())
    print("\n" + GroundTruthStrategies.strategy_4_statistical_baseline())
    
    # Run example
    # asyncio.run(example_usage())
