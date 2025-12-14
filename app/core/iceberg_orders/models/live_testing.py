"""
PRACTICAL TEST SCRIPT
Run this to start collecting ground truth and testing your detector

Usage:
    python live_testing.py --mode collect    # Collect ground truth
    python live_testing.py --mode validate   # Validate detector
    python live_testing.py --mode compare    # Compare old vs new detector
"""
import asyncio
import argparse
from datetime import datetime, timedelta
import json
from typing import Dict, List


# You'll need to import your actual implementations
# from app.core.iceberg_orders.detector.iceberg_detector import IcebergDetector
# from binance_improved import BinanceExchangeImproved
# from iceberg_detector_improved import IcebergDetectorImproved
# from ground_truth_testing import GroundTruthCollector, DetectionValidator


class LiveTestingSession:
    """Interactive session for collecting ground truth"""
    
    def __init__(self):
        self.observations = []
    
    async def collect_ground_truth_interactive(
        self,
        exchange_name: str = "binance",
        symbol: str = "BTC/USDT",
        duration_minutes: int = 30
    ):
        """
        Interactive ground truth collection
        
        Watches orderbook and prompts user to label icebergs
        """
        print("=" * 80)
        print("GROUND TRUTH COLLECTION SESSION")
        print("=" * 80)
        print(f"Exchange: {exchange_name}")
        print(f"Symbol: {symbol}")
        print(f"Duration: {duration_minutes} minutes")
        print("\nInstructions:")
        print("1. Watch the orderbook updates below")
        print("2. When you spot a potential iceberg, press 'i'")
        print("3. You'll be prompted to label it")
        print("4. Press 'q' to quit early")
        print("-" * 80)
        
        # Initialize exchange (you'll need actual implementation)
        # exchange = BinanceExchangeImproved()
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        observations = []
        
        try:
            while datetime.now() < end_time:
                # Fetch current orderbook
                # orderbook = await exchange.fetch_orderbook(symbol)
                # trades = await exchange.fetch_trades(symbol, limit=20)
                
                # Display orderbook snapshot
                # self._display_orderbook(orderbook)
                
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Observing...")
                print("Press 'i' to mark iceberg, 'n' for non-iceberg, 'q' to quit")
                
                # In real implementation, use async input or keyboard library
                # For now, simplified:
                await asyncio.sleep(10)
                
                # Prompt for user input (simplified)
                # user_input = input("Action: ").strip().lower()
                
                # if user_input == 'i':
                #     obs = await self._label_iceberg(orderbook, True)
                #     observations.append(obs)
                # elif user_input == 'n':
                #     obs = await self._label_iceberg(orderbook, False)
                #     observations.append(obs)
                # elif user_input == 'q':
                #     break
        
        except KeyboardInterrupt:
            print("\n\nSession interrupted by user")
        
        # Save observations
        self._save_observations(observations, exchange_name, symbol)
        
        print(f"\n✓ Collected {len(observations)} observations")
        return observations
    
    def _display_orderbook(self, orderbook: Dict):
        """Display orderbook in terminal"""
        print("\n" + "=" * 60)
        print("ORDERBOOK SNAPSHOT")
        print("-" * 60)
        
        # Top 10 asks
        print("ASKS (Sell Orders):")
        asks = orderbook.get('asks', [])[:10]
        for i, ask in enumerate(reversed(asks)):
            price = ask.get('price', 0)
            volume = ask.get('volume', 0)
            print(f"  {price:>10.2f}  |  {volume:>8.4f}")
        
        print("-" * 60)
        
        # Top 10 bids
        print("BIDS (Buy Orders):")
        bids = orderbook.get('bids', [])[:10]
        for bid in bids:
            price = bid.get('price', 0)
            volume = bid.get('volume', 0)
            print(f"  {price:>10.2f}  |  {volume:>8.4f}")
        
        print("=" * 60)
    
    async def _label_iceberg(self, orderbook: Dict, is_iceberg: bool) -> Dict:
        """Prompt user to label an iceberg"""
        print("\n--- LABELING ---")
        
        side = input("Side (buy/sell): ").strip().lower()
        price = float(input("Price level: ").strip())
        
        estimated_volume = None
        if is_iceberg:
            estimated_volume = float(input("Estimated hidden volume (optional): ").strip() or "0") or None
        
        notes = input("Notes: ").strip()
        
        return {
            'timestamp': datetime.now().isoformat(),
            'side': side,
            'price': price,
            'is_iceberg': is_iceberg,
            'estimated_hidden_volume': estimated_volume,
            'notes': notes
        }
    
    def _save_observations(self, observations: List[Dict], exchange: str, symbol: str):
        """Save observations to file"""
        filename = f"ground_truth_{exchange}_{symbol.replace('/', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        data = {
            'exchange': exchange,
            'symbol': symbol,
            'collection_time': datetime.now().isoformat(),
            'observations': observations
        }
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\n✓ Saved to {filename}")


class DetectorComparison:
    """Compare old vs improved detector"""
    
    async def run_comparison(
        self,
        symbol: str = "BTC/USDT",
        test_duration_minutes: int = 10
    ):
        """
        Run both detectors side-by-side and compare results
        """
        print("=" * 80)
        print("DETECTOR COMPARISON TEST")
        print("=" * 80)
        
        # Initialize both detectors
        # old_detector = IcebergDetector(threshold=0.05)
        # new_detector = IcebergDetectorImproved(threshold=0.05)
        # exchange = BinanceExchangeImproved()
        
        results = {
            'old_detector': [],
            'new_detector': []
        }
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=test_duration_minutes)
        
        iteration = 0
        
        while datetime.now() < end_time:
            iteration += 1
            print(f"\n--- Iteration {iteration} ---")
            
            # Fetch data
            # orderbook = await exchange.fetch_orderbook(symbol)
            # trades = await exchange.fetch_trades(symbol)
            
            # Run old detector
            # old_result = await old_detector.detect(orderbook, trades, "binance", symbol)
            # results['old_detector'].append(old_result)
            
            # Run new detector
            # new_result = await new_detector.detect(orderbook, trades, "binance", symbol)
            # results['new_detector'].append(new_result)
            
            # Compare
            # self._print_comparison(old_result, new_result)
            
            await asyncio.sleep(30)  # Check every 30 seconds
        
        # Generate comparison report
        # report = self._generate_comparison_report(results)
        # self._save_comparison_report(report)
        
        return results
    
    def _print_comparison(self, old_result: Dict, new_result: Dict):
        """Print side-by-side comparison"""
        old_count = len(old_result.get('icebergs', []))
        new_count = len(new_result.get('icebergs', []))
        
        old_conf = sum(i['confidence'] for i in old_result.get('icebergs', [])) / max(old_count, 1)
        new_conf = sum(i['confidence'] for i in new_result.get('icebergs', [])) / max(new_count, 1)
        
        print(f"Old Detector: {old_count} icebergs, avg confidence: {old_conf:.2%}")
        print(f"New Detector: {new_count} icebergs, avg confidence: {new_conf:.2%}")
    
    def _generate_comparison_report(self, results: Dict) -> Dict:
        """Generate detailed comparison report"""
        old_all = []
        new_all = []
        
        for r in results['old_detector']:
            old_all.extend(r.get('icebergs', []))
        
        for r in results['new_detector']:
            new_all.extend(r.get('icebergs', []))
        
        report = {
            'summary': {
                'old_detector_total': len(old_all),
                'new_detector_total': len(new_all),
                'difference': len(new_all) - len(old_all)
            },
            'old_detector': {
                'total_detections': len(old_all),
                'avg_confidence': sum(i['confidence'] for i in old_all) / max(len(old_all), 1),
                'high_confidence_count': len([i for i in old_all if i['confidence'] > 0.7])
            },
            'new_detector': {
                'total_detections': len(new_all),
                'avg_confidence': sum(i['confidence'] for i in new_all) / max(len(new_all), 1),
                'high_confidence_count': len([i for i in new_all if i['confidence'] > 0.7])
            }
        }
        
        return report
    
    def _save_comparison_report(self, report: Dict):
        """Save comparison report"""
        filename = f"comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n✓ Comparison report saved to {filename}")


class QuickValidator:
    """Quick validation against simple test cases"""
    
    def __init__(self):
        self.test_cases = []
    
    def create_test_case_obvious_iceberg(self) -> Dict:
        """
        Create a synthetic test case with obvious iceberg
        
        Scenario: Buy order at 42000 keeps refilling after trades
        """
        return {
            'name': 'Obvious Iceberg - Buy Side',
            'orderbook': {
                'bids': [
                    {'price': 42000.0, 'volume': 0.5},  # Small visible
                    {'price': 41999.0, 'volume': 1.0},
                ],
                'asks': [
                    {'price': 42001.0, 'volume': 1.0},
                    {'price': 42002.0, 'volume': 1.5},
                ],
                'timestamp': int(datetime.now().timestamp() * 1000),
                'symbol': 'BTC/USDT',
                'exchange': 'test'
            },
            'trades': [
                # Multiple large trades hitting the 42000 level
                {
                    'price': 42000.0,
                    'amount': 2.0,  # Much larger than visible 0.5
                    'side': 'sell',
                    'maker_side': 'buy',  # Buy order absorbed sells
                    'timestamp': int(datetime.now().timestamp() * 1000),
                    'id': '1'
                },
                {
                    'price': 42000.0,
                    'amount': 1.5,
                    'side': 'sell',
                    'maker_side': 'buy',
                    'timestamp': int(datetime.now().timestamp() * 1000),
                    'id': '2'
                }
            ],
            'expected_detection': True,
            'expected_side': 'buy',
            'expected_price': 42000.0
        }
    
    def create_test_case_no_iceberg(self) -> Dict:
        """
        Create a test case with no iceberg (normal orderbook)
        """
        return {
            'name': 'Normal Orderbook - No Iceberg',
            'orderbook': {
                'bids': [
                    {'price': 42000.0, 'volume': 5.0},  # Large visible
                    {'price': 41999.0, 'volume': 3.0},
                ],
                'asks': [
                    {'price': 42001.0, 'volume': 4.0},
                    {'price': 42002.0, 'volume': 3.0},
                ],
                'timestamp': int(datetime.now().timestamp() * 1000),
                'symbol': 'BTC/USDT',
                'exchange': 'test'
            },
            'trades': [
                # Small trades, nothing unusual
                {
                    'price': 42000.0,
                    'amount': 0.1,
                    'side': 'sell',
                    'maker_side': 'buy',
                    'timestamp': int(datetime.now().timestamp() * 1000),
                    'id': '1'
                }
            ],
            'expected_detection': False
        }
    
    async def run_quick_tests(self, detector):
        """Run quick sanity tests"""
        print("=" * 80)
        print("QUICK VALIDATION TESTS")
        print("=" * 80)
        
        test_cases = [
            self.create_test_case_obvious_iceberg(),
            self.create_test_case_no_iceberg()
        ]
        
        results = []
        
        for test_case in test_cases:
            print(f"\nTest: {test_case['name']}")
            
            result = await detector.detect(
                orderbook=test_case['orderbook'],
                trades=test_case['trades'],
                exchange='test',
                symbol='BTC/USDT'
            )
            
            detected = len(result.get('icebergs', [])) > 0
            expected = test_case['expected_detection']
            
            passed = detected == expected
            
            if passed:
                print(f"✓ PASSED")
            else:
                print(f"✗ FAILED")
                print(f"  Expected: {expected}, Got: {detected}")
            
            if detected:
                print(f"  Detections: {len(result['icebergs'])}")
                for iceberg in result['icebergs']:
                    print(f"    - {iceberg['side']} @ {iceberg['price']} "
                          f"(confidence: {iceberg['confidence']:.2%})")
            
            results.append({
                'test': test_case['name'],
                'passed': passed,
                'expected': expected,
                'detected': detected
            })
        
        # Summary
        passed_count = sum(1 for r in results if r['passed'])
        print(f"\n{'=' * 80}")
        print(f"SUMMARY: {passed_count}/{len(results)} tests passed")
        print(f"{'=' * 80}")
        
        return results


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def main():
    parser = argparse.ArgumentParser(description='Iceberg Detector Testing')
    parser.add_argument('--mode', choices=['collect', 'validate', 'compare', 'quick'],
                       required=True, help='Testing mode')
    parser.add_argument('--symbol', default='BTC/USDT', help='Trading symbol')
    parser.add_argument('--duration', type=int, default=30, help='Duration in minutes')
    
    args = parser.parse_args()
    
    if args.mode == 'collect':
        session = LiveTestingSession()
        await session.collect_ground_truth_interactive(
            symbol=args.symbol,
            duration_minutes=args.duration
        )
    
    elif args.mode == 'compare':
        comparison = DetectorComparison()
        await comparison.run_comparison(
            symbol=args.symbol,
            test_duration_minutes=args.duration
        )
    
    elif args.mode == 'quick':
        # You'll need to initialize your detector here
        # from iceberg_detector_improved import IcebergDetectorImproved
        # detector = IcebergDetectorImproved()
        
        validator = QuickValidator()
        # results = await validator.run_quick_tests(detector)
        
        print("\nTo run quick tests, uncomment the detector initialization in live_testing.py")
    
    elif args.mode == 'validate':
        print("Validation mode - implement after collecting ground truth")


if __name__ == "__main__":
    asyncio.run(main())
