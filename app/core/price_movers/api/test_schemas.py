"""
Test-Script für API Schemas

Testet Validation und Serialization der Pydantic Models
"""

from datetime import datetime, timedelta
import json
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.core.price_movers.api.schemas import (
    AnalysisRequest,
    QuickAnalysisRequest,
    HistoricalAnalysisRequest,
    WalletLookupRequest,
    CompareExchangesRequest,
    AnalysisResponse,
    CandleData,
    WalletMover,
    AnalysisMetadata,
    ErrorResponse,
    SuccessResponse,
)


def test_analysis_request():
    """Test AnalysisRequest Validation"""
    print("\n" + "=" * 60)
    print("TEST 1: AnalysisRequest Validation")
    print("=" * 60)
    
    # Valid Request
    print("\n✓ Valid Request:")
    request = AnalysisRequest(
        exchange="binance",
        symbol="BTC/USDT",
        timeframe="5m",
        start_time=datetime.now() - timedelta(minutes=5),
        end_time=datetime.now(),
        min_impact_threshold=0.1,
        top_n_wallets=10,
        include_trades=False
    )
    print(f"   Exchange: {request.exchange}")
    print(f"   Symbol: {request.symbol}")
    print(f"   Timeframe: {request.timeframe}")
    print(f"   Valid: ✅")
    
    # Test JSON Serialization
    print("\n✓ JSON Serialization:")
    json_data = request.json(indent=2)
    print(f"   {json_data[:200]}...")
    
    # Invalid Exchange
    print("\n✗ Invalid Exchange:")
    try:
        invalid_request = AnalysisRequest(
            exchange="invalid_exchange",
            symbol="BTC/USDT",
            timeframe="5m",
            start_time=datetime.now() - timedelta(minutes=5),
            end_time=datetime.now()
        )
    except ValueError as e:
        print(f"   Expected Error: {e}")
    
    # Invalid Timeframe
    print("\n✗ Invalid Timeframe:")
    try:
        invalid_request = AnalysisRequest(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="10m",  # Not supported
            start_time=datetime.now() - timedelta(minutes=5),
            end_time=datetime.now()
        )
    except ValueError as e:
        print(f"   Expected Error: {e}")
    
    # Invalid Time Range
    print("\n✗ Invalid Time Range (end before start):")
    try:
        invalid_request = AnalysisRequest(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="5m",
            start_time=datetime.now(),
            end_time=datetime.now() - timedelta(minutes=5)  # Before start
        )
    except ValueError as e:
        print(f"   Expected Error: {e}")
    
    # Invalid Symbol
    print("\n✗ Invalid Symbol (no slash):")
    try:
        invalid_request = AnalysisRequest(
            exchange="binance",
            symbol="BTCUSDT",  # Missing slash
            timeframe="5m",
            start_time=datetime.now() - timedelta(minutes=5),
            end_time=datetime.now()
        )
    except ValueError as e:
        print(f"   Expected Error: {e}")


def test_quick_analysis_request():
    """Test QuickAnalysisRequest"""
    print("\n" + "=" * 60)
    print("TEST 2: QuickAnalysisRequest Validation")
    print("=" * 60)
    
    request = QuickAnalysisRequest(
        exchange="bitget",
        symbol="ETH/USDT",
        timeframe="15m",
        top_n_wallets=5
    )
    
    print(f"\n✓ Valid Request:")
    print(f"   Exchange: {request.exchange}")
    print(f"   Symbol: {request.symbol}")
    print(f"   Timeframe: {request.timeframe}")
    print(f"   Top N: {request.top_n_wallets}")


def test_compare_exchanges_request():
    """Test CompareExchangesRequest"""
    print("\n" + "=" * 60)
    print("TEST 3: CompareExchangesRequest Validation")
    print("=" * 60)
    
    request = CompareExchangesRequest(
        exchanges=["binance", "bitget", "kraken"],
        symbol="BTC/USDT",
        timeframe="5m"
    )
    
    print(f"\n✓ Valid Request:")
    print(f"   Exchanges: {', '.join(request.exchanges)}")
    print(f"   Symbol: {request.symbol}")
    
    # Invalid: Empty list
    print("\n✗ Invalid: Empty exchanges list")
    try:
        invalid_request = CompareExchangesRequest(
            exchanges=[],
            symbol="BTC/USDT",
            timeframe="5m"
        )
    except ValueError as e:
        print(f"   Expected Error: {e}")


def test_analysis_response():
    """Test AnalysisResponse"""
    print("\n" + "=" * 60)
    print("TEST 4: AnalysisResponse Serialization")
    print("=" * 60)
    
    # Create Response
    response = AnalysisResponse(
        candle=CandleData(
            timestamp=datetime.now(),
            open=67500.00,
            high=67800.00,
            low=67450.00,
            close=67750.00,
            volume=1234.56,
            price_change_pct=0.37
        ),
        top_movers=[
            WalletMover(
                wallet_id="whale_0x742d35",
                wallet_type="whale",
                impact_score=0.85,
                total_volume=50.5,
                total_value_usd=3408750.00,
                trade_count=12,
                avg_trade_size=4.21,
                timing_score=0.92,
                volume_ratio=0.041,
                trades=None
            ),
            WalletMover(
                wallet_id="smart_money_5",
                wallet_type="smart_money",
                impact_score=0.72,
                total_volume=35.2,
                total_value_usd=2376400.00,
                trade_count=8,
                avg_trade_size=4.40,
                timing_score=0.85,
                volume_ratio=0.029,
                trades=None
            )
        ],
        analysis_metadata=AnalysisMetadata(
            total_unique_wallets=1523,
            total_volume=1234.56,
            total_trades=8432,
            analysis_duration_ms=450,
            data_sources=["binance_trades", "binance_candles"],
            timestamp=datetime.now()
        )
    )
    
    print("\n✓ Response Created:")
    print(f"   Candle Price Change: {response.candle.price_change_pct}%")
    print(f"   Top Movers: {len(response.top_movers)}")
    print(f"   Total Wallets: {response.analysis_metadata.total_unique_wallets}")
    
    # JSON Serialization
    print("\n✓ JSON Output:")
    json_output = json.loads(response.json())
    print(f"   Top Mover #1: {json_output['top_movers'][0]['wallet_id']}")
    print(f"   Impact Score: {json_output['top_movers'][0]['impact_score']}")
    
    # Pretty Print
    print("\n✓ Full JSON (first 500 chars):")
    pretty_json = json.dumps(json_output, indent=2)
    print(f"   {pretty_json[:500]}...")


def test_error_response():
    """Test ErrorResponse"""
    print("\n" + "=" * 60)
    print("TEST 5: ErrorResponse")
    print("=" * 60)
    
    error = ErrorResponse(
        error="ValidationError",
        message="Exchange 'invalid' wird nicht unterstützt",
        details={
            "field": "exchange",
            "supported_exchanges": ["bitget", "binance", "kraken"]
        }
    )
    
    print("\n✓ Error Response:")
    print(f"   Error Type: {error.error}")
    print(f"   Message: {error.message}")
    print(f"   Details: {error.details}")
    print(f"   Timestamp: {error.timestamp}")


def test_success_response():
    """Test SuccessResponse"""
    print("\n" + "=" * 60)
    print("TEST 6: SuccessResponse")
    print("=" * 60)
    
    success = SuccessResponse(
        message="Analyse erfolgreich gestartet",
        data={
            "job_id": "abc123",
            "estimated_duration_seconds": 5
        }
    )
    
    print("\n✓ Success Response:")
    print(f"   Success: {success.success}")
    print(f"   Message: {success.message}")
    print(f"   Data: {success.data}")


def test_wallet_lookup_request():
    """Test WalletLookupRequest"""
    print("\n" + "=" * 60)
    print("TEST 7: WalletLookupRequest")
    print("=" * 60)
    
    request = WalletLookupRequest(
        wallet_id="whale_0x742d35",
        exchange="binance",
        symbol="BTC/USDT",
        time_range_hours=24
    )
    
    print("\n✓ Valid Request:")
    print(f"   Wallet ID: {request.wallet_id}")
    print(f"   Exchange: {request.exchange}")
    print(f"   Symbol: {request.symbol}")
    print(f"   Time Range: {request.time_range_hours}h")


def test_schema_documentation():
    """Test Schema Documentation (OpenAPI)"""
    print("\n" + "=" * 60)
    print("TEST 8: Schema Documentation")
    print("=" * 60)
    
    # Get JSON Schema
    schema = AnalysisRequest.schema()
    
    print("\n✓ AnalysisRequest Schema:")
    print(f"   Title: {schema.get('title')}")
    print(f"   Properties: {len(schema.get('properties', {}))} fields")
    print(f"   Required: {schema.get('required', [])}")
    
    print("\n✓ Field Details:")
    for field_name, field_info in schema.get('properties', {}).items():
        print(f"   - {field_name}: {field_info.get('type', 'unknown')}")
        if 'description' in field_info:
            print(f"     Description: {field_info['description']}")


def test_all_exchanges():
    """Test all supported exchanges"""
    print("\n" + "=" * 60)
    print("TEST 9: All Supported Exchanges")
    print("=" * 60)
    
    exchanges = ["bitget", "binance", "kraken"]
    
    for exchange in exchanges:
        try:
            request = AnalysisRequest(
                exchange=exchange,
                symbol="BTC/USDT",
                timeframe="5m",
                start_time=datetime.now() - timedelta(minutes=5),
                end_time=datetime.now()
            )
            print(f"\n✓ {exchange.upper()}: Valid")
        except Exception as e:
            print(f"\n✗ {exchange.upper()}: {e}")


def main():
    """Run all tests"""
    
    print("\n" + "🧪 " * 30)
    print("API SCHEMAS - VALIDATION TESTS")
    print("🧪 " * 30)
    
    try:
        test_analysis_request()
        test_quick_analysis_request()
        test_compare_exchanges_request()
        test_analysis_response()
        test_error_response()
        test_success_response()
        test_wallet_lookup_request()
        test_schema_documentation()
        test_all_exchanges()
        
        print("\n" + "✅ " * 30)
        print("ALLE SCHEMA TESTS ERFOLGREICH!")
        print("✅ " * 30 + "\n")
        
    except Exception as e:
        print(f"\n❌ Test Failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
