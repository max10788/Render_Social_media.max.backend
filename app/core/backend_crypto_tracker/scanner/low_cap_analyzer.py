"""
Low-Cap-Token-Analysator - Refactored for new blockchain data system
Integriert verschiedene Analysekomponenten ohne Provider-Abhängigkeiten
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import asyncio
import logging

from app.core.backend_crypto_tracker.utils.json_helpers import sanitize_value
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import (
    APIException,
    NotFoundException,
    CustomAnalysisException,
    ValidationException
)

# Importiere die Klassen aus den bestehenden Dateien
from app.core.backend_crypto_tracker.scanner.token_analyzer import (
    TokenAnalyzer,
    TokenAnalysisConfig
)

from app.core.backend_crypto_tracker.scanner.wallet_classifier import (
    EnhancedWalletClassifier as WalletClassifier,
    WalletTypeEnum,
    WalletAnalysis
)

from app.core.backend_crypto_tracker.scanner.risk_assessor import (
    AdvancedRiskAssessor,
    RiskAssessment,
    RiskLevel
)

from app.core.backend_crypto_tracker.scanner.scoring_engine import (
    MultiChainScoringEngine,
    ScanConfig
)

from app.core.backend_crypto_tracker.utils.cache import AnalysisCache


class LowCapAnalyzer:
    """
    Zentrale Fassade/Koordinator für die gesamte Low-Cap-Token-Analyse.
    Refactored: Entfernt Provider-Abhängigkeiten, delegiert an TokenAnalyzer
    """
    
    def __init__(
        self,
        config: Optional[TokenAnalysisConfig] = None,
        scan_config: Optional[ScanConfig] = None,
        enable_cache: bool = True,
        cache_ttl: int = 300
    ):
        self.logger = get_logger(__name__)
        self.config = config or TokenAnalysisConfig()
        self.scan_config = scan_config or ScanConfig()
        
        # Cache-Einstellungen
        self.enable_cache = enable_cache
        self.cache_ttl = cache_ttl
        self.cache = AnalysisCache(max_size=1000, default_ttl=cache_ttl) if enable_cache else None
        
        # Initialisiere die Komponenten
        self.token_analyzer: Optional[TokenAnalyzer] = None
        self.risk_assessor: Optional[AdvancedRiskAssessor] = None
        self.scoring_engine: Optional[MultiChainScoringEngine] = None
        self.wallet_classifier: Optional[WalletClassifier] = None
        
        # Initialisierungs-Flag
        self._initialized = False
        
        self.logger.debug("LowCapAnalyzer initialisiert (Refactored Version)")

    async def __aenter__(self):
        """Initialisiert asynchrone Ressourcen"""
        if not self._initialized:
            self.logger.debug("Initialisiere asynchrone Ressourcen für LowCapAnalyzer")
            try:
                # Aktualisiere die Konfiguration mit Cache-Einstellungen
                self.config.enable_cache = self.enable_cache
                self.config.cache_ttl_seconds = self.cache_ttl
                
                # Erstelle TokenAnalyzer (macht jetzt alles selbst, keine Provider nötig)
                self.token_analyzer = TokenAnalyzer(self.config)
                await self.token_analyzer.__aenter__()
                
                # Erstelle andere Komponenten
                self.risk_assessor = AdvancedRiskAssessor()
                self.scoring_engine = MultiChainScoringEngine()
                self.wallet_classifier = WalletClassifier()
                await self.wallet_classifier.__aenter__()
                
                self._initialized = True
                self.logger.debug("Asynchrone Ressourcen erfolgreich initialisiert")
                return self
                
            except Exception as e:
                self.logger.error(f"Fehler bei der Initialisierung der Ressourcen: {str(e)}")
                await self.__aexit__(type(e), e, e.__traceback__)
                raise CustomAnalysisException(f"Initialisierung fehlgeschlagen: {str(e)}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Schließt asynchrone Ressourcen"""
        if self._initialized:
            self.logger.debug("Schließe asynchrone Ressourcen für LowCapAnalyzer")
            
            close_tasks = []
            
            # Schließe TokenAnalyzer
            if self.token_analyzer:
                close_tasks.append(self._safe_close_component(
                    self.token_analyzer, exc_type, exc_val, exc_tb, "token_analyzer"
                ))
            
            # Schließe WalletClassifier
            if self.wallet_classifier and hasattr(self.wallet_classifier, '__aexit__'):
                close_tasks.append(self._safe_close_component(
                    self.wallet_classifier, exc_type, exc_val, exc_tb, "wallet_classifier"
                ))
            
            # Alle Schließvorgänge parallel ausführen
            if close_tasks:
                results = await asyncio.gather(*close_tasks, return_exceptions=True)
                
                # Logge Fehler beim Schließen
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        self.logger.error(f"Fehler beim Schließen von Komponente {i}: {str(result)}")
            
            # Cache-Statistiken ausgeben
            if self.cache:
                cache_stats = self.cache.get_stats()
                self.logger.debug(f"Cache-Statistiken: {cache_stats}")
            
            self._initialized = False
            self.logger.debug("Asynchrone Ressourcen erfolgreich geschlossen")

    async def _safe_close_component(self, component, exc_type, exc_val, exc_tb, component_name):
        """Sicheres Schließen einer Komponente"""
        try:
            if hasattr(component, '__aexit__'):
                await component.__aexit__(exc_type, exc_val, exc_tb)
            if hasattr(component, 'close'):
                await component.close()
        except Exception as e:
            self.logger.error(f"Fehler beim Schließen von {component_name}: {str(e)}")

    async def analyze_custom_token(self, token_address: str, chain: str, use_cache: Optional[bool] = None) -> Dict[str, Any]:
        """
        Zentrale Analyse-Methode für einen einzelnen Token
        Refactored: Delegiert komplett an TokenAnalyzer
        """
        self.logger.debug(f"Starte Analyse für Token {token_address} auf Chain {chain}")
        
        # Bestimme, ob Cache verwendet werden soll
        should_use_cache = use_cache if use_cache is not None else self.enable_cache
        
        # Cache-Schlüssel
        cache_key = f"lowcap_analyzer_custom_{token_address}_{chain}"
        
        # Prüfe Cache
        if should_use_cache and self.cache:
            cached_result = await self.cache.get(cache_key)
            if cached_result:
                self.logger.debug(f"Verwende gecachte Analyse für {token_address} auf {chain}")
                return cached_result
        
        # Validierung
        if not token_address or not isinstance(token_address, str) or not token_address.strip():
            error_msg = "Token-Adresse muss ein nicht-leerer String sein"
            self.logger.error(error_msg)
            raise ValidationException(error_msg, field="token_address")
        
        if not chain or not isinstance(chain, str) or not chain.strip():
            error_msg = "Chain muss ein nicht-leerer String sein"
            self.logger.error(error_msg)
            raise ValidationException(error_msg, field="chain")
        
        # Normalisiere Chain-Name
        chain = chain.lower().strip()
        
        # Prüfe Initialisierung
        if not self.token_analyzer:
            error_msg = "TokenAnalyzer ist nicht initialisiert. Verwenden Sie den Analyzer innerhalb eines async-Kontext-Managers (async with)."
            self.logger.error(error_msg)
            raise CustomAnalysisException(error_msg)

        try:
            # Delegiere die Analyse an den TokenAnalyzer (der macht jetzt alles selbst)
            result = await self.token_analyzer.analyze_custom_token(token_address, chain)
            
            # Validiere Ergebnis
            if not result or 'token_info' not in result:
                raise CustomAnalysisException("Ungültiges Analyseergebnis erhalten")
            
            # Erweiterte Wallet-Klassifizierung (optional)
            if 'wallet_analysis' in result and 'top_holders' in result['wallet_analysis']:
                result = await self._enhance_wallet_analysis(result, token_address, chain)
            
            # Erweiterte Risikobewertung (optional)
            if self.risk_assessor and 'wallet_analysis' in result:
                result = await self._enhance_risk_assessment(result)
            
            # Erweiterte Scoring-Berechnung (optional)
            if self.scoring_engine and 'wallet_analysis' in result:
                result = await self._enhance_scoring(result, chain)
            
            # Speichere im Cache
            if should_use_cache and self.cache:
                await self.cache.set(result, self.cache_ttl, cache_key)
                
            self.logger.debug(f"Analyse für Token {token_address} auf Chain {chain} abgeschlossen")
            return result
            
        except ValueError as e:
            if "Token data could not be retrieved" in str(e):
                self.logger.error(f"Konnte Tokendaten nicht abrufen für {token_address} auf {chain}")
                # Minimales Ergebnis
                minimal_result = self._create_minimal_result(token_address, chain)
                
                if should_use_cache and self.cache:
                    await self.cache.set(minimal_result, self.cache_ttl, cache_key)
                
                return minimal_result
            raise CustomAnalysisException(f"Analyse fehlgeschlagen: {str(e)}") from e
        except (APIException, NotFoundException) as e:
            self.logger.error(f"Externer Fehler bei der Token-Analyse: {str(e)}")
            raise CustomAnalysisException(f"Analyse fehlgeschlagen: {str(e)}") from e
        except Exception as e:
            self.logger.error(f"Unerwarteter Fehler bei der Token-Analyse: {str(e)}", exc_info=True)
            raise CustomAnalysisException(f"Unerwarteter Fehler bei der Analyse: {str(e)}") from e

    def _create_minimal_result(self, token_address: str, chain: str) -> Dict[str, Any]:
        """Erstellt ein minimales Analyseergebnis"""
        return {
            'token_info': {
                'address': token_address,
                'name': "Unknown",
                'symbol': "UNKNOWN",
                'chain': chain,
                'market_cap': 0,
                'volume_24h': 0,
                'holders_count': 0,
                'liquidity': 0
            },
            'score': 50.0,
            'metrics': {
                'total_holders_analyzed': 0,
                'whale_wallets': 0,
                'dev_wallets': 0,
                'rugpull_suspects': 0,
                'gini_coefficient': 0,
                'whale_percentage': 0,
                'dev_percentage': 0
            },
            'risk_flags': ["limited_data"],
            'wallet_analysis': {
                'total_wallets': 0,
                'dev_wallets': 0,
                'whale_wallets': 0,
                'rugpull_suspects': 0,
                'top_holders': []
            }
        }

    async def _enhance_wallet_analysis(self, result: Dict[str, Any], token_address: str, chain: str) -> Dict[str, Any]:
        """Erweitert die Wallet-Analyse mit detaillierter Klassifizierung"""
        try:
            if not self.wallet_classifier:
                return result
            
            self.logger.debug(f"Verarbeite {len(result['wallet_analysis']['top_holders'])} Top-Holder")
            
            classified_holders = []
            for holder_data in result['wallet_analysis']['top_holders']:
                wallet_type = await self.wallet_classifier.classify_wallet(
                    holder_data['address'],
                    holder_data['balance'],
                    holder_data['percentage'],
                    chain
                )
                
                classified_holders.append({
                    'address': holder_data['address'],
                    'balance': holder_data['balance'],
                    'percentage': holder_data['percentage'],
                    'type': wallet_type.value
                })
            
            # Aktualisiere Halterdaten
            result['wallet_analysis']['top_holders'] = classified_holders
            
            # Berechne Statistiken
            result['wallet_analysis']['whale_wallets'] = len([
                h for h in classified_holders if h['type'] == 'WHALE_WALLET'
            ])
            result['wallet_analysis']['dev_wallets'] = len([
                h for h in classified_holders if h['type'] == 'DEV_WALLET'
            ])
            result['wallet_analysis']['rugpull_suspects'] = len([
                h for h in classified_holders if h['type'] == 'RUGPULL_SUSPECT'
            ])
            
        except Exception as e:
            self.logger.warning(f"Fehler bei der Wallet-Klassifizierung: {e}")
        
        return result

    async def _enhance_risk_assessment(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Erweitert die Risikobewertung"""
        try:
            if not self.risk_assessor:
                return result
            
            token_data = result['token_info']
            
            # Konvertiere Wallet-Daten
            wallet_analyses = []
            for holder in result['wallet_analysis']['top_holders']:
                wallet_type = WalletTypeEnum.UNKNOWN
                for wt in WalletTypeEnum:
                    if wt.value == holder.get('type', 'unknown'):
                        wallet_type = wt
                        break
                
                wallet_analysis = WalletAnalysis(
                    address=holder.get('address', ''),
                    balance=float(holder.get('balance', 0)),
                    is_whale=wallet_type == WalletTypeEnum.WHALE_WALLET,
                    transaction_count=0
                )
                wallet_analyses.append(wallet_analysis)
            
            # Führe Risikobewertung durch
            risk_assessment = await self.risk_assessor.assess_token_risk_advanced(
                token_data=token_data,
                wallet_analyses=wallet_analyses
            )
            
            # Füge Risikobewertung hinzu
            result['risk_assessment'] = {
                'overall_risk': risk_assessment.overall_score,
                'risk_level': risk_assessment.risk_level,
                'risk_factors': risk_assessment.risk_factors,
                'confidence': risk_assessment.confidence,
                'details': risk_assessment.details
            }
            
        except Exception as e:
            self.logger.warning(f"Fehler bei der Risikobewertung: {e}")
        
        return result

    async def _enhance_scoring(self, result: Dict[str, Any], chain: str) -> Dict[str, Any]:
        """Erweitert das Scoring"""
        try:
            if not self.scoring_engine:
                return result
            
            token_data = result['token_info']
            
            # Rekonstruiere WalletAnalysis-Objekte
            wallet_analyses = []
            for wallet_data in result.get('wallet_analysis', {}).get('top_holders', []):
                wallet_type = WalletTypeEnum.UNKNOWN
                for wt in WalletTypeEnum:
                    if wt.value == wallet_data.get('type', 'unknown'):
                        wallet_type = wt
                        break
                
                wallet_analysis = WalletAnalysis(
                    wallet_address=wallet_data.get('address', ''),
                    wallet_type=wallet_type,
                    balance=wallet_data.get('balance', 0),
                    percentage_of_supply=wallet_data.get('percentage', 0),
                    transaction_count=0,
                    first_transaction=None,
                    last_transaction=None,
                    risk_score=0
                )
                wallet_analyses.append(wallet_analysis)
            
            # Berechne erweiterten Score
            score_result = self.scoring_engine.calculate_token_score_custom(
                token_data, wallet_analyses, chain
            )
            
            result['advanced_score'] = score_result
            
        except Exception as e:
            self.logger.warning(f"Fehler bei der erweiterten Scoring-Berechnung: {str(e)}")
        
        return result

    async def scan_low_cap_tokens(self, max_tokens: Optional[int] = None, use_cache: Optional[bool] = None) -> List[Dict[str, Any]]:
        """
        Massenanalyse-Methode für mehrere Tokens
        Delegiert an TokenAnalyzer
        """
        self.logger.debug(f"Starte Low-Cap-Token-Scan mit max_tokens={max_tokens}")

        should_use_cache = use_cache if use_cache is not None else self.enable_cache
        cache_key = f"lowcap_analyzer_scan_{max_tokens or 'default'}"
        
        # Cache-Check
        if should_use_cache and self.cache:
            cached_result = await self.cache.get(cache_key)
            if cached_result:
                self.logger.debug(f"Verwende gecachte Scan-Ergebnisse")
                return cached_result

        # Prüfe Initialisierung
        if not self.token_analyzer:
            error_msg = "TokenAnalyzer ist nicht initialisiert. Verwenden Sie den Analyzer innerhalb eines async-Kontext-Managers (async with)."
            self.logger.error(error_msg)
            raise CustomAnalysisException(error_msg)

        try:
            # Delegiere an TokenAnalyzer
            results = await self.token_analyzer.scan_low_cap_tokens(max_tokens)
            
            # Cache speichern
            if should_use_cache and self.cache:
                await self.cache.set(results, self.cache_ttl, cache_key)

            self.logger.debug(f"Low-Cap-Token-Scan abgeschlossen. {len(results)} Tokens analysiert")
            return results

        except Exception as e:
            self.logger.error(f"Fehler beim Low-Cap-Token-Scan: {str(e)}", exc_info=True)
            raise CustomAnalysisException(f"Scan fehlgeschlagen: {str(e)}") from e
    
    async def invalidate_cache(self, pattern: str = None) -> int:
        """Invalidiert Cache-Einträge"""
        if not self.cache:
            return 0
        
        if pattern:
            keys = await self.cache.get_keys()
            count = 0
            for key in keys:
                if pattern in key:
                    parts = key.split(":")
                    if await self.cache.delete(*parts):
                        count += 1
            return count
        else:
            await self.cache.clear()
            return -1
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Gibt Cache-Statistiken zurück"""
        if self.cache:
            return self.cache.get_stats()
        return {"message": "Cache is disabled"}
