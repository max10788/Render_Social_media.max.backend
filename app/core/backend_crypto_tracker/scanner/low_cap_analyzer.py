"""
Low-Cap-Token-Analysator, der verschiedene Analysekomponenten integriert.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import asyncio
import logging
import aiohttp

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
    RiskAssessment
)

from app.core.backend_crypto_tracker.scanner.scoring_engine import (
    MultiChainScoringEngine,
    ScanConfig
)

from app.core.backend_crypto_tracker.utils.cache import AnalysisCache  # Verwende deine vorhandene Cache-Klasse


class LowCapAnalyzer:
    """
    Zentrale Fassade/Koordinator für die gesamte Low-Cap-Token-Analyse.
    Integriert Funktionalitäten aus verschiedenen Analysekomponenten.
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
        
        # Initialisiere die Komponenten (Ressourcen werden in __aenter__ erstellt)
        self.token_analyzer: Optional[TokenAnalyzer] = None
        self.risk_assessor: Optional[AdvancedRiskAssessor] = None
        self.scoring_engine: Optional[MultiChainScoringEngine] = None
        self.wallet_classifier: Optional[WalletClassifier] = None
        
        self.session = None
        
        self.logger.info("LowCapAnalyzer initialisiert")

    async def __aenter__(self):
        """Initialisiert asynchrone Ressourcen"""
        self.logger.info("Initialisiere asynchrone Ressourcen für LowCapAnalyzer")
        try:
            # Erstelle eine gemeinsame Session für HTTP-Anfragen
            self.session = aiohttp.ClientSession()
            
            # Aktualisiere die Konfiguration mit Cache-Einstellungen
            self.config.enable_cache = self.enable_cache
            self.config.cache_ttl_seconds = self.cache_ttl
            
            # Erstelle Instanzen der Komponenten mit asynchroner Initialisierung
            self.token_analyzer = TokenAnalyzer(self.config)
            await self.token_analyzer.__aenter__()
            
            self.risk_assessor = AdvancedRiskAssessor()
            self.scoring_engine = MultiChainScoringEngine()
            self.wallet_classifier = WalletClassifier()
            await self.wallet_classifier.__aenter__()
            
            self.logger.info("Asynchrone Ressourcen erfolgreich initialisiert")
            return self
        except Exception as e:
            self.logger.error(f"Fehler bei der Initialisierung der Ressourcen: {str(e)}")
            await self.__aexit__(type(e), e, e.__traceback__)
            raise CustomAnalysisException(f"Initialisierung fehlgeschlagen: {str(e)}")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Schließt asynchrone Ressourcen"""
        self.logger.info("Schließe asynchrone Ressourcen für LowCapAnalyzer")
        
        # Schließe alle Komponenten in umgekehrter Reihenfolge der Erstellung
        close_tasks = []
        
        # Schließe TokenAnalyzer (wichtig, da dieser die Provider verwaltet)
        if self.token_analyzer:
            close_tasks.append(self._safe_close_token_analyzer(self.token_analyzer, exc_type, exc_val, exc_tb))
        
        # Schließe Hauptkomponenten
        if self.wallet_classifier and hasattr(self.wallet_classifier, '__aexit__'):
            close_tasks.append(self._safe_close_component(
                self.wallet_classifier, exc_type, exc_val, exc_tb, "wallet_classifier"))
        
        # Schließe Session
        if self.session:
            close_tasks.append(self._safe_close_session(self.session))
        
        # Alle Schließvorgänge parallel ausführen
        if close_tasks:
            results = await asyncio.gather(*close_tasks, return_exceptions=True)
            
            # Logge Fehler beim Schließen
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    component_name = close_tasks[i].__name__ if hasattr(close_tasks[i], '__name__') else "unknown"
                    self.logger.error(f"Fehler beim Schließen von {component_name}: {str(result)}")
        
        # Gib Cache-Statistiken aus, falls Cache aktiviert ist
        if self.cache:
            cache_stats = self.cache.get_stats()
            self.logger.info(f"Cache-Statistiken: {cache_stats}")
        
        self.logger.info("Asynchrone Ressourcen erfolgreich geschlossen")

    async def _safe_close_component(self, component, exc_type, exc_val, exc_tb, component_name):
        """Sicheres Schließen einer Komponente"""
        try:
            if hasattr(component, '__aexit__'):
                await component.__aexit__(exc_type, exc_val, exc_tb)
            # Zusätzlich: Explizite close-Methode aufrufen, falls vorhanden
            if hasattr(component, 'close'):
                await component.close()
        except Exception as e:
            self.logger.error(f"Fehler beim Schließen von {component_name}: {str(e)}")
            # Kein raise hier, um andere Schließvorgänge nicht zu blockieren

    async def _safe_close_session(self, session):
        """Sicheres Schließen einer Session"""
        try:
            if session:
                # Schließe zuerst den Connector
                if hasattr(session, 'connector') and session.connector:
                    await session.connector.close()
                # Dann schließe die Session
                await session.close()
        except Exception as e:
            self.logger.error(f"Fehler beim Schließen der Session: {str(e)}")
            # Kein raise hier, um andere Schließvorgänge nicht zu blockieren

    async def _safe_close_token_analyzer(self, token_analyzer, exc_type, exc_val, exc_tb):
        """Sicheres Schließen des TokenAnalyzers"""
        try:
            if hasattr(token_analyzer, '__aexit__'):
                await token_analyzer.__aexit__(exc_type, exc_val, exc_tb)
            if hasattr(token_analyzer, 'close'):
                await token_analyzer.close()
        except Exception as e:
            self.logger.error(f"Fehler beim Schließen des TokenAnalyzers: {str(e)}")
            # Kein raise hier, um andere Schließvorgänge nicht zu blockieren

    async def analyze_custom_token(self, token_address: str, chain: str, use_cache: Optional[bool] = None) -> Dict[str, Any]:
        """Zentrale Analyse-Methode für einen einzelnen Token"""
        self.logger.info(f"Starte Analyse für Token {token_address} auf Chain {chain}")
        
        # Bestimme, ob Cache verwendet werden soll
        should_use_cache = use_cache if use_cache is not None else self.enable_cache
        
        # Cache-Schlüssel für diese Anfrage
        cache_key = f"lowcap_analyzer_custom_{token_address}_{chain}"
        
        # Prüfe, ob die Daten im Cache vorhanden sind
        if should_use_cache and self.cache:
            cached_result = await self.cache.get(cache_key)
            if cached_result:
                self.logger.info(f"Returning cached analysis for {token_address} on {chain}")
                return cached_result
        
        # Validierung der Eingabeparameter
        if not token_address or not isinstance(token_address, str) or not token_address.strip():
            error_msg = "Token-Adresse muss ein nicht-leerer String sein"
            self.logger.error(error_msg)
            raise ValidationException(error_msg, field="token_address")
        
        if not chain or not isinstance(chain, str) or not chain.strip():
            error_msg = "Chain muss ein nicht-leerer String sein"
            self.logger.error(error_msg)
            raise ValidationException(error_msg, field="chain")
        
        # Normalisiere Chain-Name (kleinschreibung)
        chain = chain.lower().strip()
        
        # Prüfe, ob der TokenAnalyzer initialisiert ist
        if not self.token_analyzer:
            error_msg = "TokenAnalyzer ist nicht initialisiert. Verwenden Sie den Analyzer innerhalb eines async-Kontext-Managers (async with)."
            self.logger.error(error_msg)
            raise CustomAnalysisException(error_msg)
        
        try:
            # Delegiere die Analyse an den TokenAnalyzer
            result = await self.token_analyzer.analyze_custom_token(token_address, chain)
            
            # Zusätzliche Prüfung des Ergebnisses
            if not result or 'token_info' not in result:
                raise CustomAnalysisException("Ungültiges Analyseergebnis erhalten")
            
            # Führe erweiterte Risikobewertung durch, falls verfügbar
            if self.risk_assessor and 'wallet_analysis' in result:
                try:
                    # Extrahiere die notwendigen Daten für die Risikobewertung
                    token_data = result['token_info']
                    wallet_analyses = []
                    
                    # Rekonstruiere WalletAnalysis-Objekte aus den serialisierten Daten
                    # Greife auf die Halterdaten aus dem Ergebnis des TokenAnalyzers zu
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
                            transaction_count=0,  # Nicht in den serialisierten Daten enthalten
                            first_transaction=None,  # Nicht in den serialisierten Daten enthalten
                            last_transaction=None,  # Nicht in den serialisierten Daten enthalten
                            risk_score=0  # Wird neu berechnet
                        )
                        wallet_analyses.append(wallet_analysis)
                    
                    # Führe die erweiterte Risikobewertung durch
                    risk_assessment = await self._perform_advanced_risk_assessment(token_data, wallet_analyses)
                    
                    # Füge die Risikobewertung zum Ergebnis hinzu
                    result['risk_assessment'] = {
                        'overall_risk': risk_assessment.overall_risk,
                        'risk_factors': risk_assessment.risk_factors,
                        'recommendation': risk_assessment.recommendation
                    }
                except Exception as e:
                    self.logger.warning(f"Fehler bei der erweiterten Risikobewertung: {str(e)}")
            
            # Führe erweiterte Scoring-Berechnung durch, falls verfügbar
            if self.scoring_engine and 'wallet_analysis' in result:
                try:
                    # Extrahiere die notwendigen Daten für das Scoring
                    token_data = result['token_info']
                    wallet_analyses = []
                    
                    # Rekonstruiere WalletAnalysis-Objekte aus den serialisierten Daten
                    # Greife auf die Halterdaten aus dem Ergebnis des TokenAnalyzers zu
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
                            transaction_count=0,  # Nicht in den serialisierten Daten enthalten
                            first_transaction=None,  # Nicht in den serialisierten Daten enthalten
                            last_transaction=None,  # Nicht in den serialisierten Daten enthalten
                            risk_score=0  # Wird neu berechnet
                        )
                        wallet_analyses.append(wallet_analysis)
                    
                    # Führe die erweiterte Scoring-Berechnung durch
                    score_result = await self._calculate_advanced_score(token_data, wallet_analyses, chain)
                    
                    # Füge das Scoring-Ergebnis zum Ergebnis hinzu
                    result['advanced_score'] = score_result
                except Exception as e:
                    self.logger.warning(f"Fehler bei der erweiterten Scoring-Berechnung: {str(e)}")
            
            # Speichere das Ergebnis im Cache
            if should_use_cache and self.cache:
                await self.cache.set(result, self.cache_ttl, cache_key)
                
            self.logger.info(f"Analyse für Token {token_address} auf Chain {chain} abgeschlossen")
            return result
        except ValueError as e:
            # Spezielle Behandlung für "Token data could not be retrieved" Fehler
            if "Token data could not be retrieved" in str(e):
                self.logger.error(f"Konnte Tokendaten nicht abrufen für {token_address} auf {chain}: {str(e)}")
                # Erstelle ein minimales Analyseergebnis, auch wenn keine Token-Daten abgerufen werden konnten
                minimal_result = {
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
                    'score': 50.0,  # Neutraler Score
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
                
                # Speichere das minimale Ergebnis im Cache
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

    async def scan_low_cap_tokens(self, max_tokens: Optional[int] = None, use_cache: Optional[bool] = None) -> List[Dict[str, Any]]:
        """
        Massenanalyse-Methode für mehrere Tokens (z.B. für periodische Scans).
        Diese Methode delegiert die Arbeit an den bereits instanziierten TokenAnalyzer.

        Args:
            max_tokens: Maximale Anzahl von Tokens, die analysiert werden sollen
            use_cache: Ob der Cache verwendet werden soll (überschreibt die Instanzeinstellung)

        Returns:
            Eine Liste von Dictionaries mit Analyseergebnissen für jeden Token
        """
        self.logger.info(f"Starte Low-Cap-Token-Scan mit max_tokens={max_tokens}")

        # Bestimme, ob Cache verwendet werden soll
        should_use_cache = use_cache if use_cache is not None else self.enable_cache
        
        # Cache-Schlüssel für diese Anfrage
        cache_key = f"lowcap_analyzer_scan_{max_tokens or 'default'}"
        
        # Prüfe, ob die Daten im Cache vorhanden sind
        if should_use_cache and self.cache:
            cached_result = await self.cache.get(cache_key)
            if cached_result:
                self.logger.info(f"Returning cached scan results for max_tokens={max_tokens}")
                return cached_result

        # Prüfe, ob der TokenAnalyzer initialisiert ist
        if not self.token_analyzer:
            error_msg = "TokenAnalyzer ist nicht initialisiert. Verwenden Sie den Analyzer innerhalb eines async-Kontext-Managers (async with)."
            self.logger.error(error_msg)
            raise CustomAnalysisException(error_msg)

        try:
            # Delegiere den Scan an den TokenAnalyzer
            # TokenAnalyzer.scan_low_cap_tokens erwartet max_tokens als int oder None
            results = await self.token_analyzer.scan_low_cap_tokens(max_tokens)
            
            # Speichere das Ergebnis im Cache
            if should_use_cache and self.cache:
                await self.cache.set(results, self.cache_ttl, cache_key)

            self.logger.info(f"Low-Cap-Token-Scan abgeschlossen. {len(results)} Tokens erfolgreich analysiert")
            return results

        except Exception as e:
            self.logger.error(f"Fehler beim Low-Cap-Token-Scan: {str(e)}", exc_info=True)
            raise CustomAnalysisException(f"Scan fehlgeschlagen: {str(e)}") from e
    
    async def invalidate_cache(self, pattern: str = None) -> int:
        """
        Invalidiert Cache-Einträge, optional mit Muster.
        
        Args:
            pattern: Muster für die zu invalidierenden Schlüssel. Wenn None, wird der gesamte Cache geleert.
        
        Returns:
            Anzahl der gelöschten Einträge
        """
        if not self.cache:
            return 0
        
        if pattern:
            # Lösche alle Einträge, die das Muster enthalten
            keys = await self.cache.get_keys()
            count = 0
            for key in keys:
                if pattern in key:
                    # Zerlege den Schlüssel in seine Bestandteile für die delete-Methode
                    parts = key.split(":")
                    if await self.cache.delete(*parts):
                        count += 1
            return count
        else:
            # Lösche den gesamten Cache
            await self.cache.clear()
            return -1  # -1 bedeutet "alle Einträge gelöscht"
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """
        Gibt Cache-Statistiken zurück.
        
        Returns:
            Dictionary mit Cache-Statistiken
        """
        if self.cache:
            return self.cache.get_stats()
        return {"message": "Cache is disabled"}

    # Optional: Erweiterte Analysemethoden, die die anderen Komponenten nutzen
    # Diese könnten verwendet werden, um zusätzliche Analysen durchzuführen,
    # die über die Standardfunktionalität des TokenAnalyzers hinausgehen.
    
    def _perform_extended_risk_assessment(self, analysis_result: Dict[str, Any]) -> Dict[str, Any]:
        """Führt eine erweiterte Risikobewertung durch"""
        try:
            # Hole die Basis-Risikobewertung
            base_risk = analysis_result.get('score', 50)  # Fallback auf 50 wenn nicht vorhanden
            risk_flags = analysis_result.get('risk_flags', [])
            metrics = analysis_result.get('metrics', {})
            
            # Erweiterte Risiko-Faktoren
            extended_risk_factors = []
            
            # 1. Liquiditäts-Risiko
            liquidity = analysis_result.get('token_info', {}).get('liquidity', 0)
            if liquidity < 10000:  # Unter $10k
                extended_risk_factors.append("very_low_liquidity")
            elif liquidity < 50000:  # Unter $50k
                extended_risk_factors.append("low_liquidity")
            
            # 2. Holder-Konzentration
            whale_percentage = metrics.get('whale_percentage', 0)
            if whale_percentage > 60:
                extended_risk_factors.append("extreme_whale_concentration")
            elif whale_percentage > 40:
                extended_risk_factors.append("high_whale_concentration")
            
            # 3. Dev-Wallet-Risiko
            dev_percentage = metrics.get('dev_percentage', 0)
            if dev_percentage > 30:
                extended_risk_factors.append("extreme_dev_concentration")
            elif dev_percentage > 15:
                extended_risk_factors.append("high_dev_concentration")
            
            # 4. Rugpull-Indikatoren
            rugpull_suspects = metrics.get('rugpull_suspects', 0)
            if rugpull_suspects > 2:
                extended_risk_factors.append("multiple_rugpull_suspects")
            elif rugpull_suspects > 0:
                extended_risk_factors.append("rugpull_suspects_detected")
            
            # 5. Marktkapitalisierungs-Risiko
            market_cap = analysis_result.get('token_info', {}).get('market_cap', 0)
            if market_cap < 50000:  # Unter $50k
                extended_risk_factors.append("micro_cap_risk")
            elif market_cap < 100000:  # Unter $100k
                extended_risk_factors.append("very_small_cap_risk")
            
            # Berechne finalen Risiko-Score
            risk_penalty = len(extended_risk_factors) * 5  # 5 Punkte pro Risikofaktor
            final_risk_score = max(0, base_risk - risk_penalty)
            
            # Klassifiziere Risiko-Level
            if final_risk_score >= 80:
                risk_level = "low"
            elif final_risk_score >= 60:
                risk_level = "moderate"
            elif final_risk_score >= 40:
                risk_level = "high"
            else:
                risk_level = "very_high"
            
            # Erstelle erweiterte Risikobewertung mit overall_risk Attribut
            extended_assessment = {
                'overall_risk': final_risk_score,  # Dies ist das fehlende Attribut!
                'risk_level': risk_level,
                'base_score': base_risk,
                'risk_penalty': risk_penalty,
                'extended_risk_factors': extended_risk_factors,
                'all_risk_flags': risk_flags + extended_risk_factors,
                'risk_breakdown': {
                    'liquidity_risk': liquidity < 50000,
                    'concentration_risk': whale_percentage > 40,
                    'dev_risk': dev_percentage > 15,
                    'rugpull_risk': rugpull_suspects > 0,
                    'market_cap_risk': market_cap < 100000
                }
            }
            
            # Aktualisiere das ursprüngliche Ergebnis
            analysis_result['extended_risk_assessment'] = extended_assessment
            analysis_result['final_risk_score'] = final_risk_score
            analysis_result['risk_level'] = risk_level
            
            return extended_assessment
            
        except Exception as e:
            logger.error(f"Fehler bei der erweiterten Risikobewertung: {e}")
            # Fallback-Risikobewertung mit overall_risk
            return {
                'overall_risk': 50,  # Fallback-Wert
                'risk_level': 'moderate',
                'base_score': 50,
                'risk_penalty': 0,
                'extended_risk_factors': [],
                'all_risk_flags': [],
                'error': str(e)
            }
        
    async def _calculate_advanced_score(self, token_data: Dict[str, Any], wallet_analyses: List[WalletAnalysis], chain: str) -> Dict[str, Any]:
        """
        Berechnet einen erweiterten Score mit dem MultiChainScoringEngine.
        
        Args:
            token_data: Token-Daten
            wallet_analyses: Ergebnisse der Wallet-Analyse
            chain: Blockchain
            
        Returns:
            Dictionary mit Scoring-Ergebnissen
        """
        if not self.scoring_engine:
            raise CustomAnalysisException("ScoringEngine ist nicht initialisiert")
            
        try:
            # Verwende die Methode aus MultiChainScoringEngine
            # Achtung: Die Signatur muss mit der in scoring_engine.py übereinstimmen
            # Dort ist es: def calculate_token_score_custom(self, token_data, wallet_analyses: List, chain: str)
            return self.scoring_engine.calculate_token_score_custom(token_data, wallet_analyses, chain)
        except Exception as e:
            self.logger.error(f"Fehler bei der erweiterten Scoring-Berechnung: {str(e)}")
            # Fallback auf ein einfaches Scoring
            return {"total_score": 50.0, "metrics": {}, "risk_flags": ["scoring_failed"]}
